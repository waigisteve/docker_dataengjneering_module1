#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from io import StringIO
from typing import List, Optional, Tuple

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import DateTime
from tqdm import tqdm


# =========================
# Data models
# =========================

@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str


@dataclass(frozen=True)
class JobConfig:
    csv_url: str
    table_name: str
    load_id: str
    chunk_size: int
    int_like_cols: Tuple[str, ...]
    date_cols: Tuple[str, ...]
    dtype_overrides: dict


# =========================
# Logging / env utils
# =========================

def setup_logger(log_file: str, verbose: bool) -> logging.Logger:
    logger = logging.getLogger("etl")
    logger.setLevel(logging.DEBUG)

    # If rerun with a different log file, clear handlers
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def load_env_file(path: str) -> None:
    """
    Minimal .env loader:
      - ignores blanks and comments
      - supports KEY=VALUE
      - does NOT overwrite existing env vars
    """
    if not path:
        return
    if not os.path.exists(path):
        raise FileNotFoundError(f"--env-file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


# =========================
# DB helpers
# =========================

def read_db_config(env_required: bool, args: argparse.Namespace) -> DBConfig:
    host = args.pg_host or os.environ.get("POSTGRES_HOST") or ("localhost" if not env_required else None)
    port_raw = args.pg_port or os.environ.get("POSTGRES_PORT") or ("5432" if not env_required else None)
    user = args.pg_user or os.environ.get("POSTGRES_USER") or ("root" if not env_required else None)
    password = args.pg_password or os.environ.get("POSTGRES_PASSWORD") or ("root" if not env_required else None)
    dbname = args.pg_db or os.environ.get("POSTGRES_DB") or ("ny_taxi" if not env_required else None)

    missing = []
    if not host:
        missing.append("POSTGRES_HOST/--pg-host")
    if not port_raw:
        missing.append("POSTGRES_PORT/--pg-port")
    if not user:
        missing.append("POSTGRES_USER/--pg-user")
    if password is None or password == "":
        missing.append("POSTGRES_PASSWORD/--pg-password")
    if not dbname:
        missing.append("POSTGRES_DB/--pg-db")

    if missing:
        raise RuntimeError(
            "Missing DB config values: " + ", ".join(missing) +
            ". Provide --env-file or pass explicit --pg-* CLI args."
        )

    return DBConfig(
        host=str(host),
        port=int(port_raw),
        user=str(user),
        password=str(password),
        dbname=str(dbname),
    )


def make_engine(db: DBConfig):
    return create_engine(
        f"postgresql+psycopg2://{db.user}:{db.password}@{db.host}:{db.port}/{db.dbname}"
    )


def make_pg_conn(db: DBConfig):
    conn = psycopg2.connect(
        host=db.host, port=db.port, user=db.user, password=db.password, dbname=db.dbname
    )
    conn.autocommit = False
    return conn


def ensure_progress_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS etl_load_progress (
        load_id TEXT PRIMARY KEY,
        table_name TEXT NOT NULL,
        source_url TEXT NOT NULL,
        chunk_size INT NOT NULL,
        rows_loaded BIGINT NOT NULL DEFAULT 0,
        chunks_loaded BIGINT NOT NULL DEFAULT 0,
        last_success_at TIMESTAMPTZ,
        last_error_at TIMESTAMPTZ,
        last_error TEXT
    );
    """
    with engine.begin() as c:
        c.execute(text(ddl))


def get_progress(engine, load_id: str) -> Tuple[int, int]:
    q = text("SELECT rows_loaded, chunks_loaded FROM etl_load_progress WHERE load_id = :load_id")
    with engine.begin() as c:
        row = c.execute(q, {"load_id": load_id}).fetchone()
    if not row:
        return 0, 0
    return int(row[0]), int(row[1])


def upsert_progress(engine, cfg: JobConfig, rows_loaded: int, chunks_loaded: int, error: Optional[str]) -> None:
    sql = text("""
    INSERT INTO etl_load_progress(load_id, table_name, source_url, chunk_size, rows_loaded, chunks_loaded, last_success_at, last_error_at, last_error)
    VALUES (:load_id, :table_name, :source_url, :chunk_size, :rows_loaded, :chunks_loaded,
            CASE WHEN :error IS NULL THEN NOW() ELSE NULL END,
            CASE WHEN :error IS NOT NULL THEN NOW() ELSE NULL END,
            :error)
    ON CONFLICT (load_id) DO UPDATE SET
        table_name = EXCLUDED.table_name,
        source_url = EXCLUDED.source_url,
        chunk_size = EXCLUDED.chunk_size,
        rows_loaded = EXCLUDED.rows_loaded,
        chunks_loaded = EXCLUDED.chunks_loaded,
        last_success_at = CASE WHEN EXCLUDED.last_error IS NULL THEN NOW() ELSE etl_load_progress.last_success_at END,
        last_error_at   = CASE WHEN EXCLUDED.last_error IS NOT NULL THEN NOW() ELSE etl_load_progress.last_error_at END,
        last_error      = EXCLUDED.last_error;
    """)
    with engine.begin() as c:
        c.execute(sql, {
            "load_id": cfg.load_id,
            "table_name": cfg.table_name,
            "source_url": cfg.csv_url,
            "chunk_size": cfg.chunk_size,
            "rows_loaded": rows_loaded,
            "chunks_loaded": chunks_loaded,
            "error": error
        })


def table_exists(engine, table_name: str) -> bool:
    insp = inspect(engine)
    return insp.has_table(table_name)


def table_columns(engine, table_name: str) -> List[str]:
    with engine.begin() as c:
        cols = [r[0] for r in c.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name = :t
            ORDER BY ordinal_position
        """), {"t": table_name}).fetchall()]
    return cols


def ensure_target_table(engine, table_name: str, first_chunk: pd.DataFrame, date_cols: Tuple[str, ...]) -> None:
    """
    Create the target table *once* with proper TIMESTAMP columns from the start.
    Avoids ALTER COLUMN casting failures.
    """
    if table_exists(engine, table_name):
        return

    df0 = first_chunk.copy()
    for c in date_cols:
        if c in df0.columns:
            df0[c] = pd.to_datetime(df0[c], errors="coerce")

    dtype_map = {c: DateTime() for c in date_cols if c in df0.columns}

    df0.head(0).to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
        dtype=dtype_map,
    )


# =========================
# COPY helpers
# =========================

def normalize_chunk(df: pd.DataFrame, int_like_cols: Tuple[str, ...], date_cols: Tuple[str, ...]) -> pd.DataFrame:
    df = df.copy()

    for c in int_like_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    return df


def make_copy_sql(table_name: str, cols: List[str]) -> str:
    cols_sql = ", ".join([f'"{c}"' for c in cols])
    return f'COPY "{table_name}" ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)'


def copy_chunk(pg_conn, copy_sql: str, cols: List[str], df: pd.DataFrame,
               int_like_cols: Tuple[str, ...], date_cols: Tuple[str, ...]) -> None:
    df = df[cols]
    df = normalize_chunk(df, int_like_cols=int_like_cols, date_cols=date_cols)

    buf = StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="", date_format="%Y-%m-%d %H:%M:%S")
    buf.seek(0)

    with pg_conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)


def _copy_with_retry(pg_conn, copy_sql: str, cols: List[str], df: pd.DataFrame, cfg: JobConfig,
                     logger: logging.Logger, retries_per_chunk: int, retry_sleep_sec: float) -> None:
    attempt = 0
    while True:
        try:
            copy_chunk(
                pg_conn,
                copy_sql=copy_sql,
                cols=cols,
                df=df,
                int_like_cols=cfg.int_like_cols,
                date_cols=cfg.date_cols,
            )
            pg_conn.commit()
            return
        except Exception as e:
            attempt += 1
            try:
                pg_conn.rollback()
            except Exception:
                pass

            logger.warning(f"[chunk failed] attempt={attempt}/{retries_per_chunk} error={repr(e)}")
            if attempt >= retries_per_chunk:
                raise
            time.sleep(retry_sleep_sec)


# =========================
# Release discovery (GitHub)
# =========================

_YELLOW_RE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.csv\.gz$", re.IGNORECASE)

def ym_from_asset(name: str) -> Optional[str]:
    m = _YELLOW_RE.search(name)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"


def fetch_release_assets(repo: str, tag: str, session: Optional[requests.Session] = None) -> List[dict]:
    url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    s = session or requests.Session()
    r = s.get(url, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("assets", [])


def discover_yellow_monthly_urls(repo: str, tag: str, start_month: Optional[str], end_month: Optional[str],
                                logger: logging.Logger) -> List[Tuple[str, str, str]]:
    assets = fetch_release_assets(repo=repo, tag=tag)

    picks: List[Tuple[str, str, str]] = []
    for a in assets:
        name = a.get("name", "")
        ym = ym_from_asset(name)
        if not ym:
            continue
        dl = a.get("browser_download_url")
        if not dl:
            continue
        picks.append((ym, name, dl))

    if not picks:
        raise RuntimeError(f"No yellow *.csv.gz assets found for repo={repo} tag={tag}")

    picks.sort(key=lambda x: x[0])

    if start_month:
        picks = [p for p in picks if p[0] >= start_month]
    if end_month:
        picks = [p for p in picks if p[0] <= end_month]

    out: List[Tuple[str, str, str]] = []
    for ym, _name, dl in picks:
        load_id = f"yellow_{ym.replace('-', '_')}"
        out.append((dl, load_id, ym))

    logger.info(f"Discovered {len(out)} files from release tag '{tag}' (repo={repo})")
    if out:
        logger.info(f"First={out[0][2]} Last={out[-1][2]}")
    return out


# =========================
# Core loader (resume-safe)
# =========================

def run_load(db: DBConfig, cfg: JobConfig, logger: logging.Logger,
             retries_per_chunk: int, retry_sleep_sec: float, skip_completed: bool) -> int:
    engine = make_engine(db)
    ensure_progress_table(engine)

    rows_loaded, chunks_loaded = get_progress(engine, cfg.load_id)
    logger.info(f"[progress] rows_loaded={rows_loaded} chunks_loaded={chunks_loaded}")
    logger.info(f"Target: table={cfg.table_name} load_id={cfg.load_id} chunk_size={cfg.chunk_size}")

    pg_conn = make_pg_conn(db)
    try:
        df_iter = pd.read_csv(
            cfg.csv_url,
            iterator=True,
            chunksize=cfg.chunk_size,
            low_memory=False,
            dtype=cfg.dtype_overrides,
            skiprows=range(1, rows_loaded + 1),
        )

        # Determine schema + COPY SQL
        if not table_exists(engine, cfg.table_name):
            first = next(df_iter)  # may raise StopIteration (empty file)
            if first is None or first.empty:
                logger.info("[done] file has no rows (first chunk empty)")
                return 0

            ensure_target_table(engine, cfg.table_name, first, cfg.date_cols)
            cols = table_columns(engine, cfg.table_name)
            copy_sql = make_copy_sql(cfg.table_name, cols)

            t0 = time.time()
            _copy_with_retry(pg_conn, copy_sql, cols, first, cfg, logger, retries_per_chunk, retry_sleep_sec)
            sec = time.time() - t0

            rows_loaded += len(first)
            chunks_loaded += 1
            upsert_progress(engine, cfg, rows_loaded, chunks_loaded, error=None)
            logger.info(f"[ok] copied chunk={chunks_loaded} rows_in_chunk={len(first)} total_rows={rows_loaded} sec={sec:.2f}")

        else:
            cols = table_columns(engine, cfg.table_name)
            copy_sql = make_copy_sql(cfg.table_name, cols)

        # Stream remaining chunks
        saw_any = False
        for df_chunk in tqdm(df_iter, desc="Loading"):
            if df_chunk is None or df_chunk.empty:
                # Don't copy, don't update progress, just skip
                continue

            saw_any = True
            t0 = time.time()
            _copy_with_retry(pg_conn, copy_sql, cols, df_chunk, cfg, logger, retries_per_chunk, retry_sleep_sec)
            sec = time.time() - t0

            rows_loaded += len(df_chunk)
            chunks_loaded += 1
            upsert_progress(engine, cfg, rows_loaded, chunks_loaded, error=None)
            logger.info(f"[ok] copied chunk={chunks_loaded} rows_in_chunk={len(df_chunk)} total_rows={rows_loaded} sec={sec:.2f}")

        # If we resumed and immediately hit EOF (no chunks processed), treat as complete and continue batch.
        if rows_loaded > 0 and not saw_any and skip_completed:
            logger.info("[done] appears complete already (no remaining chunks). Skipping.")
            return 0

    except StopIteration:
        # Happens when df_iter has no remaining data (common when month already complete)
        if rows_loaded > 0 and skip_completed:
            logger.info("[done] appears complete already (iterator empty). Skipping.")
            return 0
        logger.info("[done] no data (iterator empty)")

    except Exception as e:
        try:
            pg_conn.rollback()
        except Exception:
            pass
        upsert_progress(engine, cfg, rows_loaded, chunks_loaded, error=str(e))
        logger.exception("[FAILED] saved progress for resume")
        raise

    finally:
        try:
            pg_conn.close()
        except Exception:
            pass

    # Final count (table-wide, not per load_id)
    try:
        n = int(pd.read_sql(f'SELECT COUNT(*) AS n FROM "{cfg.table_name}"', con=engine).iloc[0, 0])
        logger.info(f"Row count: {n}")
    except Exception:
        logger.warning("Could not query final row count", exc_info=True)

    logger.info(f"[complete] rows_loaded={rows_loaded} chunks_loaded={chunks_loaded}")
    return 0


# =========================
# CLI
# =========================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Resume-safe CSV->Postgres loader (single file or GitHub release batch).")

    p.add_argument("--env-file", default=None, help="Path to .env file (optional).")
    p.add_argument("--verbose", action="store_true", help="Verbose console logging.")
    p.add_argument("--log-file", default="etl_batch.log", help="Log file path (default: etl_batch.log)")

    # DB args (override env)
    p.add_argument("--pg-host", default=None)
    p.add_argument("--pg-port", type=int, default=None)
    p.add_argument("--pg-user", default=None)
    p.add_argument("--pg-password", default=None)
    p.add_argument("--pg-db", default=None)

    # job
    p.add_argument("--table", default="yellow_taxi_data")
    p.add_argument("--chunk-size", type=int, default=50_000)
    p.add_argument("--retries-per-chunk", type=int, default=3)
    p.add_argument("--retry-sleep-sec", type=float, default=2.0)
    p.add_argument("--skip-completed", action="store_true", default=True,
                   help="Skip months that appear already fully ingested (default: true).")

    # single-file mode
    p.add_argument("--csv-url", default=None, help="Single CSV URL to ingest")
    p.add_argument("--load-id", default=None, help="Progress id (required for --csv-url unless inferred).")

    # batch-from-release mode
    p.add_argument("--from-release", default=None, help="GitHub release tag to ingest all assets from (e.g. yellow).")
    p.add_argument("--repo", default="DataTalksClub/nyc-tlc-data", help="GitHub repo for release tag.")
    p.add_argument("--start-month", default=None, help="YYYY-MM lower bound (inclusive).")
    p.add_argument("--end-month", default=None, help="YYYY-MM upper bound (inclusive).")

    return p


def infer_load_id_from_url(url: str) -> Optional[str]:
    m = _YELLOW_RE.search(url)
    if not m:
        return None
    ym = f"{m.group(1)}-{m.group(2)}"
    return f"yellow_{ym.replace('-', '_')}"


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)

    if args.env_file:
        load_env_file(args.env_file)

    logger = setup_logger(log_file=args.log_file, verbose=args.verbose)

    db = read_db_config(env_required=False, args=args)
    logger.info(f"DB: host={db.host} port={db.port} db={db.dbname} user={db.user}")

    int_like_cols = ("VendorID", "RatecodeID", "passenger_count", "payment_type")
    date_cols = ("tpep_pickup_datetime", "tpep_dropoff_datetime")
    dtype_overrides = {"store_and_fwd_flag": "string"}

    if args.csv_url and args.from_release:
        raise SystemExit("Use either --csv-url OR --from-release, not both.")

    # Batch mode
    if args.from_release or (not args.csv_url):
        tag = args.from_release or "yellow"
        jobs = discover_yellow_monthly_urls(
            repo=args.repo,
            tag=tag,
            start_month=args.start_month,
            end_month=args.end_month,
            logger=logger,
        )

        for csv_url, load_id, ym in jobs:
            cfg = JobConfig(
                csv_url=csv_url,
                table_name=args.table,
                load_id=load_id,
                chunk_size=args.chunk_size,
                int_like_cols=int_like_cols,
                date_cols=date_cols,
                dtype_overrides=dtype_overrides,
            )
            logger.info(f"Job: month={ym} csv_url={csv_url} table={cfg.table_name} load_id={cfg.load_id}")
            run_load(
                db=db,
                cfg=cfg,
                logger=logger,
                retries_per_chunk=args.retries_per_chunk,
                retry_sleep_sec=args.retry_sleep_sec,
                skip_completed=args.skip_completed,
            )

        logger.info("Batch complete.")
        return 0

    # Single-file mode
    load_id = args.load_id or infer_load_id_from_url(args.csv_url)
    if not load_id:
        raise RuntimeError("--load-id is required for --csv-url when it can't be inferred from the URL.")

    cfg = JobConfig(
        csv_url=args.csv_url,
        table_name=args.table,
        load_id=load_id,
        chunk_size=args.chunk_size,
        int_like_cols=int_like_cols,
        date_cols=date_cols,
        dtype_overrides=dtype_overrides,
    )

    logger.info(f"Job: csv_url={cfg.csv_url} table={cfg.table_name} load_id={cfg.load_id}")
    return run_load(
        db=db,
        cfg=cfg,
        logger=logger,
        retries_per_chunk=args.retries_per_chunk,
        retry_sleep_sec=args.retry_sleep_sec,
        skip_completed=args.skip_completed,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
