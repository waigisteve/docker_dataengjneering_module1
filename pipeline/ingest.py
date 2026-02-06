#!/usr/bin/env python3
"""
ingest.py

Stream NYC TLC CSV into Postgres using COPY with:
- resume (etl_load_progress)
- per-chunk retry (rollback + exponential backoff)
- file + console logging
- CLI params override environment
- can run with NO env by using --prompt-password / --pg-pass-file / --env-file

Security note:
- This script intentionally DOES NOT accept a plain-text password via CLI flag.
  Use env (POSTGRES_PASSWORD), --pg-pass-file, or --prompt-password.

Examples:

1) Using exported env (docker-compose env_file doesn't apply to host shell):
   cd /workspaces/docker_dataengjneering_module1
   set -a && source .env && set +a
   cd pipeline
   uv run python ingest.py

2) Load env file without exporting:
   uv run python ingest.py --env-file ../.env

3) Prompt for password (no secrets on CLI):
   uv run python ingest.py --env-file ../.env --prompt-password

4) Password from file (non-interactive, safer than CLI):
   uv run python ingest.py --env-file ../.env --pg-pass-file ../.pg_pass
"""

# ----------------------------
# Dependencies (all at top)
# ----------------------------
from __future__ import annotations

import argparse
import getpass
import logging
import os
import sys
import time
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from tqdm import tqdm


# ----------------------------
# Data classes
# ----------------------------
@dataclass(frozen=True)
class DbConfig:
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
    retries: int
    backoff_base: int
    log_file: str
    low_memory: bool
    dtype_store_flag: str
    int_like_cols: list[str]


# ----------------------------
# Constants
# ----------------------------
DEFAULT_CSV_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
)
DEFAULT_INT_LIKE_COLS = ["VendorID", "RatecodeID", "passenger_count", "payment_type"]

PROGRESS_DDL = """
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


# ----------------------------
# Utility helpers
# ----------------------------
def env_get(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def read_text_file(path: str) -> str:
    return Path(path).expanduser().read_text(encoding="utf-8").strip()


def load_env_file(path: str) -> None:
    """
    Minimal .env loader:
    - supports KEY=VALUE
    - ignores blank lines + comments
    - does NOT overwrite keys already present in os.environ
    """
    p = Path(path).expanduser()
    if not p.exists():
        raise RuntimeError(f"--env-file not found: {p}")

    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


# ----------------------------
# Logging
# ----------------------------
def setup_logging(log_file: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("etl")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ----------------------------
# DB config resolution (CLI overrides env)
# ----------------------------
def read_db_config(args) -> DbConfig:
    if args.env_file:
        load_env_file(args.env_file)

    host = (args.pg_host or env_get("POSTGRES_HOST") or "localhost").strip()
    port = args.pg_port or int(env_get("POSTGRES_PORT") or "5432")
    user = (args.pg_user or env_get("POSTGRES_USER")).strip()
    dbname = (args.pg_db or env_get("POSTGRES_DB")).strip()

    # Password resolution:
    # 1) POSTGRES_PASSWORD env
    # 2) --pg-pass-file
    # 3) --prompt-password
    password = env_get("POSTGRES_PASSWORD")

    if not password and args.pg_pass_file:
        password = read_text_file(args.pg_pass_file)

    if not password and args.prompt_password:
        password = getpass.getpass("Postgres password: ").strip()

    missing = [k for k, v in {
        "user (POSTGRES_USER or --pg-user)": user,
        "database (POSTGRES_DB or --pg-db)": dbname,
        "password (POSTGRES_PASSWORD / --pg-pass-file / --prompt-password)": password,
    }.items() if not v]

    if missing:
        raise RuntimeError(
            "Missing DB settings: " + "; ".join(missing) + "\n\n"
            "Provide them via:\n"
            "  - Environment variables (e.g. source .env)\n"
            "  - --env-file ../.env\n"
            "  - --pg-pass-file ../.pg_pass\n"
            "  - --prompt-password\n\n"
            "Security: password is intentionally not accepted as a plain CLI argument."
        )

    return DbConfig(host=host, port=port, user=user, password=password, dbname=dbname)


def make_engine(db: DbConfig):
    return create_engine(f"postgresql+psycopg2://{db.user}:{db.password}@{db.host}:{db.port}/{db.dbname}")


def make_pg_conn(db: DbConfig):
    conn = psycopg2.connect(host=db.host, port=db.port, user=db.user, password=db.password, dbname=db.dbname)
    conn.autocommit = False
    return conn


# ----------------------------
# Progress table helpers
# ----------------------------
def ensure_progress_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(PROGRESS_DDL))


def get_progress(engine, load_id: str) -> tuple[int, int]:
    q = text("SELECT rows_loaded, chunks_loaded FROM etl_load_progress WHERE load_id = :load_id")
    with engine.begin() as conn:
        row = conn.execute(q, {"load_id": load_id}).fetchone()
    if row is None:
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
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "load_id": cfg.load_id,
                "table_name": cfg.table_name,
                "source_url": cfg.csv_url,
                "chunk_size": cfg.chunk_size,
                "rows_loaded": rows_loaded,
                "chunks_loaded": chunks_loaded,
                "error": error,
            },
        )


def get_existing_table_columns(engine, table_name: str) -> list[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :t
                ORDER BY ordinal_position
            """),
            {"t": table_name},
        ).fetchall()
    return [r[0] for r in rows]


# ----------------------------
# COPY helpers
# ----------------------------
def normalize_types(df: pd.DataFrame, int_like_cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in int_like_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df


def make_copy_sql(table_name: str, cols: list[str]) -> str:
    cols_sql = ", ".join([f'"{c}"' for c in cols])
    return f'COPY {table_name} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)'


def copy_chunk(pg_conn, copy_sql: str, cols: list[str], df: pd.DataFrame, int_like_cols: list[str]) -> None:
    df = df[cols]
    df = normalize_types(df, int_like_cols)

    buf = StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    with pg_conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)


def copy_with_retry(
    logger: logging.Logger,
    pg_conn,
    copy_sql: str,
    cols: list[str],
    df: pd.DataFrame,
    int_like_cols: list[str],
    chunk_no: int,
    retries: int,
    backoff_base: int,
) -> None:
    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            copy_chunk(pg_conn, copy_sql, cols, df, int_like_cols)
            return
        except Exception as e:
            last_err = e
            try:
                pg_conn.rollback()
            except Exception:
                pass

            wait_s = backoff_base ** attempt
            logger.error(f"chunk={chunk_no} attempt={attempt}/{retries} FAILED: {repr(e)} | retry_in={wait_s}s")
            time.sleep(wait_s)

    assert last_err is not None
    raise last_err


# ----------------------------
# CSV iterator
# ----------------------------
def build_csv_iterator(cfg: JobConfig, rows_loaded: int) -> Iterable[pd.DataFrame]:
    """
    Resumable CSV iterator: skips previously loaded data rows but keeps header.

    NOTE:
    pandas can yield a final empty DataFrame in some edge cases when skiprows
    aligns exactly with EOF. We handle that in the main loop by breaking on empty.
    """
    return pd.read_csv(
        cfg.csv_url,
        iterator=True,
        chunksize=cfg.chunk_size,
        dtype={"store_and_fwd_flag": cfg.dtype_store_flag},
        low_memory=cfg.low_memory,
        skiprows=range(1, rows_loaded + 1),
    )


# ----------------------------
# Main load routine
# ----------------------------
def run_load(db: DbConfig, cfg: JobConfig, logger: logging.Logger) -> int:
    engine = make_engine(db)
    pg_conn = make_pg_conn(db)

    ensure_progress_table(engine)

    rows_loaded, chunks_loaded = get_progress(engine, cfg.load_id)
    logger.info(f"[progress] rows_loaded={rows_loaded} chunks_loaded={chunks_loaded}")
    logger.info(f"Target: table={cfg.table_name} load_id={cfg.load_id} chunk_size={cfg.chunk_size}")

    df_iter = build_csv_iterator(cfg, rows_loaded)

    try:
        if rows_loaded == 0:
            first = next(df_iter)  # StopIteration if empty
            if first is None or first.empty:
                logger.info("[done] first chunk is empty")
                return 0

            cols = list(first.columns)

            # Create empty table schema
            first.head(0).to_sql(cfg.table_name, con=engine, if_exists="replace", index=False)
            copy_sql = make_copy_sql(cfg.table_name, cols)

            t0 = time.time()
            copy_with_retry(
                logger=logger,
                pg_conn=pg_conn,
                copy_sql=copy_sql,
                cols=cols,
                df=first,
                int_like_cols=cfg.int_like_cols,
                chunk_no=1,
                retries=cfg.retries,
                backoff_base=cfg.backoff_base,
            )
            pg_conn.commit()

            rows_loaded += len(first)
            chunks_loaded += 1
            upsert_progress(engine, cfg, rows_loaded, chunks_loaded, error=None)
            logger.info(f"[ok] copied chunk={chunks_loaded} total_rows={rows_loaded} sec={time.time()-t0:.2f}")

        else:
            cols = get_existing_table_columns(engine, cfg.table_name)
            if not cols:
                raise RuntimeError(
                    f"Resume requested (rows_loaded={rows_loaded}) but table '{cfg.table_name}' not found or has no columns."
                )
            copy_sql = make_copy_sql(cfg.table_name, cols)

        # Remaining chunks (break on empty chunk)
        for df_chunk in tqdm(df_iter, desc="Loading"):
            # IMPORTANT: do not treat empty chunks as success
            if df_chunk is None or df_chunk.empty:
                logger.info("[done] reached end of file (empty chunk)")
                break

            chunk_no = chunks_loaded + 1
            t0 = time.time()

            copy_with_retry(
                logger=logger,
                pg_conn=pg_conn,
                copy_sql=copy_sql,
                cols=cols,
                df=df_chunk,
                int_like_cols=cfg.int_like_cols,
                chunk_no=chunk_no,
                retries=cfg.retries,
                backoff_base=cfg.backoff_base,
            )
            pg_conn.commit()

            rows_loaded += len(df_chunk)
            chunks_loaded += 1
            upsert_progress(engine, cfg, rows_loaded, chunks_loaded, error=None)

            logger.info(
                f"[ok] copied chunk={chunks_loaded} rows_in_chunk={len(df_chunk)} total_rows={rows_loaded} sec={time.time()-t0:.2f}"
            )

        logger.info(f"[complete] rows_loaded={rows_loaded} chunks_loaded={chunks_loaded}")

        # Quick sanity output
        try:
            n = pd.read_sql(f'SELECT COUNT(*) AS n FROM "{cfg.table_name}"', con=engine)
            logger.info(f"Row count: {n.iloc[0,0]}")
        except Exception as e:
            logger.warning(f"Could not query final row count: {repr(e)}")

        return 0

    except StopIteration:
        logger.info("[done] no data (iterator empty)")
        return 0

    except Exception as e:
        try:
            pg_conn.rollback()
        except Exception:
            pass

        upsert_progress(engine, cfg, rows_loaded, chunks_loaded, error=str(e))
        logger.error("[FAILED] saved progress for resume on next run")
        logger.error(f"rows_loaded={rows_loaded} chunks_loaded={chunks_loaded} error={repr(e)}")
        return 1

    finally:
        try:
            pg_conn.close()
        except Exception:
            pass


# ----------------------------
# CLI / Arg parsing
# ----------------------------
def parse_args(argv: list[str]):
    p = argparse.ArgumentParser(
        description="Stream CSV to Postgres using COPY with resume+retry+logging.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Job args
    p.add_argument("--csv-url", default=None, help="CSV URL (gz supported)")
    p.add_argument("--table", default=None, help="Target table name")
    p.add_argument("--load-id", default=None, help="Progress key for resume")
    p.add_argument("--chunk-size", type=int, default=None, help="Chunk size for streaming")
    p.add_argument("--retries", type=int, default=None, help="Retries per chunk")
    p.add_argument("--backoff-base", type=int, default=None, help="Exponential backoff base")
    p.add_argument("--log-file", default=None, help="Log file path")
    p.add_argument("--low-memory", action="store_true", default=False, help="pandas low_memory")
    p.add_argument("--dtype-store-flag", default="string", help="dtype for store_and_fwd_flag")
    p.add_argument(
        "--int-like-cols",
        default=",".join(DEFAULT_INT_LIKE_COLS),
        help="Comma-separated list of columns to coerce to integer-like (nullable Int64)",
    )

    # DB args (CLI overrides env)
    p.add_argument("--pg-host", default=None, help="Postgres host (overrides POSTGRES_HOST)")
    p.add_argument("--pg-port", type=int, default=None, help="Postgres port (overrides POSTGRES_PORT)")
    p.add_argument("--pg-user", default=None, help="Postgres user (overrides POSTGRES_USER)")
    p.add_argument("--pg-db", default=None, help="Postgres database (overrides POSTGRES_DB)")

    # Secure password sources (no plain --pg-password flag)
    p.add_argument("--prompt-password", action="store_true", help="Prompt for password (no echo)")
    p.add_argument("--pg-pass-file", default=None, help="Read password from file")
    p.add_argument("--env-file", default=None, help="Optional .env path to load (sets missing env vars only)")

    args = p.parse_args(argv)

    # Defaults from env (non-secret values are okay)
    csv_url = args.csv_url or env_get("CSV_URL") or DEFAULT_CSV_URL
    table = args.table or env_get("TABLE_NAME") or "yellow_taxi_data"
    load_id = args.load_id or env_get("LOAD_ID") or "yellow_2021_01"
    chunk_size = args.chunk_size or int(env_get("CHUNK_SIZE") or "50000")
    retries = args.retries or int(env_get("RETRIES") or "3")
    backoff_base = args.backoff_base or int(env_get("BACKOFF_BASE") or "2")
    log_file = args.log_file or env_get("LOG_FILE") or f"etl_{load_id}.log"

    int_like_cols = [c.strip() for c in (args.int_like_cols or "").split(",") if c.strip()]
    if not int_like_cols:
        int_like_cols = DEFAULT_INT_LIKE_COLS.copy()

    job_cfg = JobConfig(
        csv_url=csv_url,
        table_name=table,
        load_id=load_id,
        chunk_size=chunk_size,
        retries=retries,
        backoff_base=backoff_base,
        log_file=log_file,
        low_memory=args.low_memory,
        dtype_store_flag=args.dtype_store_flag,
        int_like_cols=int_like_cols,
    )

    return args, job_cfg


def main(argv: list[str]) -> int:
    args, cfg = parse_args(argv)
    db = read_db_config(args)
    logger = setup_logging(cfg.log_file)

    # Do not log passwords
    logger.info(f"DB: host={db.host} port={db.port} db={db.dbname} user={db.user}")
    logger.info(f"Job: csv_url={cfg.csv_url} table={cfg.table_name} load_id={cfg.load_id}")
    logger.info(f"Log file: {cfg.log_file}")

    return run_load(db=db, cfg=cfg, logger=logger)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
