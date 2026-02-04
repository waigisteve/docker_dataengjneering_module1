#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pandas as pd


# In[14]:


import sys


# In[15]:


pd.__file__


# In[16]:


sys.executable


# In[17]:


df = pd.read_csv(https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz)


# In[19]:


url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"


# In[20]:


df = pd.read_csv(url)


# In[21]:


PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
FILENAME = "yellow_tripdata_2021-01.csv.gz"

url = f"{PREFIX}/{FILENAME}"


# In[22]:


df = pd.read_csv(url)


# In[23]:


df.head()


# In[24]:


len(df)


# In[25]:


df


# In[26]:


df.dtypes


# In[27]:


df.shape


# In[28]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    low_memory=False,
)


# In[29]:


df.head()


# In[30]:


df 


# In[31]:


df['tpep_pickup_datetime']


# In[32]:


get_ipython().system('uv add sqlalchemy')


# In[36]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# below approach not production friedly as credentials are hardcoded

# In[38]:


from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql+psycopg2://root:root@ny_taxi_pg:5432/ny_taxi")
print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))


# below option is more production ready as it reads from a .env file 

# In[39]:


import os
import pandas as pd
from sqlalchemy import create_engine

def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing env var: {name}. "
            f"Make sure docker-compose has `env_file: - .env` for the jupyter service."
        )
    return value

PG_HOST = os.environ.get("POSTGRES_HOST", "ny_taxi_pg")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = require_env("POSTGRES_USER")
PG_PASS = require_env("POSTGRES_PASSWORD")
PG_DB   = require_env("POSTGRES_DB")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))


# In[40]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[45]:


csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=100_000,
    dtype={
        "store_and_fwd_flag": "string"
    }
)


# In[46]:


df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=100_000,
    low_memory=False
)


# In[47]:


for df_chunk in df_iter:
    print(len(df_chunk))


# In[48]:


df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[49]:


uv add tqdm


# In[1]:


from tqdm import tqdm


# In[2]:


from tqdm.auto import tqdm

for df_chunk in tqdm(df_iter):
    ...


# In[3]:


import pandas as pd

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=100_000,
    dtype={"store_and_fwd_flag": "string"}
)


# In[6]:


from tqdm.auto import tqdm

for df_chunk in tqdm(df_iter):
    ...


# In[7]:


import pandas as pd
from tqdm import tqdm  # <- avoids ipywidgets warning

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=100_000,
    dtype={"store_and_fwd_flag": "string"},
)

for df_chunk in tqdm(df_iter, total=14):
    print(len(df_chunk))


# In[8]:


from tqdm.auto import tqdm

for df_chunk in tqdm(df_iter, total=14):
    print(len(df_chunk))


# In[9]:


from tqdm import tqdm

for i, df_chunk in enumerate(tqdm(df_iter, total=14), start=1):
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"Inserted chunk {i}")


# In[10]:


from tqdm import tqdm

for i, df_chunk in enumerate(tqdm(df_iter, total=14), start=1):
    print(f"Chunk {i}, rows={len(df_chunk)}")


# In[12]:


from tqdm import tqdm

print("Starting insert loop...")

for i, df_chunk in enumerate(tqdm(df_iter, total=14), start=1):
    print(f"Chunk {i} loaded, inserting...")
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"Inserted chunk {i}")


# In[13]:


try:
    first = next(df_iter)
    print("Iterator is alive. First chunk rows:", len(first))
except StopIteration:
    print("Iterator is exhausted. Recreate df_iter.")


# In[14]:


import pandas as pd
from tqdm import tqdm

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

# Recreate iterator (because it was exhausted)
df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=100_000,
    dtype={"store_and_fwd_flag": "string"},
    low_memory=False,
)

# Prefetch first chunk (this also proves it's alive)
first_chunk = next(df_iter)
print("Prefetched chunk 1 rows:", len(first_chunk))

# Create/replace table schema
first_chunk.head(0).to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="replace",
    index=False,
)

# Insert chunk 1
first_chunk.to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="append",
    index=False,
    method="multi",
)
print("Inserted chunk 1")

# Insert remaining chunks with progress
for i, df_chunk in enumerate(tqdm(df_iter, total=13), start=2):
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"Inserted chunk {i}")


# In[15]:


import os
from sqlalchemy import create_engine

engine = create_engine(
    f"postgresql+psycopg2://"
    f"{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@"
    f"{os.environ.get('POSTGRES_HOST', 'ny_taxi_pg')}:"
    f"{os.environ.get('POSTGRES_PORT', 5432)}/"
    f"{os.environ['POSTGRES_DB']}"
)

print("Engine created OK")


# In[ ]:


import pandas as pd
from tqdm import tqdm

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=100_000,
    dtype={"store_and_fwd_flag": "string"},
    low_memory=False,
)

first_chunk = next(df_iter)
print("Prefetched chunk 1 rows:", len(first_chunk))

first_chunk.head(0).to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="replace",
    index=False,
)

first_chunk.to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="append",
    index=False,
    method="multi",
)
print("Inserted chunk 1")

for i, df_chunk in enumerate(tqdm(df_iter, total=13), start=2):
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"Inserted chunk {i}")


# In[1]:


from tqdm import tqdm

for i, df_chunk in enumerate(tqdm(df_iter, total=13), start=2):
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"Inserted chunk {i}")


# In[ ]:


import os
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

# recreate engine
engine = create_engine(
    f"postgresql+psycopg2://"
    f"{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@"
    f"{os.environ.get('POSTGRES_HOST','ny_taxi_pg')}:"
    f"{os.environ.get('POSTGRES_PORT',5432)}/"
    f"{os.environ['POSTGRES_DB']}"
)

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=100_000,
    dtype={"store_and_fwd_flag": "string"},
    low_memory=False,
)

# prefetch first chunk
first_chunk = next(df_iter)

# DROP + recreate table schema
first_chunk.head(0).to_sql(
    "yellow_taxi_data",
    con=engine,
    if_exists="replace",
    index=False,
)

# insert all chunks
first_chunk.to_sql(
    "yellow_taxi_data",
    con=engine,
    if_exists="append",
    index=False,
    method="multi",
)
print("Inserted chunk 1")

for i, df_chunk in enumerate(tqdm(df_iter, total=13), start=2):
    df_chunk.to_sql(
        "yellow_taxi_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"Inserted chunk {i}")


# In[1]:


import os
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    f"postgresql+psycopg2://"
    f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@{os.environ.get('POSTGRES_HOST','ny_taxi_pg')}:{os.environ.get('POSTGRES_PORT',5432)}"
    f"/{os.environ['POSTGRES_DB']}"
)

pd.read_sql("SELECT COUNT(*) AS n FROM yellow_taxi_data", con=engine)


# In[2]:


import os
import psycopg2
from sqlalchemy import create_engine

PG_HOST = os.environ.get("POSTGRES_HOST", "ny_taxi_pg")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]
PG_DB   = os.environ["POSTGRES_DB"]

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

pg_conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
)

print("DB connections OK")


# In[3]:


import pandas as pd
from tqdm import tqdm
from io import StringIO

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

# Stream CSV in chunks (smaller chunks reduce memory spikes)
df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=50_000,
    dtype={"store_and_fwd_flag": "string"},
    low_memory=False,
)

# Prefetch first chunk
first_chunk = next(df_iter)
print("Prefetched rows:", len(first_chunk))

# Create/replace table schema (empty table with correct columns)
first_chunk.head(0).to_sql("yellow_taxi_data", con=engine, if_exists="replace", index=False)

# Prepare COPY statement (quote column names safely)
cols = list(first_chunk.columns)
cols_sql = ", ".join([f'"{c}"' for c in cols])
copy_sql = f'COPY yellow_taxi_data ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)'

def copy_chunk(conn, chunk: pd.DataFrame):
    # ensure same column order
    chunk = chunk[cols]

    # write chunk to in-memory CSV
    buf = StringIO()
    chunk.to_csv(buf, index=False, header=False)
    buf.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()

# COPY first chunk
copy_chunk(pg_conn, first_chunk)
print("Copied chunk 1")

# COPY the rest with tqdm
for i, chunk in enumerate(tqdm(df_iter), start=2):
    copy_chunk(pg_conn, chunk)
    if i % 5 == 0:
        print(f"Copied chunk {i}", flush=True)

print("Done")


# In[4]:


import pandas as pd
pd.read_sql("SELECT COUNT(*) AS n FROM yellow_taxi_data", con=engine)


# In[7]:


import pandas as pd
from tqdm import tqdm

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

n_loaded = int(pd.read_sql("SELECT COUNT(*) AS n FROM yellow_taxi_data", con=engine).iloc[0,0])
print("Rows already loaded:", n_loaded)

df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=50_000,
    dtype={"store_and_fwd_flag": "string"},
    low_memory=False,
    skiprows=range(1, n_loaded + 1),  # skip header handled by range starting at 1
)

# continue COPY
for i, chunk in enumerate(tqdm(df_iter), start=1):
    copy_chunk(pg_conn, chunk)
    if i % 5 == 0:
        print(f"Resumed: copied {i} more chunks", flush=True)

print("Resume done")


# In[6]:


pg_conn.rollback()
print("Rolled back")


# In[8]:


pg_conn.rollback()


# In[9]:


import pandas as pd
from io import StringIO

INT_LIKE_COLS = ["VendorID", "RatecodeID", "passenger_count", "payment_type"]

def normalize_types(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = chunk.copy()
    for c in INT_LIKE_COLS:
        if c in chunk.columns:
            chunk[c] = pd.to_numeric(chunk[c], errors="coerce").astype("Int64")
    return chunk

def copy_chunk(conn, chunk: pd.DataFrame):
    chunk = chunk[cols]
    chunk = normalize_types(chunk)

    buf = StringIO()
    chunk.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()


# In[10]:


import pandas as pd
from tqdm import tqdm

csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

n_loaded = int(pd.read_sql("SELECT COUNT(*) AS n FROM yellow_taxi_data", con=engine).iloc[0,0])
print("Rows already loaded:", n_loaded)

df_iter = pd.read_csv(
    csv_url,
    iterator=True,
    chunksize=50_000,
    dtype={"store_and_fwd_flag": "string"},
    low_memory=False,
    skiprows=range(1, n_loaded + 1),
)

for i, chunk in enumerate(tqdm(df_iter), start=1):
    copy_chunk(pg_conn, chunk)

print("Resume done")


# In[14]:


pd.read_sql("SELECT COUNT(*) AS n FROM yellow_taxi_data", con=engine)


# In[15]:


pd.read_sql("SELECT * FROM yellow_taxi_data LIMIT 5", con=engine)


# In[16]:


pd.read_sql("""
SELECT
  MIN(tpep_pickup_datetime) AS min_pickup,
  MAX(tpep_pickup_datetime) AS max_pickup
FROM yellow_taxi_data
""", con=engine)


# this script truncates the table in case the process fails midway usually due to memory pressure

# In[3]:


import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- Recreate engine ---
PG_HOST = os.environ.get("POSTGRES_HOST", "ny_taxi_pg")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]
PG_DB   = os.environ["POSTGRES_DB"]

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

TABLE_NAME = "yellow_taxi_data"
LOAD_ID = "yellow_2021_01"

with engine.begin() as conn:
    # 1) Ensure progress table exists (so DELETE won't fail)
    conn.execute(text("""
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
    """))

    # 2) Truncate data table if it exists (skip if not yet created)
    conn.execute(text(f'DO $$ BEGIN '
                      f'IF to_regclass(\'public."{TABLE_NAME}"\') IS NOT NULL THEN '
                      f'EXECUTE \'TRUNCATE TABLE "{TABLE_NAME}"\'; '
                      f'END IF; '
                      f'END $$;'))

    # 3) Reset progress for this load id
    conn.execute(text("DELETE FROM etl_load_progress WHERE load_id = :load_id"), {"load_id": LOAD_ID})

# Sanity check
pd.read_sql(f"""
SELECT
  COALESCE((SELECT COUNT(*) FROM "{TABLE_NAME}"), 0) AS table_rows,
  (SELECT COUNT(*) FROM etl_load_progress WHERE load_id = '{LOAD_ID}') AS progress_rows
""", con=engine)


# this is a comprehensive script that includes all the main dependencies, retry measures incase of failue rollbacks and logging measures with resumption mechanism upon server restart 

# In[4]:


import os
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from tqdm import tqdm
from io import StringIO

# ----------------------------
# CONFIG
# ----------------------------
CSV_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
TABLE_NAME = "yellow_taxi_data"
LOAD_ID = "yellow_2021_01"          # change if you load a different file
CHUNK_SIZE = 50_000
INT_LIKE_COLS = ["VendorID", "RatecodeID", "passenger_count", "payment_type"]

# Read DB creds from environment (docker-compose env_file injects these)
PG_HOST = os.environ.get("POSTGRES_HOST", "ny_taxi_pg")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]
PG_DB   = os.environ["POSTGRES_DB"]

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB)
pg_conn.autocommit = False

# ----------------------------
# HELPERS
# ----------------------------
def ensure_progress_table():
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
    with engine.begin() as conn:
        conn.execute(text(ddl))

def get_progress():
    q = text("SELECT rows_loaded, chunks_loaded FROM etl_load_progress WHERE load_id = :load_id")
    with engine.begin() as conn:
        row = conn.execute(q, {"load_id": LOAD_ID}).fetchone()
    if row is None:
        return 0, 0
    return int(row[0]), int(row[1])

def upsert_progress(rows_loaded: int, chunks_loaded: int, error: str | None = None):
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
        conn.execute(sql, {
            "load_id": LOAD_ID,
            "table_name": TABLE_NAME,
            "source_url": CSV_URL,
            "chunk_size": CHUNK_SIZE,
            "rows_loaded": rows_loaded,
            "chunks_loaded": chunks_loaded,
            "error": error
        })

def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in INT_LIKE_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def make_copy_sql(cols):
    cols_sql = ", ".join([f'"{c}"' for c in cols])
    return f'COPY {TABLE_NAME} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)'

def copy_chunk(conn, copy_sql: str, cols: list[str], df: pd.DataFrame):
    df = df[cols]
    df = normalize_types(df)

    buf = StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)

# ----------------------------
# MAIN LOAD (resume-safe)
# ----------------------------
ensure_progress_table()

rows_loaded, chunks_loaded = get_progress()
print(f"[progress] rows_loaded={rows_loaded} chunks_loaded={chunks_loaded}")

# (Re)create the iterator, skipping already loaded rows
df_iter = pd.read_csv(
    CSV_URL,
    iterator=True,
    chunksize=CHUNK_SIZE,
    dtype={"store_and_fwd_flag": "string"},
    low_memory=False,
    skiprows=range(1, rows_loaded + 1)  # skip data rows; keep header
)

# If we're starting fresh, we need to create the table schema
# If resuming, we must not replace the table.
try:
    if rows_loaded == 0:
        first = next(df_iter)
        cols = list(first.columns)

        # Create empty table schema
        first.head(0).to_sql(TABLE_NAME, con=engine, if_exists="replace", index=False)
        copy_sql = make_copy_sql(cols)

        # Copy first chunk
        copy_chunk(pg_conn, copy_sql, cols, first)
        pg_conn.commit()

        rows_loaded += len(first)
        chunks_loaded += 1
        upsert_progress(rows_loaded, chunks_loaded, error=None)
        print(f"[ok] copied chunk {chunks_loaded} total_rows={rows_loaded}")
    else:
        # Get column order from the existing table (important!)
        with engine.begin() as conn:
            cols = [r[0] for r in conn.execute(text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :t
                ORDER BY ordinal_position
            """), {"t": TABLE_NAME}).fetchall()]
        copy_sql = make_copy_sql(cols)

    # Continue streaming remaining chunks
    for df_chunk in tqdm(df_iter):
        copy_chunk(pg_conn, copy_sql, cols, df_chunk)
        pg_conn.commit()

        rows_loaded += len(df_chunk)
        chunks_loaded += 1
        upsert_progress(rows_loaded, chunks_loaded, error=None)

except StopIteration:
    print("[done] no data (iterator empty)")

except Exception as e:
    # Roll back DB transaction so the connection can be reused after restart
    try:
        pg_conn.rollback()
    except Exception:
        pass

    # Persist the failure info for next run
    upsert_progress(rows_loaded, chunks_loaded, error=str(e))

    print("\n[FAILED]")
    print("Saved progress so you can rerun this cell to resume.")
    print("rows_loaded =", rows_loaded)
    print("chunks_loaded =", chunks_loaded)
    print("error =", repr(e))
    raise

finally:
    # Keep connections open if you want; otherwise close them
    # pg_conn.close()
    pass

print(f"\n[complete] rows_loaded={rows_loaded} chunks_loaded={chunks_loaded}")
print(pd.read_sql(f"SELECT COUNT(*) AS n FROM {TABLE_NAME}", con=engine))
print(pd.read_sql("SELECT * FROM etl_load_progress WHERE load_id = %s" % repr(LOAD_ID), con=engine))

