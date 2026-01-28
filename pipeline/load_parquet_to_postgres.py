import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --- CONFIG FROM ENV ---
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "ny_taxi_pg")  # Docker Compose service name
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "root")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "root")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ny_taxi")
TABLE_NAME = "inventory"

# --- DETERMINE FILE PATH ---
script_dir = os.path.dirname(__file__)
parquet_file = os.path.join(script_dir, "output_month_01.parquet")

if not os.path.exists(parquet_file):
    raise FileNotFoundError(f"Parquet file not found at: {parquet_file}")

# --- WAIT FOR POSTGRES ---
max_retries = 60  # ~2 minutes
retry_count = 0
engine = None

while retry_count < max_retries:
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))  # SQLAlchemy 2.x requires text()
        print(f"Connected to Postgres at {POSTGRES_HOST}:{POSTGRES_PORT}", flush=True)
        break
    except Exception as e:
        retry_count += 1
        print(f"Waiting for Postgres to be ready (attempt {retry_count}/{max_retries}): {e}", flush=True)
        time.sleep(2)
else:
    raise RuntimeError(f"Could not connect to Postgres after {max_retries} retries")

# --- LOAD PARQUET ---
print(f"Loading Parquet file: {parquet_file}", flush=True)
try:
    df = pd.read_parquet(parquet_file, engine='pyarrow')
    print(f"Loaded {len(df)} rows from {parquet_file}", flush=True)
except Exception as e:
    raise RuntimeError(f"Error reading Parquet file: {e}")

# --- WRITE TO POSTGRES ---
try:
    df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, chunksize=1000)
    print(f"Successfully inserted {len(df)} rows into {TABLE_NAME}", flush=True)
except SQLAlchemyError as e:
    print(f"Error inserting into Postgres: {e}", flush=True)
finally:
    if engine:
        engine.dispose()
