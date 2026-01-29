import os
import time
import glob
import shutil
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

# --- CONFIG FROM ENV ---
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "ny_taxi_pg")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "root")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "root")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ny_taxi")
TABLE_NAME = "inventory"

# --- DETERMINE FOLDERS ---
script_dir = os.path.dirname(__file__)
WATCH_FOLDER = script_dir
PROCESSED_FOLDER = os.path.join(script_dir, "processed")
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# --- WAIT FOR POSTGRES ---
def wait_for_postgres(max_retries=60, retry_interval=2):
    retry_count = 0
    engine = None
    while retry_count < max_retries:
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"Connected to Postgres at {POSTGRES_HOST}:{POSTGRES_PORT}", flush=True)
            return engine
        except Exception as e:
            retry_count += 1
            print(f"Waiting for Postgres (attempt {retry_count}/{max_retries}): {e}", flush=True)
            time.sleep(retry_interval)
    raise RuntimeError(f"Could not connect to Postgres after {max_retries} retries")

engine = wait_for_postgres()
inspector = inspect(engine)

# --- WATCH LOOP ---
POLL_INTERVAL = 5  # seconds
print(f"Watching folder {WATCH_FOLDER} for Parquet files...", flush=True)

try:
    while True:
        parquet_files = glob.glob(os.path.join(WATCH_FOLDER, "*.parquet"))
        for parquet_file in parquet_files:
            file_name = os.path.basename(parquet_file)
            processed_path = os.path.join(PROCESSED_FOLDER, file_name)

            if os.path.exists(processed_path):
                continue

            print(f"Loading Parquet file: {parquet_file}", flush=True)
            try:
                df = pd.read_parquet(parquet_file, engine='pyarrow')
                print(f"Loaded {len(df)} rows from {file_name}", flush=True)
            except Exception as e:
                print(f"Error reading {file_name}: {e}", flush=True)
                continue

            # --- FILTER COLUMNS THAT EXIST IN TABLE ---
            if inspector.has_table(TABLE_NAME):
                table_columns = [col['name'] for col in inspector.get_columns(TABLE_NAME)]
                df = df[[c for c in df.columns if c in table_columns]]
                print(f"Columns after filtering to match table: {df.columns.tolist()}", flush=True)

            # --- WRITE TO POSTGRES ---
            try:
                df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, chunksize=1000)
                print(f"Inserted {len(df)} rows into {TABLE_NAME}", flush=True)
                shutil.move(parquet_file, processed_path)
                print(f"Moved {file_name} to processed folder", flush=True)
            except SQLAlchemyError as e:
                print(f"Error inserting into Postgres: {e}", flush=True)

        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    print("Pipeline stopped by user", flush=True)
finally:
    if engine:
        engine.dispose()
