# Base image
FROM python:3.13.11-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies and Python packages
RUN apt-get update && apt-get install -y gcc libpq-dev && \
    pip install --no-cache-dir pandas sqlalchemy pyarrow psycopg2-binary && \
    rm -rf /var/lib/apt/lists/*

# Copy the pipeline scripts into /app
# Make sure both load_parquet_to_postgres.py and output_month_01.parquet are in the same folder as this Dockerfile
COPY . /app

# Default command to run the pipeline script
ENTRYPOINT ["python3", "-u", "load_parquet_to_postgres.py"]
