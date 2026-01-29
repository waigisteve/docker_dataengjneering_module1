# Base image
FROM python:3.13.11-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies and Python packages
RUN apt-get update && apt-get install -y gcc libpq-dev && \
    pip install --no-cache-dir pandas sqlalchemy pyarrow psycopg2-binary && \
    rm -rf /var/lib/apt/lists/*

# Copy the pipeline script
COPY load_parquet_to_postgres.py /app/

# Ensure processed folder exists (runtime will persist if mounted)
RUN mkdir -p /app/processed

# Default command to run the pipeline script
ENTRYPOINT ["python3", "-u", "load_parquet_to_postgres.py"]
