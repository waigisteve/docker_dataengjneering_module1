# Base image
FROM python:3.13-slim

# Working directory
WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Copy dependency metadata FIRST (Docker cache optimization)
COPY pyproject.toml uv.lock ./

# Install dependencies via uv (locked + reproducible)
RUN uv sync --frozen

# Copy application code
COPY load_parquet_to_postgres.py /app/

# Ensure processed folder exists
RUN mkdir -p /app/processed

# Default command
ENTRYPOINT ["python", "-u", "load_parquet_to_postgres.py"]
