FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y bash gcc libpq-dev curl git && \
    rm -rf /var/lib/apt/lists/*

# Install uv (reliable path inside Docker)
RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen

COPY load_parquet_to_postgres.py /app/
RUN mkdir -p /app/processed

EXPOSE 8888
CMD ["bash"]
