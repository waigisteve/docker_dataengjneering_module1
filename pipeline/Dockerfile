FROM python:3.13.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    pyarrow \
    psycopg2-binary \
    pgcli \
    notebook\
    ipykernel

# Optionally install UV
# RUN curl -LsSf https://raw.githubusercontent.com/astral-sh/uv/main/install.py | python3 -

COPY pyproject.toml uv.lock ./

# Copy pipeline scripts
COPY load_parquet_to_postgres.py /app/

# Ensure processed folder exists
RUN mkdir -p /app/processed

EXPOSE 8888

ENTRYPOINT ["python3", "-u", "load_parquet_to_postgres.py"]
