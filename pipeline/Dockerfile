# Base image with Python
FROM python:3.13.11-slim

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /code

# Ensure uv virtualenv is used
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first (for Docker layer caching)
COPY pyproject.toml .python-version uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Copy application code
COPY pipeline.py .

# Run the pipeline
ENTRYPOINT ["python", "pipeline.py"]
