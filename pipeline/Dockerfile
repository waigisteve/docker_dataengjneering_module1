# Base image with Python
FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
# Set working directory inside container
COPY pyproject.toml .python-version uv.lock ./
WORKDIR /code

# Copy the Python script into the container
COPY pipeline/pipeline.py .

# Install required packages
# RUN pip install --no-cache-dir pandas pyarrow

# Default command to run the script
# ENTRYPOINT ["python", "pipeline.py"]
