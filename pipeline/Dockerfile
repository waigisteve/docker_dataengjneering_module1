# Base image with Python
FROM python:3.13.11-slim

# Set working directory inside container
WORKDIR /code

# Copy the Python script into the container
COPY pipeline.py .

# Install required packages
RUN pip install --no-cache-dir pandas pyarrow

# Default command to run the script
ENTRYPOINT ["python", "pipeline.py"]
