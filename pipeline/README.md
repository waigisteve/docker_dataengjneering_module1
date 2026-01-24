# Data Engineering Pipeline

A containerized data processing pipeline built with Docker and Python for efficient data transformation and analysis.

## Overview

This project provides a scalable, containerized solution for data engineering tasks using Docker and Python. The pipeline processes data using pandas and Apache Arrow for high-performance data manipulation.

## Prerequisites

- Docker (latest version)
- Docker Compose (optional, for orchestration)

## Project Structure

```
pipeline/
├── Dockerfile          # Container configuration for the pipeline
├── pipeline.py         # Main Python script for data processing
├── README.md          # Project documentation
└── requirements.txt   # Python dependencies (optional)
```

## Setup & Installation

### 1. Build the Docker Image

```bash
docker build -t data-pipeline:latest .
```

### 2. Run the Pipeline

```bash
docker run data-pipeline:latest
```

## Technologies Used

- **Python 3.13.11** - Lightweight slim image for minimal footprint
- **Pandas** - Data manipulation and analysis
- **PyArrow** - Efficient columnar data format support
- **Docker** - Containerization for consistency across environments

## Configuration

The Docker image installs the following Python packages:
- `pandas` - Data analysis and manipulation
- `pyarrow` - Apache Arrow implementation for fast data processing

## Development

### Building with Custom Options

```bash
docker build --no-cache -t data-pipeline:latest .
```

### Running Interactive Mode

```bash
docker run -it data-pipeline:latest /bin/bash
```

## Notes

- The image uses `python:3.13.11-slim` for optimized size and performance
- Dependencies are cached during build for faster rebuilds
- The default entry point runs `pipeline.py` automatically

## Contributing

When modifying the pipeline:
1. Update `pipeline.py` with your changes
2. Test locally with `python pipeline.py`
3. Rebuild the Docker image with `docker build -t data-pipeline:latest .`
4. Commit changes to version control

## License

See LICENSE file in the root repository for details.
