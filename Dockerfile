FROM python:3.11-slim

WORKDIR /app

# System deps (optional; useful for some excel/duckdb backends)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY Tenable_Export_Suite.py README.md EXAMPLES.md POWERBI_MODEL.md .env.example ./

# Default output directory
RUN mkdir -p /app/exports /app/logs

# Environment: user will pass TENABLE_ACCESS_KEY / TENABLE_SECRET_KEY at runtime
ENV TES_OUTPUT_DIR=/app/exports

# Default command: parquet + duckdb
CMD ["python", "Tenable_Export_Suite.py", "-o", "parquet", "duckdb", "--output-dir", "/app/exports"]
