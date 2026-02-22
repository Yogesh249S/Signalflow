# =====================================================================
# processing/Dockerfile
# =====================================================================
# OPTIMISATION CHANGES vs original
# ---------------------------------
# 1. PIN DEPENDENCY VERSIONS via requirements.txt (same as ingestion)
# 2. LAYER CACHING: copy requirements.txt before source code
# 3. NON-ROOT USER: runs as unprivileged `appuser`
# 4. python-snappy added for Kafka consumer-side snappy decompression
# =====================================================================

FROM python:3.11-slim

# snappy compression requires the native library
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsnappy-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# CHANGE: requirements first for Docker layer caching
COPY processing/requirements.txt ./processing/requirements.txt
RUN pip install --no-cache-dir -r processing/requirements.txt

# CHANGE: non-root user
RUN useradd --no-create-home --shell /bin/false appuser

COPY . .

USER appuser

CMD ["python", "-m", "processing.main_processor"]
