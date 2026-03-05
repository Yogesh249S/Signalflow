FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libsnappy-dev libpq-dev && rm -rf /var/lib/apt/lists/*
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion/ ./ingestion/
COPY storage/   ./storage/
CMD ["python", "-m", "ingestion.main"]
