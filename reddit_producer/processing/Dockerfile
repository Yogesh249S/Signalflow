FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libsnappy-dev libpq-dev curl && rm -rf /var/lib/apt/lists/*
COPY processing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_md
COPY processing/ ./processing/
COPY storage/    ./storage/
CMD ["python", "-m", "processing.main_processor"]
