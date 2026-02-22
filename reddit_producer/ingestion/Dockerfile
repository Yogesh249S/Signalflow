# =====================================================================
# ingestion/Dockerfile
# =====================================================================
# OPTIMISATION CHANGES vs original
# ---------------------------------
# 1. PIN DEPENDENCY VERSIONS
#    Original: `pip install praw kafka-python python-dotenv` — no versions
#    pinned. A new release of any package could silently break the build.
#    New: all packages pinned via requirements.txt (see that file).
#
# 2. asyncpraw + aiokafka ADDED
#    The rewritten scheduler uses asyncpraw (async PRAW) and aiokafka
#    (async Kafka producer) in place of the sync equivalents.
#
# 3. COPY requirements.txt FIRST (Docker layer caching)
#    Original: `COPY . .` then pip install — every code change invalidated
#    the pip cache layer, forcing a full re-download on every rebuild.
#    New: copy requirements.txt first, install, THEN copy source. As long
#    as requirements.txt doesn't change, pip install is served from cache.
#
# 4. NON-ROOT USER for security
#    Original: ran as root inside the container.
#    New: creates an unprivileged `appuser` to run the process.
# =====================================================================

FROM python:3.11-slim

WORKDIR /app

# CHANGE: copy requirements first so Docker caches the pip layer separately
# from the source code. Re-builds on code-only changes are ~10× faster.
COPY ingestion/requirements.txt ./ingestion/requirements.txt
RUN pip install --no-cache-dir -r ingestion/requirements.txt

# CHANGE: create non-root user before copying source
RUN useradd --no-create-home --shell /bin/false appuser

COPY . .

# Drop privileges before running
USER appuser

CMD ["python", "-m", "ingestion.main"]
