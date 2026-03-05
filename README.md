# SignalFlow

Real-time cross-platform social signal intelligence. Ingests content from Reddit, HackerNews, Bluesky, and YouTube, processes it through a dual-path stream pipeline, and surfaces enriched signals — sentiment, velocity, trending score, and cross-platform divergence — via a REST and WebSocket API.

---

## Live

![SignalFlow Grafana Dashboard](./reddit_producer/assets/d1)
(./reddit_producer/assets/d2-1)
(./reddit_producer/assets/d2-2)
(./reddit_producer/assets/d3)

*Topic Intelligence — 1.49K active cross-platform topics, 14 topics live on 4+ platforms simultaneously, sentiment tracked over time, cross-platform lead/lag detection*

---

## How it works

A post appears on r/technology. Within seconds:

1. `asyncpraw` picks it up and publishes to Kafka on two parallel paths — `reddit.posts.raw` (legacy, unchanged) and `signals.normalised` (unified schema shared by all 4 platforms)
2. The processing service runs VADER sentiment on title + body, extracts named entities via spaCy NER, computes score velocity and trending score, and writes to TimescaleDB
3. The topic aggregator buckets signals into 15-minute windows per `(topic, platform)` and fires a cross-platform event when the same topic appears on 2+ platforms simultaneously
4. When the post score jumps from 45 → 1200 five minutes later, velocity recalculates `(1200-45)/300 = 3.85/s`, trending score updates, and a Redis pub/sub push delivers the update to every connected WebSocket client instantly
5. Grafana reads pre-aggregated Postgres views — topic traction timeline, lead/lag heatmap, trending 24h, platform leaderboard

> **Interactive architecture diagram** → [`signal_flow_architecture.html`]((./reddit_producer/assets/signal_flow_architecture.html)) *(open in browser — traces a single post through every layer)*

---

## API

SignalFlow exposes a fully authenticated REST and WebSocket API. Once running, external developers can build their own analysis dashboards directly on top of the enriched signal data — no need to handle ingestion, NLP, or cross-platform normalisation themselves.

### Authentication

Create a superuser, then obtain JWT tokens:

```bash
# Create superuser
docker exec -it reddit_django python manage.py createsuperuser

# Get access + refresh tokens
curl -X POST http://localhost:8080/api/token/ \
  -H "Content-Type: application/json" \
  -d '{"username": "your_username", "password": "your_password"}'

# Response
{
  "access":  "eyJ0eXAiOiJKV1QiL...",   # use this in Authorization header
  "refresh": "eyJ0eXAiOiJKV1QiL..."    # use this to get a new access token
}

# Refresh when access token expires (60 min lifetime)
curl -X POST http://localhost:8080/api/token/refresh/ \
  -H "Content-Type: application/json" \
  -d '{"refresh": "your_refresh_token"}'
```

Use the access token on every request:

```bash
curl http://localhost:8080/api/v1/signals/ \
  -H "Authorization: Bearer your_access_token"
```

### Endpoints

| Method | Endpoint | What it returns |
|---|---|---|
| `GET` | `/api/v1/signals/` | Unified cross-platform feed — all signals with sentiment, velocity, trending score, NER topics |
| `GET` | `/api/v1/pulse/` | Topic sentiment summary — VADER compound scores, named entities, sentiment distribution per topic |
| `GET` | `/api/v1/trending/` | Velocity-ranked trending signals — sorted by `trending_score`, filterable by platform and time window |
| `GET` | `/api/v1/compare/` | Cross-platform divergence events — topics that surfaced on 2+ platforms with spread time and delta score |
| `WS` | `ws://localhost:8080/signals/live/` | Real-time push stream — fires on every processing batch flush, no polling needed |
| `POST` | `/api/token/` | Obtain JWT access + refresh tokens |
| `POST` | `/api/token/refresh/` | Rotate access token using refresh token |
| `GET` | `/health/` | Health check — no auth required |

### Example signal response

```json
{
  "id": "xyz789",
  "platform": "reddit",
  "title": "New breakthrough in quantum computing",
  "raw_score": 1200,
  "score_velocity": 3.85,
  "comment_velocity": 0.14,
  "trending_score": 0.60,
  "is_trending": true,
  "sentiment_compound": 0.34,
  "sentiment_label": "positive",
  "topics": ["quantum computing", "ibm", "breakthrough"],
  "first_seen": "2026-03-05T06:30:00Z",
  "last_updated": "2026-03-05T06:35:00Z"
}
```

### What you can build on top

The API gives you processed, normalised, deduplicated signals across 4 platforms — ready to query. No ingestion infrastructure, no NLP pipeline, no cross-platform schema mapping needed.

- **Sentiment dashboards** — `/api/v1/pulse/` gives per-topic sentiment over time, ready to chart
- **Trend detection tools** — `/api/v1/trending/` returns velocity-ranked signals; filter by platform to compare how topics rise differently across Reddit vs HackerNews vs Bluesky
- **Divergence alerts** — `/api/v1/compare/` surfaces when a topic breaks on one platform before others — useful for early signal detection
- **Live feeds** — the WebSocket endpoint pushes updates the moment a batch flushes, no polling delay

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11 · asyncpraw · aiokafka · asyncio |
| Processing | Python 3.11 · VADER sentiment · spaCy NER |
| Message broker | Apache Kafka 7.5.0 (Confluent) · 3 brokers · RF=3 |
| Database | PostgreSQL 15 + TimescaleDB · WAL streaming replication |
| Cache / Channels | Redis 7 · 4 logical DBs |
| API | Django · Django REST Framework · Django Channels |
| ASGI server | Daphne |
| Observability | Prometheus · Grafana |
| Storage | AWS S3 · signal archive |
| Containers | Docker Compose |

---

## Key design decisions

| Decision | Problem it solves |
|---|---|
| Dual Kafka paths | Legacy Reddit pipeline stays untouched. `signals.normalised` introduces a platform-agnostic schema — all 4 sources write the same shape, one consumer reads one topic instead of four |
| `signals.normalised` has 6 partitions | All 4 sources write to it — higher combined throughput than any single-source topic |
| Manual Kafka offset commit | `consumer.commit()` only fires after confirmed DB write. Crash mid-batch → re-delivers from last committed offset. `ON CONFLICT DO UPDATE` makes re-delivery idempotent |
| Velocity in Redis, not Postgres | 3 processing replicas need shared velocity state. Per-replica in-memory dicts cause split-brain — velocity always 0.0 on the second replica to see a post. Shared Redis DB0 fixes it |
| Topic aggregator as separate container | Failure here doesn't affect ingestion or API serving. Watermark-based polling means it restarts without reprocessing already-aggregated signals |
| Read replica routing | `ReadReplicaRouter` sends all Django `db_for_read()` to the replica. Primary never handles API query load |
| TimescaleDB hypertable | Append-only time-series. Chunk-based retention drops entire day-files — no WAL-bloating row-level DELETEs. Retention window set via `TIMESCALE_RETENTION` |

---

## Getting started

**Prerequisites:** Docker Engine 24+, Docker Compose v2, Reddit OAuth credentials ([reddit.com/prefs/apps](https://reddit.com/prefs/apps)), YouTube Data API key

```bash
git clone https://github.com/Yogesh249S/Reddit_signalflow
cd Reddit_signalflow
cp .env.example .env
```

Fill in `.env` — required values:

```bash
# Reddit
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=signalflow:v1.0 (by /u/your_username)

# YouTube
YOUTUBE_API_KEY=your_api_key
YOUTUBE_CHANNELS=UCknLrEdhRCp1aegoMqRaCZg,...  # comma-separated channel IDs

# Django
DJANGO_SECRET_KEY=generate-a-long-random-string

# AWS (signal archive)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=eu-north-1
S3_BUCKET_NAME=your_bucket
```

```bash
docker compose up -d
docker exec -it reddit_django python manage.py migrate
docker exec -it reddit_django python manage.py createsuperuser
```

Add subreddits at `http://localhost:8080/admin` → Subreddit Configs. Changes propagate within 60 seconds, no restart needed.

| Service | URL |
|---|---|
| Django API | http://localhost:8080/api/ |
| Django Admin | http://localhost:8080/admin/ |
| Grafana | http://localhost:3000 *(admin / value of `GRAFANA_PASSWORD`)* |
| Prometheus | http://localhost:9090 |
| DLQ replay | http://localhost:8001/dlq |

---

## Configuration

All thresholds are environment variables — no rebuild needed.

```bash
# Per-platform rate limits (requests/min)
RATE_LIMIT_REDDIT=10
RATE_LIMIT_HACKERNEWS=5
RATE_LIMIT_BLUESKY=20
RATE_LIMIT_YOUTUBE=10

# Trending score weights (see trending_score.py)
TRENDING_VELOCITY_HIGH=50      # score velocity → +0.4
TRENDING_VELOCITY_LOW=5        # score velocity → +0.2
TRENDING_SENTIMENT_THRESH=0.3  # abs(sentiment)  → +0.2
TRENDING_COMMENT_MIN=10        # comment count   → +0.2
TRENDING_CUTOFF=0.3            # minimum score to flag is_trending

# Infrastructure
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
REDIS_URL=redis://redis:6379/0
TIMESCALE_RETENTION=90
METRICS_PORT=8000
DLQ_REPLAY_PORT=8001
GRAFANA_PASSWORD=admin
```

---

## Project structure

```
apps/reddit/           Django app — models, serializers, views, URLs
config/                Django settings, ASGI routing
reddit_producer/       Ingestion, processing, topic aggregator
reddit_dashboard_v3/   React dashboard
signal_flow_v2.html    Interactive architecture diagram
Dockerfile.django
requirements.txt
```