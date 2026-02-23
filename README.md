# Reddit SignalFlow

A real-time social media intelligence platform built on a production-grade event streaming architecture. Ingests, processes, and analyses Reddit content across 120+ subreddits simultaneously — surfacing trending signals, engagement velocity, and sentiment patterns as they emerge.

---

## Deploy

### Prerequisites

- Docker Engine 24+ and Docker Compose v2
- 8 GB RAM minimum
- Reddit OAuth credentials — [reddit.com/prefs/apps](https://reddit.com/prefs/apps)
- Python 3.11+ and Node 18+ for local development

### 1 — Environment

```bash
git clone https://github.com/Yogesh249S/Reddit_signalflow.git
cd Reddit_signalflow
touch .env
```

#### Reddit OAuth credentials

Go to [reddit.com/prefs/apps](https://reddit.com/prefs/apps), click **"create another app"**, and fill in the form as shown below. Select **"web app"** as the type, set the redirect URI to `http://localhost:8080`, then copy the two values into your `.env`.

![Reddit app setup — annotated](reddit_producer/assets/reddit_app_annotated.png)

- **③ `REDDIT_CLIENT_ID`** — the short string directly below the app type line
- **④ `REDDIT_CLIENT_SECRET`** — shown next to the "secret" label. Never commit this value — keep it in `.env` only and ensure `.env` is in `.gitignore`
- **⑤ `REDDIT_USER_AGENT`** — add your Reddit username as a developer on the app, then use the format `signalflow:v1.0 (by /u/your_username)`

```bash
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=signalflow:v1.0 (by /u/your_username)


POSTGRES_DB=reddit
POSTGRES_USER=reddit
POSTGRES_PASSWORD=reddit
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
REDIS_URL=redis://redis:6379/0
METRICS_PORT=8000
DLQ_REPLAY_PORT=8001
GRAFANA_PASSWORD=admin

TRENDING_VELOCITY_HIGH=50
TRENDING_VELOCITY_LOW=10
TRENDING_SENTIMENT_THRESH=0.5
TRENDING_COMMENT_MIN=100
TRENDING_CUTOFF=0.5
TIMESCALE_RETENTION=90
```

### 2 — Start

```bash
cd reddit_producer
pip install -r requirements.txt
docker compose up --build
```

Wait for all containers to show `healthy`. The `migrate` container runs first and must complete before ingestion starts.

### 3 — Django

```bash
python manage.py migrate
python manage.py createsuperuser
```

### 4 — Load subreddits

```bash
docker exec -i postgres psql -U reddit -d reddit < subreddits.sql
```

Loads 120 pre-categorised subreddits across fast (60s), medium (180s), and slow (600s) tiers. Ingestion picks up within 60 seconds. Edit `subreddits.sql` to track different subreddits.

The file starts with `DELETE FROM subreddit_config` — re-running it resets to the default set. Data persists across `docker compose down` + `docker compose up`. `docker compose down -v` destroys volumes and requires re-running this.

To manage subreddits individually: Django Admin → Subreddit Configs. Changes apply within 60 seconds.

### 5 — Dashboard

```bash
cd dashboard/reddit_dashboard && npm install && npm run dev
```

### 6 — Verify

```bash
curl http://localhost:8080/health/
curl http://localhost:8080/api/posts/?page_size=5
curl "http://localhost:8080/api/stats/?start=2026-02-22&end=2026-02-22"

docker exec -it postgres psql -U reddit -d reddit \
  -c "SELECT COUNT(*), MAX(last_polled_at) FROM posts;"
```

### 7 — Monitoring

Prometheus scrapes all three processing replicas every 15 seconds automatically. Open `http://localhost:9090/targets` to confirm all targets show `UP`.

Grafana at `http://localhost:3000` (admin/admin) has a pre-provisioned dashboard — no setup required. Four panels show Messages/sec, Batch Flush Latency P50/P95/P99, Batch Outcomes, and DLQ rate. Data appears within a minute of ingestion running.

### Services

| Service | URL |
|---|---|
| React dashboard | http://localhost:5173 |
| Django API | http://localhost:8080/api/ |
| Django Admin | http://localhost:8080/admin/ |
| DLQ replay | http://localhost:8001/dlq |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |

### Run Django locally (optional)

```bash
python manage.py runserver
# with WebSocket support:
daphne -b 0.0.0.0 -p 8000 config.routing:application
```

Connects to Postgres on port 5433 and Redis on 6379 as configured in `settings.py`.

### Production checklist

1. Set `IsAuthenticated` in `settings.py` — currently `AllowAny`
2. Rotate `DJANGO_SECRET_KEY`
3. Set `DEBUG=false`, populate `ALLOWED_HOSTS`
4. Uncomment `DATABASE_ROUTERS` once `pg_stat_replication` shows `streaming`
5. Put Nginx or Traefik in front of Daphne for HTTPS/WSS

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Data Flow](#2-data-flow)
3. [Ingestion Service](#3-ingestion-service)
4. [Processing Service](#4-processing-service)
5. [Database Layer](#5-database-layer)
6. [Django API & WebSocket](#6-django-api--websocket)
7. [Observability](#7-observability)
8. [Operational Runbook](#8-operational-runbook)
9. [Configuration Reference](#9-configuration-reference)
10. [Not Yet Active Locally](#10-not-yet-active-locally)

---

## 1. Architecture

### Why it's built this way

| Decision | Reason |
|---|---|
| Kafka over a task queue | Two data streams (raw vs refresh) with different consumers. Horizontal scaling by adding replicas — no code change. |
| TimescaleDB over plain Postgres | Append-only time-series. Hypertable chunks scan only relevant partitions. Retention drops chunk files, not rows. |
| asyncio concurrent polling | Sequential: 20 subreddits × 1s = 20s/cycle. asyncio: all 20 complete in ~1s. |
| Redis velocity cache | Three replicas had separate in-memory dicts — velocity always returned 0.0 on cross-replica refreshes. Shared Redis eliminates split-brain. |
| Micro-batching + manual offsets | 1 write/message = 1000 DB round-trips/min. batch_size=50 → 10/min. `commit()` only after confirmed write — crash recovery re-delivers from last offset, idempotent via upserts. |
| DB-driven subreddit config | Hard-coded list required restarts. `subreddit_config` + Django Admin + config_watcher: zero-downtime changes within 60s. |
| Daphne ASGI | Gunicorn can't handle WebSocket. Daphne serves HTTP and WebSocket on the same port. |
| Read replica routing | Django reads competed with processing write bursts. Router sends ORM reads to replica, writes to primary. |
| DLQ + HTTP replay | Silent failures = data loss. DLQ captures failed batches with full error context, replay API re-processes without code changes. |

### Services

| Container | Role | Port |
|---|---|---|
| `zookeeper` | Kafka coordination | 2181 |
| `kafka`, `kafka2`, `kafka3` | 3-broker cluster (RF=3, minISR=2) | 29092, 9093, 9094 |
| `postgres` | Primary DB (TimescaleDB) | 5433 |
| `postgres-replica` | Read replica (hot standby) | 5434 |
| `redis` | Cache + channels + velocity | 6379 |
| `ingestion` | Reddit API polling | — |
| `processing` ×3 | Stream processing | 8010–8012 |
| `dlq-consumer` | DLQ monitoring + replay | 8001 |
| `reddit_django` | API + WebSocket (Daphne) | 8080 |
| `prometheus` | Metrics scraper | 9090 |
| `grafana` | Dashboards | 3000 |

### Infrastructure

```
Reddit API  (OAuth, 60 req/min per app)
     │
     ▼
Ingestion Service  (asyncio, concurrent per-subreddit tasks)
     │  aiokafka — Snappy compression, 500ms linger
     ▼
Kafka Cluster  (3 brokers, RF=3, minISR=2)
     ├── reddit.posts.raw      3 partitions
     ├── reddit.posts.refresh  3 partitions
     └── reddit.posts.dlq      1 partition
     │
     ▼
Processing Service  (×3 replicas, one partition each)
     │  Micro-batch flush · VADER sentiment · velocity · trending score
     │
     ├──▶  Postgres Primary (TimescaleDB)
     │          │  WAL streaming replication
     │          ▼
     │     Postgres Replica  ◀── ORM reads (ReadReplicaRouter)
     │
     ├──▶  Redis DB0  (velocity cache, shared across replicas)
     └──▶  Redis DB2  (Django Channels pub/sub)
               ▼
          Django / Daphne ASGI
               ├── REST API   /api/posts/  /api/stats/
               ├── WebSocket  /ws/posts/
               └── Admin      /admin/
               │
               ├──▶  Redis DB1  (30s response cache)
               └──▶  React Dashboard

Prometheus  scrapes ×3 replicas every 15s  →  Grafana
```

---

## 2. Data Flow

```
1. Post appears on r/technology  (ID: xyz789)

2. INGESTION
   poll_subreddit() → xyz789 not seen → priority "aggressive"
   producer.send("reddit.posts.raw", post)

3. KAFKA
   Lands on partition hash(xyz789) % 3 · replicated to all 3 brokers

4. PROCESSING — raw
   50 messages or 2s → flush_batches()
     sentiment(title) → 0.34
     keywords → ["breakthrough", "quantum", "computing"]
     bulk_upsert_posts()         one execute_values() call
     bulk_upsert_nlp_features()  one execute_values() call
     consumer.commit()

5. POSTGRES
   Primary WAL → replica applies within milliseconds

6. INGESTION — refresh (5 min later)
   producer.send("reddit.posts.refresh", updated_post)

7. PROCESSING — refresh
   score_velocity   = (1200 - 45) / 300 = 3.85/s
   comment_velocity = (45 - 2)   / 300 = 0.14/s
   Redis HSET vel:xyz789 updated
   trending_score → 0.2
   bulk_upsert_posts() · bulk_insert_metrics_history()
   Redis PUBLISH asgi:group:posts_feed

8. Django Channels delivers to all connected WebSocket clients

9. Dashboard updates score, velocity, trending badge — no HTTP poll
```

---

## 3. Ingestion Service

**Concurrent polling** — asyncio tasks overlap network I/O. 20 subreddits in ~1s vs 20s sequentially.

**Priority tiers:**

```
aggressive    post < 5 min      every 5 min
normal        post < 60 min     every 30 min
slow          post < 24 h       every 2 hours
inactive      post > 24 h       not refreshed
```

**Hot-reload** — `config_watcher` polls `subreddit_config` every 60s. New subreddits spawn tasks, removed ones are cancelled. No restart needed.

**Rate limits** — ~60 req/min per OAuth app. Each subreddit costs ~3.4 req/min. One app supports 16–18 subreddits at aggressive intervals. Add OAuth apps + ingestion containers with distinct `INGESTION_SHARD_ID` for more.

**Burst management under load** — During a 12-hour stress test across 120 subreddits, refresh cycles caused periodic bursts peaking at 25 req/s — the Reddit API rate limit ceiling. Rather than throttling at the ingestion layer, the asyncio task scheduler naturally staggers these bursts: sustained throughput between cycles holds at 2–3 req/s, well inside the 60 req/min cap. Batch flush P99 latency settled from ~130ms at startup to a stable ~70ms after warm-up and held there overnight. The 50 DLQ messages visible in the dashboard are the expected one-time flush from inserting the initial `subreddits.sql` seed — the counter was static throughout the run, confirming zero ongoing data loss.

![10-hour stress test — Messages/sec, Batch Flush Latency P50/P95/P99, Batch Outcomes, DLQ rate](reddit_producer/assets/graphana_stress_graph.png)

*Burst spikes to 25 req/s at refresh cycles, P99 latency stabilising at ~70ms, sustained ok batch rate with negligible errors, DLQ flat at 100 after initial seed flush.*

---

## 4. Processing Service

**Micro-batching** — flushes at 50 messages or 2s. `execute_values()` sends one SQL statement per batch — one round-trip, one WAL write.

**Exactly-once** — `enable_auto_commit=False`. Offsets advance only after confirmed DB write. Crash = re-delivery from last offset. `ON CONFLICT DO UPDATE` makes re-delivery idempotent.

**Sentiment** — VADER runs once per post on ingestion, cached for the 24h lifecycle.

**Velocity:**
```python
score_velocity   = (current_score    - prev_score)    / max(delta_seconds, 1.0)
comment_velocity = (current_comments - prev_comments) / max(delta_seconds, 1.0)
```

**Trending score:**

| Condition | Weight |
|---|---|
| score_velocity > 50/s | +0.4 |
| score_velocity > 10/s | +0.2 |
| abs(sentiment) > 0.5  | +0.2 |
| comments > 100        | +0.2 |

`is_trending = trending_score >= 0.5`

**DLQ** — failed batches published to `reddit.posts.dlq`. Replay API on port 8001:

```
GET  /dlq               view failures
GET  /dlq/stats         counts by topic
POST /dlq/replay        replay all
POST /dlq/replay/{idx}  replay one
```

---

## 5. Database Layer

```
posts                   upserted on every refresh cycle
post_metrics_history    TimescaleDB hypertable — daily chunks, 90d retention
post_nlp_features       sentiment, keywords JSONB, topic_cluster
post_metrics_hourly     continuous aggregate — 1h buckets, refreshed 30min
subreddit_config        Django Admin managed hot-reload config
dlq_events              durable DLQ persistence
```

**Key columns — posts:**

```sql
score_velocity    FLOAT        -- upvotes/sec
comment_velocity  FLOAT        -- comments/sec
trending_score    FLOAT        -- 0.0–1.0
is_trending       BOOLEAN      -- trending_score >= 0.5
first_seen_at     TIMESTAMPTZ  -- set once
last_polled_at    TIMESTAMPTZ  -- updated every cycle
```

**TimescaleDB** — `post_metrics_history` partitioned by day. Retention drops chunk files (no WAL-bloating DELETEs). `post_metrics_hourly` continuous aggregate serves trend charts without scanning the raw hypertable.

**Migrations** — custom sequential runner `storage/migrate.py`, V1–V4, tracked in `schema_migrations`. Django system tables managed separately via `manage.py migrate`.

---

## 6. Django API & WebSocket

```
GET  /api/posts/          paginated feed — annotated, NLP joined
GET  /api/stats/          aggregates — 30s Redis cache
POST /api/token/          obtain JWT
POST /api/token/refresh/  rotate token
GET  /health/             no auth, no DB
WS   /ws/posts/           live update stream
```

**Post response:**

```json
{
  "id": "xyz789",
  "subreddit": "technology",
  "current_score": 1200,
  "score_velocity": 3.85,
  "comment_velocity": 0.14,
  "trending_score": 0.2,
  "is_trending": false,
  "engagement_score": 1290,
  "age_minutes": 312.4,
  "momentum": 4.12,
  "nlp": {
    "sentiment_score": 0.34,
    "keywords": ["breakthrough", "quantum", "computing"],
    "topic_cluster": null
  }
}
```

`engagement_score`, `age_minutes`, `momentum` — SQL annotations, not Python. `nlp` — single `prefetch_related` join. Cursor pagination — no `OFFSET` degradation.

---

## 7. Observability

| Metric | Type |
|---|---|
| `reddit_processor_messages_total{topic}` | Counter |
| `reddit_processor_batches_total{status}` | Counter |
| `reddit_processor_dlq_messages_total` | Counter |
| `reddit_processor_batch_flush_seconds` | Histogram |

**Grafana — healthy state:**

| Panel | Target |
|---|---|
| Messages/sec | 2–5 sustained, spikes to ~25 during refresh bursts |
| Batch Flush P99 | < 250ms |
| Batch Outcomes | No error spikes |
| DLQ rate | 0 — any non-zero sustained value = data loss |

---

## 8. Operational Runbook

**Add/pause a subreddit** — Django Admin → Subreddit Configs. Takes effect within 60 seconds.

**Check replication:**
```bash
docker exec -it postgres psql -U reddit -d reddit \
  -c "SELECT client_addr, state FROM pg_stat_replication;"
```

**DLQ:**
```bash
curl http://localhost:8001/dlq
curl -X POST http://localhost:8001/dlq/replay
```

**Scale processing:**
```bash
docker compose up -d --scale processing=5
# max 3 useful with 3 partitions — increase partitions first
```

**Tune thresholds** — edit `.env`, `docker compose up -d processing`. No rebuild.

---

## 9. Configuration Reference

### Reddit
| Variable | Description |
|---|---|
| `REDDIT_CLIENT_ID` | OAuth client ID |
| `REDDIT_CLIENT_SECRET` | OAuth client secret |
| `REDDIT_USER_AGENT` | App identifier |
| `INGESTION_SHARD_ID` | Shard index (default `0`) |

### Kafka
| Variable | Default |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092,kafka2:9092,kafka3:9092` |
| `BATCH_SIZE` | `50` |
| `BATCH_TIMEOUT` | `2.0` |

### Database
| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `postgres` |
| `POSTGRES_PORT` | `5433` |
| `POSTGRES_REPLICA_HOST` | `postgres-replica` |
| `POSTGRES_REPLICA_PORT` | `5434` |

### Processing
| Variable | Default |
|---|---|
| `TRENDING_VELOCITY_HIGH` | `50` |
| `TRENDING_VELOCITY_LOW` | `10` |
| `TRENDING_SENTIMENT_MIN` | `0.5` |
| `TRENDING_COMMENT_MIN` | `100` |
| `SCHEDULER_CONFIG_POLL_S` | `60` |

### Django
| Variable | Default |
|---|---|
| `DJANGO_SECRET_KEY` | `insecure-dev-key` |
| `DJANGO_DEBUG` | `true` |
| `DJANGO_ALLOWED_HOSTS` | `localhost,127.0.0.1` |

---

## 10. Not Yet Active Locally

The following are implemented in the codebase but not enabled in the local development configuration:

| Feature | Location | To enable |
|---|---|---|
| Read replica routing | `config/db_router.py` | Uncomment `DATABASE_ROUTERS` in `settings.py` once `pg_stat_replication` shows `streaming` |
| JWT authentication | `config/settings.py` | Switch `AllowAny` back to `IsAuthenticated` |
| DLQ persistence to `dlq_events` | `processing/dlq_consumer.py` | Wire in the existing `_persist_to_db()` call |
| `topic_cluster` assignment | `processing/flush_batches.py` | Cluster logic written, not yet called |
| Velocity cache size gauge | `processing/analytics/velocity_cache.py` | Gauge defined, set call missing |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11, asyncpraw, aiokafka |
| Stream processing | Python 3.11, kafka-python, psycopg2, vaderSentiment |
| Message broker | Apache Kafka 7.5.0, Zookeeper |
| Database | PostgreSQL 15 + TimescaleDB |
| Cache / Channels | Redis 7 |
| API | Django 6, DRF, Django Channels, Daphne |
| Auth | djangorestframework-simplejwt |
| Dashboard | React 19, Vite, Recharts, Framer Motion |
| Monitoring | Prometheus 2.47, Grafana 10.1 |
| Containerisation | Docker Compose 3.9 |

---

*SignalFlow — Phase 2. Built by Yogesh S.*
