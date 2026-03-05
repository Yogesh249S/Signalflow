# SignalFlow DBT Project

Transformation layer on top of the SignalFlow real-time Reddit pipeline.

## Why DBT exists in this stack

The raw tables written by the ingestion and processing pipeline are optimised
for writes — upserts, bulk inserts, append-only time-series. They are not
organised for analytical queries. DBT provides:

- **Separation of concerns**: raw ingestion tables vs clean analytical models
- **Documented lineage**: every model knows where it came from
- **Automated tests**: data quality checks run on every `dbt test`
- **Incremental processing**: fact models only reprocess new data

## Architecture

```
Raw Tables (Postgres/TimescaleDB)
    posts
    post_metrics_history      ← TimescaleDB hypertable
    post_nlp_features
    subreddit_config
         │
         ▼
Staging Layer (views)
    stg_posts                 ← active posts, cast + derived columns
    stg_metrics_history       ← cleaned snapshots, time truncations
    stg_nlp_features          ← sentiment + keywords cleaned
         │
         ▼
Dimension Layer (tables)
    dim_posts                 ← enriched post record (posts + NLP joined)
    dim_subreddits            ← per-subreddit aggregated stats + signal tier
         │
         ▼
Fact Layer (incremental tables)
    fct_post_lifecycle        ← full lifecycle per post: time-to-trend, peak engagement
    fct_hourly_engagement     ← hourly rollup per subreddit with sentiment context
    fct_sentiment_vs_velocity ← does initial sentiment predict velocity at 1h/3h/6h?
```

## Setup

### 1. Install DBT
```bash
pip install dbt-postgres
```

### 2. Configure connection
Copy `profiles.yml` to `~/.dbt/profiles.yml` or set `DBT_PROFILES_DIR`:
```bash
export DBT_PROFILES_DIR=.
```

### 3. Verify connection
```bash
dbt debug
```

### 4. Run all models
```bash
dbt run
```

### 5. Run tests
```bash
dbt test
```

### 6. Run specific layer
```bash
dbt run --select staging
dbt run --select dimensions
dbt run --select facts
```

### 7. Generate and serve docs
```bash
dbt docs generate
dbt docs serve
# Opens lineage graph at http://localhost:8080
```

## Key Analytical Questions These Models Answer

**fct_post_lifecycle**
- How long does it take for a post to trend after ingestion?
- What fraction of posts ever reach trending status?
- Does a post's age at trending correlate with peak score?

**fct_hourly_engagement**
- Which hours of the day see peak velocity per subreddit?
- Does sentiment shift at different times of day?
- Which subreddits have the most consistent engagement?

**fct_sentiment_vs_velocity**
- Does a post's initial VADER sentiment predict its velocity at 1h/3h/6h?
- Do strongly positive posts trend faster than neutral ones?
- Is negative sentiment a signal of viral controversy (high velocity)?

**dim_subreddits**
- Which subreddits have the highest trending rate?
- Which subreddits produce the highest peak velocities?
- What is the dominant sentiment per subreddit?

## Running on a Schedule

Add to the Airflow DAG or run as a cron job after the nightly archival:

```bash
# After archival completes, refresh analytical models
dbt run --select facts
dbt test
```

Or add a PythonOperator to the existing Airflow DAG that runs:
```python
subprocess.run(["dbt", "run", "--profiles-dir", ".", "--project-dir", "."], check=True)
```
