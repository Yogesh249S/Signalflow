-- fct_post_lifecycle.sql
-- One row per post. Answers the full lifecycle question:
-- How long did it take to trend? What was peak engagement?
-- Did initial sentiment predict whether it would trend?
-- Incremental — only processes posts updated since last dbt run.

{{
    config(
        materialized='incremental',
        unique_key='post_id',
        incremental_strategy='merge'
    )
}}

with history as (
    select * from {{ ref('stg_metrics_history') }}

    {% if is_incremental() %}
    where recorded_at >= now() - interval '2 hours'
    {% endif %}
),

posts as (
    select * from {{ ref('dim_posts') }}
),

-- peak engagement from history snapshots
-- note: history doesn't store velocity/trending — those come from posts table
peak_stats as (
    select
        post_id,
        count(*)                    as total_snapshots,
        max(score)                  as peak_score_ever,
        max(num_comments)           as peak_comments_ever,
        avg(score)                  as avg_score_lifetime,
        avg(upvote_ratio)           as avg_upvote_ratio,
        min(recorded_at)            as first_snapshot_at,
        max(recorded_at)            as last_snapshot_at
    from history
    group by post_id
),

final as (
    select
        p.post_id,
        p.subreddit_id,
        p.title,
        p.author,
        p.created_utc,
        p.first_seen_at,
        p.last_polled_at,
        p.age_hours,
        p.poll_priority,

        -- current state from posts table
        p.score                                         as current_score,
        p.num_comments                                  as current_comments,
        p.is_trending,
        p.trending_score,
        p.score_velocity,
        p.comment_velocity,

        -- NLP at ingestion time
        p.sentiment_score                               as initial_sentiment_score,
        p.sentiment_label                               as initial_sentiment_label,
        p.keyword_count,

        -- lifecycle stats from history
        h.total_snapshots,
        h.peak_score_ever,
        h.peak_comments_ever,
        h.avg_score_lifetime,
        h.avg_upvote_ratio,
        h.first_snapshot_at,
        h.last_snapshot_at,

        -- score growth: how much did the score grow from first to peak
        case
            when h.total_snapshots > 1
            then h.peak_score_ever - p.score
            else 0
        end                                             as score_growth,

        -- sentiment bucket for grouping
        case
            when p.sentiment_score >=  0.5  then 'strongly_positive'
            when p.sentiment_score >=  0.05 then 'mildly_positive'
            when p.sentiment_score <= -0.5  then 'strongly_negative'
            when p.sentiment_score <= -0.05 then 'mildly_negative'
            else                                 'neutral'
        end                                             as sentiment_bucket,

        -- engagement composite
        p.engagement_score,
        p.momentum_score,
        p.age_bucket

    from posts p
    left join peak_stats h on h.post_id = p.post_id
)

select * from final
