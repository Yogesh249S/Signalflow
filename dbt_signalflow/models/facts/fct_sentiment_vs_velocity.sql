-- fct_sentiment_vs_velocity.sql
-- Core analytical question: does a post's initial sentiment predict velocity?
-- For each post, captures sentiment at ingestion and velocity at
-- 1h, 3h, 6h intervals after first_seen_at.
-- This requires joining history snapshots at specific time offsets
-- which is not possible with the raw TimescaleDB continuous aggregate.
-- This is the model that justifies DBT in the stack.

{{
    config(
        materialized='incremental',
        unique_key='post_id',
        incremental_strategy='merge'
    )
}}

with posts as (
    select * from {{ ref('dim_posts') }}

    {% if is_incremental() %}
    where last_polled_at >= now() - interval '2 hours'
    {% endif %}
),

history as (
    select * from {{ ref('stg_metrics_history') }}
),

-- for each post, get score at ~1h after first_seen_at
score_at_1h as (
    select distinct on (h.post_id)
        h.post_id,
        h.score                                         as score_at_1h,
        h.num_comments                                  as comments_at_1h,
        h.recorded_at                                   as snapshot_1h_at
    from history h
    join posts p on p.post_id = h.post_id
    where h.recorded_at between p.first_seen_at + interval '45 minutes'
                            and p.first_seen_at + interval '90 minutes'
    order by h.post_id, abs(extract(epoch from (
        h.recorded_at - (p.first_seen_at + interval '1 hour')
    )))
),

score_at_3h as (
    select distinct on (h.post_id)
        h.post_id,
        h.score                                         as score_at_3h,
        h.num_comments                                  as comments_at_3h,
        h.recorded_at                                   as snapshot_3h_at
    from history h
    join posts p on p.post_id = h.post_id
    where h.recorded_at between p.first_seen_at + interval '2.5 hours'
                            and p.first_seen_at + interval '3.5 hours'
    order by h.post_id, abs(extract(epoch from (
        h.recorded_at - (p.first_seen_at + interval '3 hours')
    )))
),

score_at_6h as (
    select distinct on (h.post_id)
        h.post_id,
        h.score                                         as score_at_6h,
        h.num_comments                                  as comments_at_6h,
        h.recorded_at                                   as snapshot_6h_at
    from history h
    join posts p on p.post_id = h.post_id
    where h.recorded_at between p.first_seen_at + interval '5.5 hours'
                            and p.first_seen_at + interval '6.5 hours'
    order by h.post_id, abs(extract(epoch from (
        h.recorded_at - (p.first_seen_at + interval '6 hours')
    )))
),

final as (
    select
        p.post_id,
        p.subreddit_id,
        p.first_seen_at,
        p.age_hours,
        p.is_trending,

        -- initial sentiment (the predictor variable)
        p.initial_sentiment_score,
        p.initial_sentiment_label,
        p.sentiment_bucket,

        -- score at time intervals (outcome variables)
        v1.score_at_1h,
        v1.comments_at_1h,

        v3.score_at_3h,
        v3.comments_at_3h,

        v6.score_at_6h,
        v6.comments_at_6h,

        -- current state
        p.current_score,
        p.score_velocity                                as current_velocity,

        -- derived: score growth between intervals
        -- positive = accelerating, null = snapshot not available yet
        case
            when v1.score_at_1h is not null and v3.score_at_3h is not null
            then v3.score_at_3h - v1.score_at_1h
            else null
        end                                             as score_growth_1h_to_3h,

        case
            when v3.score_at_3h is not null and v6.score_at_6h is not null
            then v6.score_at_6h - v3.score_at_3h
            else null
        end                                             as score_growth_3h_to_6h,

        -- data completeness flags
        (v1.post_id is not null)                        as has_1h_snapshot,
        (v3.post_id is not null)                        as has_3h_snapshot,
        (v6.post_id is not null)                        as has_6h_snapshot

    from {{ ref('fct_post_lifecycle') }} p
    left join score_at_1h v1 on v1.post_id = p.post_id
    left join score_at_3h v3 on v3.post_id = p.post_id
    left join score_at_6h v6 on v6.post_id = p.post_id
    where p.age_hours >= 1
)

select * from final
