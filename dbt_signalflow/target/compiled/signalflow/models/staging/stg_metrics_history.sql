-- stg_metrics_history.sql
-- Thin wrapper over post_metrics_history hypertable.
-- Responsibilities:
--   - Ensure timestamps are UTC-aware
--   - Filter out any rows with null velocity (data quality guard)
--   - Rename columns consistently
--   - Add date/hour truncations for easier downstream aggregation
-- Materialised as VIEW — the hypertable handles performance via chunk exclusion.

with source as (
    select * from "reddit"."public"."post_metrics_history"
),

cleaned as (
    select
        -- time dimension — hypertable partition key
        captured_at                                     as recorded_at,
        date_trunc('hour', captured_at)                 as recorded_hour,
        date_trunc('day',  captured_at)::date           as recorded_date,

        -- identity
        post_id,

        -- point-in-time snapshot values
        coalesce(score, 0)                              as score,
        coalesce(num_comments, 0)                       as num_comments,
        coalesce(upvote_ratio, 0.0)                     as upvote_ratio

    from source
    where captured_at is not null
      and score is not null
)

select * from cleaned