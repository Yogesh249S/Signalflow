-- stg_posts.sql
-- Thin wrapper over the raw posts table.
-- Responsibilities:
--   - Filter to active posts only (inactive = no longer polled)
--   - Cast and rename columns to consistent snake_case analytic naming
--   - Coalesce nulls on velocity columns (new posts have no velocity yet)
--   - Compute derived column: age_hours since first_seen_at
-- Materialised as VIEW so it always reflects the latest upserted state.

with source as (
    select * from {{ source('reddit_raw', 'posts') }}
),

cleaned as (
    select
        -- identity
        id                                              as post_id,
        title,
        author,
        subreddit_id,
        created_utc,

        -- engagement metrics (latest values — upserted on every refresh)
        current_score                                   as score,
        current_comments                                as num_comments,
        coalesce(current_ratio, 0.0)                    as upvote_ratio,

        -- velocity — upvotes/comments per second since last refresh
        coalesce(score_velocity,   0.0)                 as score_velocity,
        coalesce(comment_velocity, 0.0)                 as comment_velocity,
        coalesce(velocity, 0.0)                         as combined_velocity,

        -- trending signals
        coalesce(trending_score, 0.0)                   as trending_score,
        coalesce(is_trending, false)                    as is_trending,

        -- lifecycle
        poll_priority,
        first_seen_at,
        last_polled_at,

        -- derived: how old is this post in hours since it was created on Reddit
        extract(epoch from (now() - created_utc)) / 3600.0      as age_hours,

        -- derived: how long have we been tracking it
        extract(epoch from (
            coalesce(last_polled_at, now()) - first_seen_at
        )) / 3600.0                                     as hours_polled

    from source
    where is_active = true
)

select * from cleaned
