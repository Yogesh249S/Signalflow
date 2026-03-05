-- fct_hourly_engagement.sql
-- Hourly rollup of engagement metrics per subreddit.
-- This is the documented, tested, version-controlled equivalent of the
-- TimescaleDB continuous aggregate (post_metrics_hourly).
-- The difference: this model adds sentiment context and trending counts
-- which the raw continuous aggregate doesn't include.
-- Incremental on recorded_hour — each run only processes new hours.



with history as (
    select * from "reddit"."public_staging"."stg_metrics_history"

    
    where recorded_hour >= date_trunc('hour', now() - interval '3 hours')
    
),

posts as (
    select post_id, subreddit_id, score_velocity, comment_velocity,
           is_trending, trending_score
    from "reddit"."public_staging"."stg_posts"
),

nlp as (
    select post_id, sentiment_score, sentiment_label
    from "reddit"."public_staging"."stg_nlp_features"
),

-- join history snapshots with current post state and NLP
joined as (
    select
        h.recorded_hour,
        h.recorded_date,
        p.subreddit_id,
        h.post_id,
        h.score,
        h.num_comments,
        h.upvote_ratio,
        -- velocity comes from posts table (current state)
        -- history table doesn't store velocity per snapshot
        p.score_velocity,
        p.comment_velocity,
        p.is_trending,
        p.trending_score,
        n.sentiment_score,
        n.sentiment_label

    from history h
    join  posts p on p.post_id = h.post_id
    left join nlp n on n.post_id = h.post_id
),

aggregated as (
    select
        recorded_hour,
        recorded_date,
        subreddit_id,

        -- volume
        count(distinct post_id)                         as unique_posts_observed,
        count(*)                                        as total_snapshots,
        count(*) filter (where is_trending)             as trending_snapshots,

        -- engagement aggregates
        avg(score)                                      as avg_score,
        max(score)                                      as max_score,
        avg(num_comments)                               as avg_comments,
        avg(upvote_ratio)                               as avg_upvote_ratio,

        -- velocity from current post state
        avg(score_velocity)                             as avg_score_velocity,
        max(score_velocity)                             as peak_score_velocity,
        avg(comment_velocity)                           as avg_comment_velocity,
        avg(trending_score)                             as avg_trending_score,

        -- sentiment aggregates
        avg(sentiment_score)                            as avg_sentiment_score,
        count(*) filter (
            where sentiment_label = 'positive'
        )                                               as positive_post_count,
        count(*) filter (
            where sentiment_label = 'negative'
        )                                               as negative_post_count,
        count(*) filter (
            where sentiment_label = 'neutral'
        )                                               as neutral_post_count,

        -- trending rate
        round(
            count(*) filter (where is_trending)::numeric
            / nullif(count(*), 0) * 100,
            2
        )                                               as trending_rate_pct,

        -- time dimensions for pattern analysis
        extract(hour from recorded_hour)                as hour_of_day,
        extract(dow from recorded_hour)                 as day_of_week

    from joined
    group by recorded_hour, recorded_date, subreddit_id
)

select * from aggregated