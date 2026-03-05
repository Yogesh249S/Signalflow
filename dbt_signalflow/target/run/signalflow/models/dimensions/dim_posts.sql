
  
    

  create  table "reddit"."public_analytics"."dim_posts__dbt_tmp"
  
  
    as
  
  (
    -- dim_posts.sql
-- One enriched row per Reddit post.
-- Joins posts with NLP features into a single clean analytical record.
-- Materialised as TABLE — stable reference for all fact model joins.
-- Refreshed on every dbt run (full table rebuild).

with posts as (
    select * from "reddit"."public_staging"."stg_posts"
),

nlp as (
    select * from "reddit"."public_staging"."stg_nlp_features"
),

enriched as (
    select
        -- identity
        p.post_id,
        p.title,
        p.author,
        p.subreddit_id,
        p.created_utc,

        -- current engagement state
        p.score,
        p.num_comments,
        p.upvote_ratio,
        p.score_velocity,
        p.comment_velocity,
        p.combined_velocity,

        -- trending state
        p.trending_score,
        p.is_trending,

        -- lifecycle
        p.poll_priority,
        p.first_seen_at,
        p.last_polled_at,
        p.age_hours,
        p.hours_polled,

        -- NLP signals
        coalesce(n.sentiment_score, 0.0)                as sentiment_score,
        coalesce(n.sentiment_label, 'neutral')          as sentiment_label,
        coalesce(n.keywords_raw, '[]')                  as keywords_raw,
        coalesce(n.keyword_count, 0)                    as keyword_count,
        coalesce(n.topic_cluster, 'unknown')            as topic_cluster,

        -- derived engagement composite
        p.score + (p.num_comments * 2)                  as engagement_score,

        -- momentum: velocity weighted by age
        case
            when p.age_hours > 0
            then p.score_velocity * ln(1 + p.age_hours)
            else 0.0
        end                                             as momentum_score,

        -- post age bucket
        case
            when p.age_hours <  1  then 'under_1h'
            when p.age_hours <  6  then '1h_to_6h'
            when p.age_hours < 12  then '6h_to_12h'
            when p.age_hours < 24  then '12h_to_24h'
            else                        'over_24h'
        end                                             as age_bucket

    from posts p
    left join nlp n on n.post_id = p.post_id
)

select * from enriched
  );
  