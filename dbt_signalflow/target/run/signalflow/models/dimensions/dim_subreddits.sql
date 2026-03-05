
  
    

  create  table "reddit"."public_analytics"."dim_subreddits__dbt_tmp"
  
  
    as
  
  (
    -- dim_subreddits.sql
-- One row per subreddit with aggregated performance stats.
-- Answers: which subreddits are most active, most positive, most likely to trend?
-- Materialised as TABLE — rebuilt on every dbt run.

with posts as (
    select * from "reddit"."public_staging"."stg_posts"
),

nlp as (
    select * from "reddit"."public_staging"."stg_nlp_features"
),

-- subreddits table has id (int) and name (text) — the correct name lookup
subreddits as (
    select id, name from "reddit"."public"."subreddits"
),

config as (
    select * from "reddit"."public"."subreddit_config"
),

post_stats as (
    select
        p.subreddit_id,

        -- volume
        count(*)                                        as total_posts,
        count(*) filter (where p.is_trending)           as trending_posts,

        -- engagement
        avg(p.score)                                    as avg_score,
        max(p.score)                                    as peak_score,
        avg(p.num_comments)                             as avg_comments,
        avg(p.score_velocity)                           as avg_score_velocity,
        max(p.score_velocity)                           as peak_score_velocity,

        -- trending rate
        round(
            count(*) filter (where p.is_trending)::numeric
            / nullif(count(*), 0) * 100,
            2
        )                                               as trending_rate_pct,

        -- recency
        max(p.last_polled_at)                           as last_activity_at,
        min(p.first_seen_at)                            as first_post_seen_at

    from posts p
    group by p.subreddit_id
),

sentiment_stats as (
    select
        p.subreddit_id,
        avg(n.sentiment_score)                          as avg_sentiment_score,
        mode() within group (order by n.sentiment_label) as dominant_sentiment,
        count(*) filter (where n.sentiment_label = 'positive') as positive_posts,
        count(*) filter (where n.sentiment_label = 'negative') as negative_posts,
        count(*) filter (where n.sentiment_label = 'neutral')  as neutral_posts

    from posts p
    left join nlp n on n.post_id = p.post_id
    where n.post_id is not null
    group by p.subreddit_id
),

final as (
    select
        ps.subreddit_id,

        -- name from subreddits table (correct lookup via int FK)
        s.name                                          as subreddit_name,

        -- config metadata
        c.is_active                                     as is_actively_polled,
        c.interval_seconds                              as poll_interval_seconds,

        -- volume stats
        ps.total_posts,
        ps.trending_posts,
        ps.trending_rate_pct,

        -- engagement stats
        ps.avg_score,
        ps.peak_score,
        ps.avg_comments,
        ps.avg_score_velocity,
        ps.peak_score_velocity,

        -- sentiment stats
        ss.avg_sentiment_score,
        ss.dominant_sentiment,
        ss.positive_posts,
        ss.negative_posts,
        ss.neutral_posts,

        -- recency
        ps.last_activity_at,
        ps.first_post_seen_at,

        -- signal tier
        case
            when ps.trending_rate_pct >= 20 and ps.avg_score_velocity >= 10
                then 'high_signal'
            when ps.trending_rate_pct >= 10 or  ps.avg_score_velocity >= 5
                then 'medium_signal'
            else
                'low_signal'
        end                                             as signal_tier

    from post_stats ps
    left join subreddits s           on s.id   = ps.subreddit_id
    left join sentiment_stats ss     on ss.subreddit_id = ps.subreddit_id
    left join config c               on c.name = s.name
)

select * from final
  );
  