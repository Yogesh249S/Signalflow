-- stg_nlp_features.sql
-- Thin wrapper over post_nlp_features.
-- Responsibilities:
--   - Coalesce nulls (topic_cluster is never populated per known gap in README)
--   - Cast sentiment_score to float
--   - Convert keywords JSONB to a cleaner text representation
-- Materialised as VIEW.

with source as (
    select * from {{ source('reddit_raw', 'post_nlp_features') }}
),

cleaned as (
    select
        post_id,

        -- VADER compound score: -1.0 (most negative) to +1.0 (most positive)
        coalesce(sentiment_score, 0.0)::float           as sentiment_score,

        -- derive sentiment label from score since schema has no label column
        case
            when coalesce(sentiment_score, 0) >=  0.05 then 'positive'
            when coalesce(sentiment_score, 0) <= -0.05 then 'negative'
            else 'neutral'
        end                                             as sentiment_label,

        -- keywords stored as JSONB array
        coalesce(keywords::text, '[]')                  as keywords_raw,
        coalesce(jsonb_array_length(keywords), 0)       as keyword_count,

        -- topic_cluster is int in real schema (null = not yet assigned)
        topic_cluster::text                             as topic_cluster

    from source
    where post_id is not null
)

select * from cleaned
