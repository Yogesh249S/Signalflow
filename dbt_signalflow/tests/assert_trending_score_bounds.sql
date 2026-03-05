-- tests/assert_trending_score_bounds.sql
-- Custom singular test: trending_score must always be between 0 and 1.
-- The processing service computes this as a weighted sum of boolean signals
-- so it should mathematically never exceed 1.0 or go below 0.0.
-- Any violation indicates a bug in compute_trending() in the processing service.

select
    post_id,
    trending_score
from {{ ref('stg_posts') }}
where trending_score < 0
   or trending_score > 1

-- dbt test passes when this query returns 0 rows
