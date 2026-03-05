-- tests/assert_velocity_not_negative.sql
-- Checks for extreme negative velocity that would indicate a data bug.
-- Note: small negative velocity is valid on Reddit (posts get downvoted).
-- We only flag values below -100 which would indicate a processing error
-- not normal downvoting behaviour.

select
    post_id,
    score_velocity,
    comment_velocity
from {{ ref('stg_posts') }}
where score_velocity < -100
   or comment_velocity < -100
