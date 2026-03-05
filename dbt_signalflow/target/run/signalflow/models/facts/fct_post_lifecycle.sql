
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "reddit"."public_analytics"."fct_post_lifecycle" as DBT_INTERNAL_DEST
        using "fct_post_lifecycle__dbt_tmp175107044997" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.post_id = DBT_INTERNAL_DEST.post_id))

    
    when matched then update set
        "post_id" = DBT_INTERNAL_SOURCE."post_id","subreddit_id" = DBT_INTERNAL_SOURCE."subreddit_id","title" = DBT_INTERNAL_SOURCE."title","author" = DBT_INTERNAL_SOURCE."author","created_utc" = DBT_INTERNAL_SOURCE."created_utc","first_seen_at" = DBT_INTERNAL_SOURCE."first_seen_at","last_polled_at" = DBT_INTERNAL_SOURCE."last_polled_at","age_hours" = DBT_INTERNAL_SOURCE."age_hours","poll_priority" = DBT_INTERNAL_SOURCE."poll_priority","current_score" = DBT_INTERNAL_SOURCE."current_score","current_comments" = DBT_INTERNAL_SOURCE."current_comments","is_trending" = DBT_INTERNAL_SOURCE."is_trending","trending_score" = DBT_INTERNAL_SOURCE."trending_score","score_velocity" = DBT_INTERNAL_SOURCE."score_velocity","comment_velocity" = DBT_INTERNAL_SOURCE."comment_velocity","initial_sentiment_score" = DBT_INTERNAL_SOURCE."initial_sentiment_score","initial_sentiment_label" = DBT_INTERNAL_SOURCE."initial_sentiment_label","keyword_count" = DBT_INTERNAL_SOURCE."keyword_count","total_snapshots" = DBT_INTERNAL_SOURCE."total_snapshots","peak_score_ever" = DBT_INTERNAL_SOURCE."peak_score_ever","peak_comments_ever" = DBT_INTERNAL_SOURCE."peak_comments_ever","avg_score_lifetime" = DBT_INTERNAL_SOURCE."avg_score_lifetime","avg_upvote_ratio" = DBT_INTERNAL_SOURCE."avg_upvote_ratio","first_snapshot_at" = DBT_INTERNAL_SOURCE."first_snapshot_at","last_snapshot_at" = DBT_INTERNAL_SOURCE."last_snapshot_at","score_growth" = DBT_INTERNAL_SOURCE."score_growth","sentiment_bucket" = DBT_INTERNAL_SOURCE."sentiment_bucket","engagement_score" = DBT_INTERNAL_SOURCE."engagement_score","momentum_score" = DBT_INTERNAL_SOURCE."momentum_score","age_bucket" = DBT_INTERNAL_SOURCE."age_bucket"
    

    when not matched then insert
        ("post_id", "subreddit_id", "title", "author", "created_utc", "first_seen_at", "last_polled_at", "age_hours", "poll_priority", "current_score", "current_comments", "is_trending", "trending_score", "score_velocity", "comment_velocity", "initial_sentiment_score", "initial_sentiment_label", "keyword_count", "total_snapshots", "peak_score_ever", "peak_comments_ever", "avg_score_lifetime", "avg_upvote_ratio", "first_snapshot_at", "last_snapshot_at", "score_growth", "sentiment_bucket", "engagement_score", "momentum_score", "age_bucket")
    values
        ("post_id", "subreddit_id", "title", "author", "created_utc", "first_seen_at", "last_polled_at", "age_hours", "poll_priority", "current_score", "current_comments", "is_trending", "trending_score", "score_velocity", "comment_velocity", "initial_sentiment_score", "initial_sentiment_label", "keyword_count", "total_snapshots", "peak_score_ever", "peak_comments_ever", "avg_score_lifetime", "avg_upvote_ratio", "first_snapshot_at", "last_snapshot_at", "score_growth", "sentiment_bucket", "engagement_score", "momentum_score", "age_bucket")


  