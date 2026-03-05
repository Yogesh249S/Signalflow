
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into "reddit"."public_analytics"."fct_hourly_engagement" as DBT_INTERNAL_DEST
        using "fct_hourly_engagement__dbt_tmp175104663020" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.subreddit_id = DBT_INTERNAL_DEST.subreddit_id
                ) and (
                    DBT_INTERNAL_SOURCE.recorded_hour = DBT_INTERNAL_DEST.recorded_hour
                )

    
    when matched then update set
        "recorded_hour" = DBT_INTERNAL_SOURCE."recorded_hour","recorded_date" = DBT_INTERNAL_SOURCE."recorded_date","subreddit_id" = DBT_INTERNAL_SOURCE."subreddit_id","unique_posts_observed" = DBT_INTERNAL_SOURCE."unique_posts_observed","total_snapshots" = DBT_INTERNAL_SOURCE."total_snapshots","trending_snapshots" = DBT_INTERNAL_SOURCE."trending_snapshots","avg_score" = DBT_INTERNAL_SOURCE."avg_score","max_score" = DBT_INTERNAL_SOURCE."max_score","avg_comments" = DBT_INTERNAL_SOURCE."avg_comments","avg_upvote_ratio" = DBT_INTERNAL_SOURCE."avg_upvote_ratio","avg_score_velocity" = DBT_INTERNAL_SOURCE."avg_score_velocity","peak_score_velocity" = DBT_INTERNAL_SOURCE."peak_score_velocity","avg_comment_velocity" = DBT_INTERNAL_SOURCE."avg_comment_velocity","avg_trending_score" = DBT_INTERNAL_SOURCE."avg_trending_score","avg_sentiment_score" = DBT_INTERNAL_SOURCE."avg_sentiment_score","positive_post_count" = DBT_INTERNAL_SOURCE."positive_post_count","negative_post_count" = DBT_INTERNAL_SOURCE."negative_post_count","neutral_post_count" = DBT_INTERNAL_SOURCE."neutral_post_count","trending_rate_pct" = DBT_INTERNAL_SOURCE."trending_rate_pct","hour_of_day" = DBT_INTERNAL_SOURCE."hour_of_day","day_of_week" = DBT_INTERNAL_SOURCE."day_of_week"
    

    when not matched then insert
        ("recorded_hour", "recorded_date", "subreddit_id", "unique_posts_observed", "total_snapshots", "trending_snapshots", "avg_score", "max_score", "avg_comments", "avg_upvote_ratio", "avg_score_velocity", "peak_score_velocity", "avg_comment_velocity", "avg_trending_score", "avg_sentiment_score", "positive_post_count", "negative_post_count", "neutral_post_count", "trending_rate_pct", "hour_of_day", "day_of_week")
    values
        ("recorded_hour", "recorded_date", "subreddit_id", "unique_posts_observed", "total_snapshots", "trending_snapshots", "avg_score", "max_score", "avg_comments", "avg_upvote_ratio", "avg_score_velocity", "peak_score_velocity", "avg_comment_velocity", "avg_trending_score", "avg_sentiment_score", "positive_post_count", "negative_post_count", "neutral_post_count", "trending_rate_pct", "hour_of_day", "day_of_week")


  