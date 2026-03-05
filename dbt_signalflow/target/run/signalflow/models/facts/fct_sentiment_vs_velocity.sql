
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "reddit"."public_analytics"."fct_sentiment_vs_velocity" as DBT_INTERNAL_DEST
        using "fct_sentiment_vs_velocity__dbt_tmp175109768789" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.post_id = DBT_INTERNAL_DEST.post_id))

    
    when matched then update set
        "post_id" = DBT_INTERNAL_SOURCE."post_id","subreddit_id" = DBT_INTERNAL_SOURCE."subreddit_id","first_seen_at" = DBT_INTERNAL_SOURCE."first_seen_at","age_hours" = DBT_INTERNAL_SOURCE."age_hours","is_trending" = DBT_INTERNAL_SOURCE."is_trending","initial_sentiment_score" = DBT_INTERNAL_SOURCE."initial_sentiment_score","initial_sentiment_label" = DBT_INTERNAL_SOURCE."initial_sentiment_label","sentiment_bucket" = DBT_INTERNAL_SOURCE."sentiment_bucket","score_at_1h" = DBT_INTERNAL_SOURCE."score_at_1h","comments_at_1h" = DBT_INTERNAL_SOURCE."comments_at_1h","score_at_3h" = DBT_INTERNAL_SOURCE."score_at_3h","comments_at_3h" = DBT_INTERNAL_SOURCE."comments_at_3h","score_at_6h" = DBT_INTERNAL_SOURCE."score_at_6h","comments_at_6h" = DBT_INTERNAL_SOURCE."comments_at_6h","current_score" = DBT_INTERNAL_SOURCE."current_score","current_velocity" = DBT_INTERNAL_SOURCE."current_velocity","score_growth_1h_to_3h" = DBT_INTERNAL_SOURCE."score_growth_1h_to_3h","score_growth_3h_to_6h" = DBT_INTERNAL_SOURCE."score_growth_3h_to_6h","has_1h_snapshot" = DBT_INTERNAL_SOURCE."has_1h_snapshot","has_3h_snapshot" = DBT_INTERNAL_SOURCE."has_3h_snapshot","has_6h_snapshot" = DBT_INTERNAL_SOURCE."has_6h_snapshot"
    

    when not matched then insert
        ("post_id", "subreddit_id", "first_seen_at", "age_hours", "is_trending", "initial_sentiment_score", "initial_sentiment_label", "sentiment_bucket", "score_at_1h", "comments_at_1h", "score_at_3h", "comments_at_3h", "score_at_6h", "comments_at_6h", "current_score", "current_velocity", "score_growth_1h_to_3h", "score_growth_3h_to_6h", "has_1h_snapshot", "has_3h_snapshot", "has_6h_snapshot")
    values
        ("post_id", "subreddit_id", "first_seen_at", "age_hours", "is_trending", "initial_sentiment_score", "initial_sentiment_label", "sentiment_bucket", "score_at_1h", "comments_at_1h", "score_at_3h", "comments_at_3h", "score_at_6h", "comments_at_6h", "current_score", "current_velocity", "score_growth_1h_to_3h", "score_growth_3h_to_6h", "has_1h_snapshot", "has_3h_snapshot", "has_6h_snapshot")


  