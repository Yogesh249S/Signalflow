from rest_framework import serializers
from .models import Subreddit, Post, Comment, KeywordTrend, SubredditStats
from django.utils.timezone import now, make_aware, is_naive

class SubredditSerializer(serializers.ModelSerializer):
    class Meta:
        model = Subreddit
        fields = "__all__"

class PostSerializer(serializers.ModelSerializer):
    subreddit = serializers.StringRelatedField()
    # engagement_score = serializers.SerializerMethodField()
    # age_minutes = serializers.SerializerMethodField()
    # momentum = serializers.SerializerMethodField()

    engagement_score = serializers.FloatField(read_only=True)
    age_minutes = serializers.FloatField(read_only=True)
    momentum = serializers.FloatField(read_only=True)


    class Meta:
        model = Post
        fields = "__all__"

    def get_engagement_score(self, obj):
        return obj.current_score + (obj.current_comments * 2)

    def get_age_minutes(self, obj):
        created = obj.created_utc
        if is_naive(created):
            created = make_aware(created)

        delta = now() - created
        return int(delta.total_seconds() / 60)


    def get_momentum(self, obj):
        created = obj.created_utc
        if is_naive(created):
            created = make_aware(created)

        delta = now() - created
        minutes = max(delta.total_seconds() / 60, 1)
        return round(obj.current_score / minutes, 2)



class CommentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Comment
        fields = "__all__"


class KeywordTrendSerializer(serializers.ModelSerializer):
    class Meta:
        model = KeywordTrend
        fields = "__all__"


class SubredditStatsSerializer(serializers.ModelSerializer):
    class Meta:
        model = SubredditStats
        fields = "__all__"



