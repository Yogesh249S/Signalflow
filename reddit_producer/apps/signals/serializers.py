"""
apps/signals/serializers.py
============================
Replaces: apps/reddit/serializers.py entirely.

SignalSerializer     — replaces PostSerializer
CommunitySerializer  — replaces SubredditSerializer
SourceConfigSerializer — replaces SubredditConfigSerializer (admin use)
"""

from rest_framework import serializers
from .models import Signal, Community, PlatformDivergence, SourceConfig


class CommunitySerializer(serializers.ModelSerializer):
    class Meta:
        model  = Community
        fields = ["id", "platform", "name"]


class SignalSerializer(serializers.ModelSerializer):
    community_name     = serializers.SerializerMethodField()
    community_platform = serializers.SerializerMethodField()

    # Computed annotations from get_queryset — must be declared explicitly
    engagement_score = serializers.FloatField(read_only=True, default=0.0)
    age_minutes      = serializers.FloatField(read_only=True, default=0.0)
    momentum         = serializers.FloatField(read_only=True, default=0.0)

    class Meta:
        model  = Signal
        fields = [
            # Identity
            "id", "platform", "source_id",
            "community_name", "community_platform",

            # Content
            "title", "body", "url", "author",

            # Timing
            "published_at", "first_seen_at",

            # Engagement
            "raw_score", "comment_count", "normalised_score",

            # Velocity
            "score_velocity", "comment_velocity",

            # Trending
            "trending_score", "is_trending",

            # NLP
            "sentiment_compound", "sentiment_label",
            "keywords", "topics",

            # Computed annotations
            "engagement_score", "age_minutes", "momentum",
        ]

    def get_community_name(self, obj):
        return obj.community.name if obj.community else None

    def get_community_platform(self, obj):
        return obj.community.platform if obj.community else None


class PlatformDivergenceSerializer(serializers.ModelSerializer):
    class Meta:
        model  = PlatformDivergence
        fields = [
            "id", "topic", "detected_at",
            "platform_a", "platform_b",
            "sentiment_a", "sentiment_b",
            "divergence_score",
            "origin_platform", "origin_lag_minutes",
            "is_resolved",
        ]


class SourceConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model  = SourceConfig
        fields = "__all__"
