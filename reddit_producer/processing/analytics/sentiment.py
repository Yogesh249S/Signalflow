"""
processing/analytics/sentiment.py
===================================
OPTIMISATION CHANGES vs original
----------------------------------
1. SHARED ANALYSER INSTANCE (unchanged — original already did this)
   `SentimentIntensityAnalyzer()` is expensive to initialise (~50 ms, loads
   the VADER lexicon). The original correctly created it once at module level.
   This is kept as-is.

2. SENTIMENT CACHING moved upstream
   The original called `analyze_sentiment()` on every Kafka message, including
   refresh events for posts whose title never changes.
   The caching fix lives in main_processor.py (`_sentiment_cache` dict) — this
   function is now only called when a cache miss occurs, i.e. once per post
   per its 24-h lifetime rather than once per refresh cycle.

3. TYPE HINTS added for clarity
"""

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Module-level singleton — initialised once, reused for every call.
# SentimentIntensityAnalyzer is NOT thread-safe for concurrent writes,
# but polarity_scores() is a read-only operation and IS safe to share.
_analyzer = SentimentIntensityAnalyzer()


def analyze_sentiment(text: str) -> tuple[float, str]:
    """
    Run VADER sentiment analysis on `text`.

    Returns
    -------
    compound : float in [-1.0, 1.0]
    label    : "positive" | "neutral" | "negative"
    """
    scores   = _analyzer.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    return compound, label
