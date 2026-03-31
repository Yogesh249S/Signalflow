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
4. DUAL MODEL routing — RoBERTa for comments, VADER for posts
   Comments are short, informal, sarcastic text — VADER misclassifies them
   badly. RoBERTa (cardiffnlp/twitter-roberta-base-sentiment-latest) was
   trained on 124M tweets and handles this profile correctly.
   Posts keep VADER — fast, no memory overhead, good enough for headlines.
   Routing is controlled by the `is_comment` flag in signal["extra"].
"""

import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logger = logging.getLogger(__name__)

# ── VADER — posts ─────────────────────────────────────────────────────────────
# Module-level singleton — initialised once, reused for every call.
# SentimentIntensityAnalyzer is NOT thread-safe for concurrent writes,
# but polarity_scores() is a read-only operation and IS safe to share.
_analyzer = SentimentIntensityAnalyzer()

# ── RoBERTa — comments ────────────────────────────────────────────────────────
# Lazy-loaded on first comment — avoids 2-3s startup delay when the
# processing container starts, and skips the load entirely if no comments
# flow through (e.g. in test environments).
_roberta_pipeline = None
_roberta_available = None   # None = untried, True = loaded, False = failed


def _get_roberta():
    """
    Lazy-load the RoBERTa sentiment pipeline.
    Returns the pipeline on success, None on failure.
    Logs a clear warning if transformers/torch are not installed.
    """
    global _roberta_pipeline, _roberta_available

    if _roberta_available is True:
        return _roberta_pipeline
    if _roberta_available is False:
        return None  # already failed, don't retry

    try:
        from transformers import pipeline as hf_pipeline
        logger.info("Loading RoBERTa sentiment model — first comment received...")
        _roberta_pipeline = hf_pipeline(
            task="sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            top_k=1,
            truncation=True,
            max_length=512,
        )
        _roberta_available = True
        logger.info("RoBERTa sentiment model loaded successfully.")
        return _roberta_pipeline
    except Exception as exc:
        _roberta_available = False
        logger.warning(
            "RoBERTa model unavailable (%s) — falling back to VADER for comments.", exc
        )
        return None


# ── Label normalisation ───────────────────────────────────────────────────────
# RoBERTa returns "positive"/"neutral"/"negative" but with different
# capitalisation depending on the model version. Normalise to lowercase.
_ROBERTA_LABEL_MAP = {
    "positive": "positive",
    "negative": "negative",
    "neutral":  "neutral",
    # some model versions use these
    "label_0":  "negative",
    "label_1":  "neutral",
    "label_2":  "positive",
}

# RoBERTa doesn't return a [-1, 1] compound score — it returns a confidence
# probability [0, 1] for the predicted class. We convert to a signed compound
# so downstream code (divergence detector, trending score) works unchanged.
def _roberta_to_compound(label: str, score: float) -> float:
    """Convert RoBERTa (label, confidence) to a VADER-compatible compound."""
    if label == "positive":
        return score          # 0.0 to +1.0
    elif label == "negative":
        return -score         # 0.0 to -1.0
    else:
        return 0.0            # neutral


def _analyze_roberta(text: str) -> tuple[float, str]:
    """Run RoBERTa sentiment on comment text."""
    pipe = _get_roberta()
    if pipe is None:
        # Graceful fallback to VADER if model failed to load
        return _analyze_vader(text)

    try:
        # pipeline returns [[{"label": "positive", "score": 0.94}]]
        result = pipe(text[:512])[0]
        if isinstance(result, list):
            result = result[0]
        raw_label = result["label"].lower()
        label     = _ROBERTA_LABEL_MAP.get(raw_label, "neutral")
        compound  = _roberta_to_compound(label, result["score"])
        return compound, label
    except Exception as exc:
        logger.debug("RoBERTa inference failed (%s) — falling back to VADER", exc)
        return _analyze_vader(text)


def _analyze_vader(text: str) -> tuple[float, str]:
    """Run VADER sentiment on text."""
    scores   = _analyzer.polarity_scores(text)
    compound = scores["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return compound, label


def analyze_sentiment(text: str, is_comment: bool = False) -> tuple[float, str]:
    """
    Run sentiment analysis on `text`.

    Routes to RoBERTa for comments (better on informal/sarcastic social text)
    and VADER for posts (fast, good enough for headlines and longer text).

    Parameters
    ----------
    text       : the text to analyse
    is_comment : True for Reddit comments, False for posts/headlines

    Returns
    -------
    compound : float in [-1.0, 1.0]
    label    : "positive" | "neutral" | "negative"
    """
    if not text or not text.strip():
        return 0.0, "neutral"

    if is_comment:
        return _analyze_roberta(text)
    return _analyze_vader(text)
