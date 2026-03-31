"""
processing/analytics/topic_extractor.py
========================================
Extracts topic entities from signal text using spaCy NER.

Two layers:
  1. EntityRuler — small seed list of tech terms spaCy's training data
     doesn't know well (dbt, TimescaleDB, PySpark etc.). Runs BEFORE
     the neural NER so it takes priority on known terms.

  2. spaCy NER — catches everything else: companies, people, products,
     places, technologies that appear in its training data (Google,
     Python, AWS, Elon Musk, etc.).

Why not a static dictionary:
  - spaCy catches new entities automatically (new companies, people,
    products) without any code changes.
  - The seed list only needs entries for niche tech terms spaCy misses.
  - TF-IDF discovery (Phase 4) will surface new seeds from real data.

Adding a new niche term: add one pattern to TECH_SEED_PATTERNS.
That's the only manual step ever needed.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── spaCy entity labels we care about ────────────────────────────────────────
# ORG     = organisations, companies (Google, Anthropic, Apache)
# PERSON  = people (Sam Altman, Linus Torvalds)
# PRODUCT = named products (ChatGPT, Docker, Kubernetes)
# GPE     = geopolitical entities — cities, countries (useful for geo signals)
# NORP    = nationalities, political groups (useful for worldnews signals)
RELEVANT_LABELS = {"ORG", "PERSON", "PRODUCT", "GPE", "NORP", "WORK_OF_ART", "EVENT", "LAW"}

# ── Tech seed patterns ────────────────────────────────────────────────────────
# Only terms spaCy's en_core_web_sm/md doesn't reliably detect.
# Keep this small — spaCy handles the rest.
# Format: {"label": ENTITY_TYPE, "pattern": text_or_pattern}
TECH_SEED_PATTERNS = [
    # High-frequency political figures spaCy en_core_web_md misses
    {"label": "PERSON", "pattern": [{"LOWER": "trump"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "biden"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "obama"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "putin"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "zelensky"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "netanyahu"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "modi"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "xi"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "musk"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "altman"}]},
    {"label": "PERSON", "pattern": [{"LOWER": "zuckerberg"}]},
    # High-frequency orgs spaCy misses in short titles
    {"label": "ORG", "pattern": [{"LOWER": "cia"}]},
    {"label": "ORG", "pattern": [{"LOWER": "fbi"}]},
    {"label": "ORG", "pattern": [{"LOWER": "nato"}]},
    {"label": "ORG", "pattern": [{"LOWER": "un"}]},
    {"label": "ORG", "pattern": [{"LOWER": "imf"}]},
    {"label": "ORG", "pattern": [{"LOWER": "fed"}]},
    {"label": "ORG", "pattern": [{"LOWER": "sec"}]},
    {"label": "ORG", "pattern": [{"LOWER": "idf"}]},
    {"label": "ORG", "pattern": [{"LOWER": "hamas"}]},
    {"label": "ORG", "pattern": [{"LOWER": "hezbollah"}]},
    # Data engineering tools
    {"label": "PRODUCT", "pattern": [{"LOWER": "pyspark"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "dbt"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "airflow"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "kafka"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "timescaledb"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "duckdb"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "databricks"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "snowflake"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "flink"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "trino"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "superset"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "dagster"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "prefect"}]},

    # AI / ML tools
    {"label": "PRODUCT", "pattern": [{"LOWER": "pytorch"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "tensorflow"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "huggingface"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "langchain"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "llamaindex"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "ollama"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "mistral"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "llama"}]},

    # AI companies spaCy might miss or confuse
    {"label": "ORG", "pattern": [{"LOWER": "anthropic"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "claude"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "chatgpt"}]},
    {"label": "ORG", "pattern": [{"LOWER": "openai"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "gemini"}]},
    {"label": "ORG", "pattern": [{"LOWER": "mistral"}, {"LOWER": "ai"}]},
    {"label": "ORG", "pattern": [{"LOWER": "cohere"}]},
    {"label": "ORG", "pattern": [{"LOWER": "perplexity"}]},

    # Cloud / infra
    {"label": "PRODUCT", "pattern": [{"LOWER": "kubernetes"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "k8s"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "terraform"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "pulumi"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "grafana"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "prometheus"}]},
    {"label": "ORG",     "pattern": [{"LOWER": "vercel"}]},
    {"label": "ORG",     "pattern": [{"LOWER": "supabase"}]},
    {"label": "ORG",     "pattern": [{"LOWER": "neon"}]},    # neon db

    # Languages that spaCy treats as common words
    {"label": "PRODUCT", "pattern": [{"LOWER": "rust"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "golang"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "zig"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "elixir"}]},

    # Dev tools
    {"label": "PRODUCT", "pattern": [{"LOWER": "neovim"}]},
    {"label": "PRODUCT", "pattern": [{"LOWER": "cursor"}]},  # cursor IDE
    {"label": "PRODUCT", "pattern": [{"LOWER": "warp"}]},    # warp terminal
]

# ── Noise filter ──────────────────────────────────────────────────────────────
# Entities that get extracted frequently but carry no signal value.
# Checked after extraction — O(1) set lookup.
NOISE_ENTITIES = {
    # Too generic
    "new", "the", "one", "first", "last", "year", "day", "week",
    "today", "yesterday", "monday", "tuesday", "wednesday",
    "thursday", "friday", "saturday", "sunday",
    # Common words misclassified as entities
    "use", "way", "time", "good", "best", "help", "work",
    "make", "take", "give", "come", "know", "think",
    # Places too broad to be useful signals
    "us", "uk", "eu", "usa", "world", "internet",
    # Abbreviations and stopwords that spaCy extracts as named entities
    "un", "gop", "nato", "cdt", "pst", "est", "gmt", "utc",
    "bc", "ad", "pm", "am", "st", "nd", "rd", "th",
    "co", "inc", "ltd", "llc", "gov", "org", "corp",
    "rt", "via", "re", "cc", "mon", "tue", "wed", "thu", "fri", "sat", "sun",
    "afd", "aita", "wibta", "nta", "yta", "esh", "nah", "tldr", "imo", "imho", "fyi", "tbh", "jaan",
    # YouTube channel names that leak through as entities
    "dw news", "bbc news", "bbc newscast", "al jazeera", "sky news",
    "abc news", "cnn", "msnbc", "fox news", "nbc news", "cbs news",
    "pbs news", "npr", "c-span", "euronews",
    # Weather bot boilerplate
    "iembot", "nws", "noaa",
    # Own platform noise
    "bluesky", "reddit", "hackernews", "hacker news",
    # Spam and product noise
    "manscape", "manspot", "4 guard",
    # Sensitive content
    "nazi", "nazis",
    # Spam and noise
    "handheld electric", "legacy indie radio", "handheld",
    # Common names too generic to be useful signals
    "mary", "john", "james", "michael", "david", "robert", "william",
    # Generic media terms
    "news", "breaking", "watch", "live", "update", "report",
    # Generic words too broad to be signals
    "state", "house", "donald", "vic", "idk", "bsky",
    "signal", "signals", "youtube", "twitter", "facebook",
    # Demonyms that should be blocked
    "asian", "jewish", "korean", "greek", "canadian", "canadians",
    "european", "african", "arab", "arabic",
    # Weather/news bot garbage patterns handled by noise patterns below
    "iembot additional details here",
}

# ── Canonical topic map ───────────────────────────────────────────────────────
# Merges morphological variants and aliases into a single canonical topic.
# Applied AFTER extraction so signal counts are aggregated correctly.
# Keys are lowercased extracted strings → value is the canonical form.
# Rules of thumb:
#   - Demonyms → country name  (iranian → iran, american → america)
#   - Plural/adjective forms → base noun  (israelis → israel)
#   - Named-entity aliases → primary name  (donald trump → trump)
#   - Media brand variants → canonical brand  (bbc news → bbc)
CANONICAL_MAP: dict[str, str] = {
    # Iran cluster
    "iranian":          "iran",
    "iranians":         "iran",

    # America cluster
    "american":         "america",
    "americans":        "america",
    "canadian":         "canada",
    "canadians":        "canada",
    "korean":           "korea",
    "greek":            "greece",
    "german":           "germany",
    "germans":          "germany",
    "brazilian":        "brazil",
    "polish":           "poland",
    # India cluster
    "indian":           "india",
    "indians":          "india",
    # Japan cluster
    "japanese":         "japan",
    "japaneses":        "japan",
    # Japan cluster
    "japanese":         "japan",
    "japaneses":        "japan",
    # India cluster
    "indian":           "india",
    "indians":          "india",
    "u.s.":             "america",
    "united states":    "america",
    "the united states":"america",

    # Israel cluster
    "israeli":          "israel",
    "israelis":         "israel",

    # Russia cluster
    "russian":          "russia",
    "russians":         "russia",

    # China cluster
    "chinese":          "china",

    # Ukraine cluster
    "ukrainian":        "ukraine",
    "ukrainians":       "ukraine",

    # UK cluster
    "british":          "uk",
    "britain":          "uk",
    "england":          "uk",
    "english":          "uk",

    # People aliases
    "donald trump":     "trump",
    "biden":            "joe biden",

    # Media aliases
    "bbc news":         "bbc",
    "bbc newscast":     "bbc",
    "al jazeera":       "aljazeera",

    # Common political terms
    "democrat":         "democrats",
    "republican":       "republicans",
    "gop":              "republicans",
}


def _canonicalise(topic: str) -> str:
    """Return the canonical form of a topic, or the topic itself if not mapped."""
    return CANONICAL_MAP.get(topic, topic)

# ── spaCy model loader ────────────────────────────────────────────────────────
import spacy
from spacy.language import Language

_nlp: Optional[Language] = None

def _load_model() -> Language:
    """Load spaCy model once and cache it. Adds EntityRuler on first load."""
    global _nlp
    if _nlp is not None:
        return _nlp
    try:
        nlp = spacy.load("en_core_web_md")
    except OSError:
        nlp = spacy.load("en_core_web_sm")
    # Add EntityRuler before NER so seed patterns take priority
    ruler = nlp.add_pipe("entity_ruler", before="ner", config={"overwrite_ents": True})
    ruler.add_patterns(TECH_SEED_PATTERNS)
    _nlp = nlp
    logger.info("spaCy model loaded with %d seed patterns.", len(TECH_SEED_PATTERNS))
    return _nlp


# ── Noise patterns — checked against full entity string ──────────────────────
# For patterns that can't be caught by exact set lookup
import re as _re
_NOISE_PATTERNS = _re.compile(
    r'^(the |a |an |\d+$|https?://|\w{1,2}$|#|")',
    _re.IGNORECASE,
)
def _is_noise(text: str) -> bool:

    """Return True if the entity should be filtered out."""
    if not text:
        return True
    # Block non-ASCII heavy strings (foreign language sentences)
    non_ascii = sum(1 for c in text if ord(c) > 127)
    if non_ascii > 2:
        return True
    if 'http' in text or '](https' in text or text.startswith('www.'):
        return True
    if text[0].isdigit():
        return True
    if len(text.split()) >= 4:
        return True
    if text in NOISE_ENTITIES:
        return True
    if _NOISE_PATTERNS.match(text):
        return True
    if '\r' in text or '\n' in text or len(text) > 40:
        return True
    return False


def extract_topics(text: str, max_topics: int = 8) -> list[str]:
    """
    Extract named entity topics from text.

    Returns a deduplicated list of lowercased entity strings,
    filtered to relevant labels and noise-free, capped at max_topics.

    Examples:
      "Sam Altman announced OpenAI's GPT-5"
      → ["sam altman", "openai", "gpt-5"]

      "Kafka 3.7 released by Apache with new Raft consensus"
      → ["kafka", "apache"]

      "dbt Cloud now supports PySpark transformations"
      → ["dbt", "pyspark"]
    """
    if not text or not text.strip():
        return []

    try:
        nlp = _load_model()
    except Exception as e:
        logger.warning("spaCy unavailable — topic extraction skipped: %s", e)
        return []

    try:
        doc = nlp(text[:1000])  # cap input length for performance

        seen  = set()
        topics = []

        for ent in doc.ents:
            if ent.label_ not in RELEVANT_LABELS:
                continue

            normalised = ent.text.strip().lower()
            normalised = _canonicalise(normalised)

            # Skip noise, short tokens, numeric-only
            if (_is_noise(normalised)
                    or len(normalised) < 2
                    or normalised.isdigit()
                    or normalised in seen):
                continue

            seen.add(normalised)
            topics.append(normalised)

            if len(topics) >= max_topics:
                break

        return topics

    except Exception:
        logger.exception("Topic extraction failed for text: %.80s", text)
        return []


def extract_topics_batch(texts: list[str], max_topics: int = 8) -> list[list[str]]:
    """
    Batch version of extract_topics. Uses spaCy's nlp.pipe() which is
    significantly faster than calling extract_topics() in a loop because
    it processes texts in parallel through the pipeline.

    Use this in flush_signal_batch() instead of calling extract_topics()
    per signal — at batch_size=50 this is ~3x faster.
    """
    if not texts:
        return []

    try:
        nlp = _load_model()
    except Exception as e:
        logger.warning("spaCy unavailable: %s", e)
        return [[] for _ in texts]

    try:
        results = []
        # nlp.pipe processes all texts in one pass through the model
        for doc in nlp.pipe(
            [t[:1000] if t else "" for t in texts],
            batch_size=32,
        ):
            seen, topics = set(), []
            for ent in doc.ents:
                if ent.label_ not in RELEVANT_LABELS:
                    continue
                normalised = ent.text.strip().lower()
                normalised = _canonicalise(normalised)
                if (normalised in NOISE_ENTITIES
                        or len(normalised) < 2
                        or normalised.isdigit()
                        or normalised in seen):
                    continue
                seen.add(normalised)
                topics.append(normalised)
                if len(topics) >= max_topics:
                    break
            results.append(topics)
        return results

    except Exception:
        logger.exception("Batch topic extraction failed.")
        return [[] for _ in texts]
