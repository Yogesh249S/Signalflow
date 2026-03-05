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
RELEVANT_LABELS = {"ORG", "PERSON", "PRODUCT", "GPE", "NORP"}

# ── Tech seed patterns ────────────────────────────────────────────────────────
# Only terms spaCy's en_core_web_sm/md doesn't reliably detect.
# Keep this small — spaCy handles the rest.
# Format: {"label": ENTITY_TYPE, "pattern": text_or_pattern}
TECH_SEED_PATTERNS = [
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
}

# ── Model loader ──────────────────────────────────────────────────────────────
_nlp = None


def _load_model():
    """
    Load spaCy model once at first call. Lazy so the import doesn't
    block container startup if the model isn't downloaded yet.

    Uses en_core_web_md (medium) if available — better NER accuracy
    for tech content. Falls back to en_core_web_sm (small).

    Install:
      python -m spacy download en_core_web_md   # recommended
      python -m spacy download en_core_web_sm   # fallback
    """
    global _nlp
    if _nlp is not None:
        return _nlp

    import spacy

    for model in ("en_core_web_md", "en_core_web_sm"):
        try:
            _nlp = spacy.load(model, disable=["parser", "lemmatizer"])
            # Disable parser and lemmatizer — we only need NER and tokenizer.
            # This makes the pipeline ~30% faster.
            logger.info("spaCy model loaded: %s", model)
            break
        except OSError:
            continue

    if _nlp is None:
        raise RuntimeError(
            "No spaCy model found. Run: python -m spacy download en_core_web_md"
        )

    # Add EntityRuler BEFORE the NER component so seed patterns take priority.
    # overwrite_ents=True means our patterns win if NER disagrees.
    ruler = _nlp.add_pipe("entity_ruler", before="ner", config={"overwrite_ents": True})
    ruler.add_patterns(TECH_SEED_PATTERNS)
    logger.info("EntityRuler loaded with %d seed patterns.", len(TECH_SEED_PATTERNS))

    return _nlp


# ── Public API ────────────────────────────────────────────────────────────────

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

            # Skip noise, short tokens, numeric-only
            if (normalised in NOISE_ENTITIES
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
