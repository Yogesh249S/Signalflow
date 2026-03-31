"""
topic_summariser.py
-------------------
Async job that runs every 15 minutes.
Reads top trending topics from the DB, calls an LLM to generate:
  - summary_text:             3-sentence narrative of what's being discussed
  - divergence_explanation:   1-sentence explanation of why platforms diverge
Writes results to topic_summaries table.

Provider is controlled by LLM_PROVIDER env var:
  openai    → gpt-4o-mini      (~$0.015/hr at current volume)
  anthropic → claude-haiku-3-5 (comparable cost)
  groq      → llama-3.3-70b     (free tier, faster)
  gemini    → gemini-2.0-flash  (free tier via Google AI Studio, higher quality)

Architecture: sits OUTSIDE the hot path.
  Kafka → processing → signals table  (unchanged, no latency impact)
  topic_summariser job reads signals table every 15 min → writes topic_summaries

Run as a separate Docker service:
  docker compose -f docker-compose.hetzner.yml up -d topic-summariser
"""

import os
import json
import time
import logging
import asyncio
import httpx
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from typing import Optional

# ── CONFIG ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [summariser] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname=os.getenv("POSTGRES_DB", "reddit"),
    user=os.getenv("POSTGRES_USER", "reddit"),
    password=os.getenv("POSTGRES_PASSWORD", "reddit"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
)

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")
import itertools

OPENAI_KEY    = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_KEY    = os.getenv("GEMINI_API_KEY", "")  # kept for single-key fallback

# ── Gemini key pool — round-robin across multiple keys to multiply rate limit ──
# Add keys as GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc. in .env / docker-compose.
# Falls back to GEMINI_API_KEY if no numbered keys found.
# Free tier = 15 req/min per key — 2 keys = 30 req/min combined.
# KEY ROTATION HAPPENS AT THE WORKER LEVEL (run_once), not inside call_gemini.
# Each worker owns one key for the full run — keys never contend within a single
# topic's retry loop.
def _load_gemini_keys() -> list:
    keys = []
    i = 1
    while True:
        k = os.getenv(f"GEMINI_API_KEY_{i}", "")
        if not k:
            break
        keys.append(k)
        i += 1
    if not keys:
        k = os.getenv("GEMINI_API_KEY", "")
        if k:
            keys.append(k)
    return keys

GEMINI_KEYS = _load_gemini_keys()
_gemini_key_cycle = itertools.cycle(GEMINI_KEYS) if GEMINI_KEYS else iter([])

# ── Daily quota tracking ───────────────────────────────────────────────────────
# 2000 req/day hard ceiling across ALL keys combined.
# Each key is a separate Google account = separate 1500 req/day quota at Google,
# but we self-impose 2000 total so we never get close to any single key's limit.
# Counters reset at midnight UTC via _maybe_reset_daily_counters().
DAILY_REQUEST_LIMIT = int(os.getenv("GEMINI_DAILY_REQUEST_LIMIT", 2000))

# Per-key daily counters — index matches GEMINI_KEYS list
_daily_counts: list[int] = []      # populated after GEMINI_KEYS is known
_daily_reset_date: str = ""        # "YYYY-MM-DD" — date of last reset

def _init_daily_counters():
    global _daily_counts, _daily_reset_date
    _daily_counts = [0] * len(GEMINI_KEYS)
    _daily_reset_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _maybe_reset_daily_counters():
    """Reset per-key counters if we've crossed into a new UTC day."""
    global _daily_counts, _daily_reset_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != _daily_reset_date:
        log.info(f"new UTC day ({today}) — resetting daily quota counters")
        _daily_counts = [0] * len(GEMINI_KEYS)
        _daily_reset_date = today

def _total_daily_requests() -> int:
    return sum(_daily_counts)

def _increment_key_counter(key: str):
    if key in GEMINI_KEYS:
        _daily_counts[GEMINI_KEYS.index(key)] += 1

def _daily_quota_log():
    total = _total_daily_requests()
    per_key = ", ".join(
        f"key{i+1}={_daily_counts[i]}" for i in range(len(GEMINI_KEYS))
    )
    remaining = DAILY_REQUEST_LIMIT - total
    log.info(f"daily quota: {total}/{DAILY_REQUEST_LIMIT} used ({per_key}) — {remaining} remaining")

# ── Groq key pool — round-robin across multiple keys to multiply rate limit ──
# Add keys as GROQ_API_KEY_1, GROQ_API_KEY_2, GROQ_API_KEY_3 in .env
# Falls back to GROQ_API_KEY if no numbered keys found.
# Each free key = 30 req/min — 3 keys = 90 req/min combined.
def _load_groq_keys() -> list:
    keys = []
    i = 1
    while True:
        k = os.getenv(f"GROQ_API_KEY_{i}", "")
        if not k:
            break
        keys.append(k)
        i += 1
    if not keys:
        k = os.getenv("GROQ_API_KEY", "")
        if k:
            keys.append(k)
    return keys

GROQ_KEYS = _load_groq_keys()
_groq_key_cycle = itertools.cycle(GROQ_KEYS) if GROQ_KEYS else iter([])
GROQ_KEY  = GROQ_KEYS[0] if GROQ_KEYS else ""  # kept for validation check

POLL_INTERVAL  = int(os.getenv("SUMMARISER_INTERVAL_SECONDS", 900))   # 15 min
WINDOW_MINUTES = int(os.getenv("SUMMARISER_WINDOW_MINUTES", 60))
MIN_SIGNALS    = int(os.getenv("SUMMARISER_MIN_SIGNALS", 10))          # skip tiny topics
TOP_SIGNALS    = int(os.getenv("SUMMARISER_TOP_SIGNALS", 25))          # signals sent to LLM
MAX_TOPICS     = int(os.getenv("SUMMARISER_MAX_TOPICS", 50))           # topics per run
SUMMARY_TTL    = int(os.getenv("SUMMARISER_TTL_MINUTES", 30))          # skip if fresh


# ── DB SCHEMA ─────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS topic_summaries (
    id                      BIGSERIAL PRIMARY KEY,
    topic                   TEXT NOT NULL,
    window_minutes          INTEGER NOT NULL DEFAULT 60,
    generated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- narrative fields
    summary_text            TEXT,           -- 3-sentence summary of discourse
    divergence_explanation  TEXT,           -- why platforms disagree (null if no divergence)
    dominant_narrative      TEXT,           -- 1 phrase: what most people are saying
    emerging_angle          TEXT,           -- 1 phrase: minority view / counter-narrative

    -- metadata
    signal_count            INTEGER,
    platform_count          INTEGER,
    platforms               JSONB,
    avg_sentiment           FLOAT,
    model_used              TEXT,
    prompt_tokens           INTEGER,
    completion_tokens       INTEGER,
    latency_ms              INTEGER,

    UNIQUE (topic, window_minutes)
);

CREATE INDEX IF NOT EXISTS idx_topic_summaries_topic_generated
    ON topic_summaries (topic, generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_summaries_generated
    ON topic_summaries (generated_at DESC);
"""


# ── LLM CLIENTS ───────────────────────────────────────────────────────────────

async def call_openai(prompt: str, client: httpx.AsyncClient) -> tuple[str, int, int]:
    """Returns (response_text, prompt_tokens, completion_tokens)"""
    resp = await client.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}"},
        json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300,
            "temperature": 0.3,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    text = data["choices"][0]["message"]["content"].strip()
    usage = data.get("usage", {})
    return text, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)


async def call_anthropic(prompt: str, client: httpx.AsyncClient) -> tuple[str, int, int]:
    resp = await client.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    text = data["content"][0]["text"].strip()
    usage = data.get("usage", {})
    return text, usage.get("input_tokens", 0), usage.get("output_tokens", 0)


async def call_groq(
    prompt: str,
    client: httpx.AsyncClient,
    api_key: str = "",
) -> tuple[str, int, int]:
    key = api_key or (GROQ_KEYS[0] if GROQ_KEYS else "")
    resp = await client.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}"},
        json={
            "model": "llama-3.3-70b-versatile",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300,
            "temperature": 0.3,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    text = data["choices"][0]["message"]["content"].strip()
    usage = data.get("usage", {})
    return text, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)


async def call_gemini(
    prompt: str,
    client: httpx.AsyncClient,
    api_key: str = "",
) -> tuple[str, int, int]:
    """Gemini 2.0 Flash via Google AI Studio.

    Uses only the key it is given — NO internal key switching.
    Key rotation happens at the worker level in run_once() so each key's
    rate limit budget is consumed independently. Alternating keys inside
    this function burns both keys simultaneously on a single topic's retry
    loop, which is what caused the cascade of 429s in the logs.

    Backoff: 10s then 20s. Free tier resets per minute so 10s is enough
    to recover partial headroom without waiting a full 60s.
    """
    key = api_key or (GEMINI_KEYS[0] if GEMINI_KEYS else "")
    key_idx = (GEMINI_KEYS.index(key) + 1) if key in GEMINI_KEYS else "?"

    for attempt in range(3):  # 1 attempt + 2 retries on this key only
        if attempt > 0:
            wait = attempt * 10  # 10s, 20s
            log.info(f"Gemini 429 — waiting {wait}s before retry {attempt}/2 (key{key_idx})")
            await asyncio.sleep(wait)

        resp = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={key}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": 300,
                },
            },
            timeout=60,
        )

        if resp.status_code == 429:
            if attempt == 2:
                resp.raise_for_status()  # exhausted — caller logs the warning and moves on
            continue

        resp.raise_for_status()
        _increment_key_counter(key)   # count successful requests against daily quota
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        usage = data.get("usageMetadata", {})
        return text, usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0)


async def call_llm(
    prompt: str,
    client: httpx.AsyncClient,
    api_key: str = "",
) -> tuple[str, int, int]:
    if LLM_PROVIDER == "gemini":
        return await call_gemini(prompt, client, api_key=api_key)
    elif LLM_PROVIDER == "anthropic":
        return await call_anthropic(prompt, client)
    elif LLM_PROVIDER == "groq":
        return await call_groq(prompt, client, api_key=api_key)
    else:
        return await call_openai(prompt, client)


# ── PREFLIGHT CHECK ──────────────────────────────────────────────────────────

async def gemini_keys_available(client: httpx.AsyncClient) -> bool:
    """
    Fire one cheap probe request per key before starting a run.
    If ALL keys return 429 immediately, the daily quota is likely exhausted
    and there's no point hammering 20 topics × 3 retries each.
    Returns True if at least one key is responsive.
    """
    if LLM_PROVIDER != "gemini":
        return True

    probe_prompt = '{"test": true}'  # minimal token usage
    available = []
    for i, key in enumerate(GEMINI_KEYS):
        try:
            resp = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={key}",
                json={"contents": [{"parts": [{"text": probe_prompt}]}],
                      "generationConfig": {"maxOutputTokens": 5}},
                timeout=15,
            )
            if resp.status_code == 429:
                log.warning(f"preflight: key{i+1} is rate-limited (429)")
                available.append(False)
            else:
                log.info(f"preflight: key{i+1} is responsive ({resp.status_code})")
                available.append(True)
        except Exception as e:
            log.warning(f"preflight: key{i+1} error — {e}")
            available.append(False)

    return any(available)


# ── PROMPTS ───────────────────────────────────────────────────────────────────

def build_prompt(topic: str, signals: list[dict], platform_sentiment: dict) -> str:
    """
    Build a focused prompt. We send signal titles/bodies (not raw API responses)
    to the LLM. ~500 tokens input, ~150 tokens output per topic.
    """
    import re
    _noise_re = re.compile(
        r'^(lol|lmao|wow|wtf|omg|this|same|yes|no|ok|okay|true|facts?|thread|'
        r'breaking|watch|read|listen|check|see|look)[.!?\s]*$',
        re.IGNORECASE,
    )

    snippets = []
    for s in signals[:TOP_SIGNALS]:
        text = (s.get("title") or s.get("body") or "").strip()
        if not text:
            continue
        text = re.sub(r'https?://\S+', '', text).strip()
        if len(text) < 8:
            continue
        if _noise_re.match(text):
            continue
        words = text.split()
        specials = len(re.findall(r'[@#]\w+', text))
        if len(words) > 0 and specials / len(words) > 0.6:
            continue
        pf = s.get("platform", "?")[:3].upper()
        snippets.append(f"[{pf}] {text[:200]}")

    if not snippets:
        return ""

    pf_lines = []
    for pf, data in platform_sentiment.items():
        pf_lines.append(
            f"  {pf}: {data['count']} signals, avg sentiment {data['sentiment']:+.2f}"
        )
    pf_block = "\n".join(pf_lines) if pf_lines else "  (single platform)"

    snippets_block = "\n".join(snippets[:20])

    prompt = f"""You are analysing social media signal data for the topic: "{topic}"

Platform breakdown:
{pf_block}

Top signals (platform prefix, then text):
{snippets_block}

Respond ONLY with valid JSON, no markdown, no explanation. Use this exact structure:
{{
  "summary": "3 sentences. What are people actually saying about {topic}? What's driving the volume? What's the overall mood?",
  "divergence": "1 sentence explaining why platforms differ in their coverage, OR null if all platforms cover it similarly.",
  "dominant_narrative": "5-8 words: the main thing most people are saying",
  "emerging_angle": "5-8 words: minority view or counter-narrative, OR null"
}}"""

    return prompt


# ── DB HELPERS ────────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(**PG)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    log.info("schema ok")


def get_trending_topics(conn) -> list[dict]:
    TOPIC_BLOCKLIST = (
        "un", "us", "uk", "eu", "it", "he", "she", "we", "me", "my",
        "the", "this", "that", "they", "them", "you", "your", "our",
        "am", "pm", "st", "nd", "rd", "th",
        "co", "inc", "ltd", "llc", "gov", "org",
        "rt", "via", "re", "cc",
        "bbc news", "bbc newscast", "dw news", "al jazeera", "sky news",
        "abc news", "cnn", "msnbc", "fox news", "nbc news", "cbs news",
        "ryan exclusive:", "c - computerphile", "upfront",
        "iembot", "additional details here",
    )

    sql = """
        SELECT
            unnested_topic                          AS topic,
            COUNT(*)                                AS signal_count,
            COUNT(DISTINCT platform)                AS platform_count,
            array_agg(DISTINCT platform)            AS platforms,
            AVG(sentiment_compound)                 AS avg_sentiment
        FROM (
            SELECT
                jsonb_array_elements_text(topics) AS unnested_topic,
                platform,
                sentiment_compound
            FROM signals
            WHERE last_updated_at >= NOW() - INTERVAL '%s minutes'
              AND topics IS NOT NULL
              AND jsonb_array_length(topics) > 0
        ) sub
        WHERE LENGTH(unnested_topic) >= 3
          AND LENGTH(unnested_topic) <= 40
          AND LOWER(unnested_topic) != ALL(%s::text[])
          AND unnested_topic NOT LIKE '%%\r%%'
          AND unnested_topic NOT LIKE '%% - %%'
          AND unnested_topic NOT LIKE '%%:\t%%'
        GROUP BY unnested_topic
        HAVING COUNT(*) >= %s
        ORDER BY COUNT(*) DESC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (WINDOW_MINUTES, list(TOPIC_BLOCKLIST), MIN_SIGNALS, MAX_TOPICS))
        return [dict(r) for r in cur.fetchall()]


def get_signals_for_topic(conn, topic: str) -> list[dict]:
    sql = """
        SELECT
            platform,
            title,
            body,
            author,
            sentiment_compound,
            trending_score,
            score_velocity
        FROM signals
        WHERE last_updated_at >= NOW() - INTERVAL '%s minutes'
          AND topics @> to_jsonb(ARRAY[%s::text])
        ORDER BY COALESCE(trending_score, 0) DESC, COALESCE(score_velocity, 0) DESC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (WINDOW_MINUTES, topic, TOP_SIGNALS))
        return [dict(r) for r in cur.fetchall()]


def get_platform_sentiment(signals: list[dict]) -> dict:
    result = {}
    for s in signals:
        pf = s.get("platform", "unknown")
        sent = s.get("sentiment_compound") or 0
        if pf not in result:
            result[pf] = {"count": 0, "sentiment_sum": 0.0}
        result[pf]["count"] += 1
        result[pf]["sentiment_sum"] += sent
    for pf, data in result.items():
        data["sentiment"] = data["sentiment_sum"] / data["count"] if data["count"] else 0
    return result


def summary_is_fresh(conn, topic: str) -> bool:
    sql = """
        SELECT 1 FROM topic_summaries
        WHERE topic = %s
          AND generated_at >= NOW() - INTERVAL '%s minutes'
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (topic, SUMMARY_TTL))
        return cur.fetchone() is not None


def write_summary(conn, topic: str, topic_meta: dict, parsed: dict,
                  model: str, prompt_tok: int, completion_tok: int, latency_ms: int):
    sql = """
        INSERT INTO topic_summaries (
            topic, window_minutes, generated_at,
            summary_text, divergence_explanation,
            dominant_narrative, emerging_angle,
            signal_count, platform_count, platforms, avg_sentiment,
            model_used, prompt_tokens, completion_tokens, latency_ms
        ) VALUES (
            %s, %s, NOW(),
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (topic, window_minutes) DO UPDATE SET
            summary_text = EXCLUDED.summary_text,
            divergence_explanation = EXCLUDED.divergence_explanation,
            dominant_narrative = EXCLUDED.dominant_narrative,
            emerging_angle = EXCLUDED.emerging_angle,
            generated_at = EXCLUDED.generated_at,
            signal_count = EXCLUDED.signal_count,
            platform_count = EXCLUDED.platform_count,
            platforms = EXCLUDED.platforms,
            avg_sentiment = EXCLUDED.avg_sentiment,
            model_used = EXCLUDED.model_used,
            prompt_tokens = EXCLUDED.prompt_tokens,
            completion_tokens = EXCLUDED.completion_tokens,
            latency_ms = EXCLUDED.latency_ms
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            topic,
            WINDOW_MINUTES,
            parsed.get("summary"),
            parsed.get("divergence"),
            parsed.get("dominant_narrative"),
            parsed.get("emerging_angle"),
            topic_meta.get("signal_count"),
            topic_meta.get("platform_count"),
            json.dumps(list(topic_meta.get("platforms") or [])),
            topic_meta.get("avg_sentiment"),
            model,
            prompt_tok,
            completion_tok,
            latency_ms,
        ))
    conn.commit()


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────

async def process_topic(
    topic_meta: dict,
    conn,
    client: httpx.AsyncClient,
    api_key: str = "",
) -> Optional[dict]:
    topic = topic_meta["topic"]

    if await asyncio.to_thread(summary_is_fresh, conn, topic):
        log.debug(f"skip {topic!r} — fresh summary exists")
        return None

    signals = await asyncio.to_thread(get_signals_for_topic, conn, topic)
    if not signals:
        log.debug(f"skip {topic!r} — no signals found")
        return None

    authors = [s.get("author") for s in signals if s.get("author")]
    if authors:
        top_author_share = max(authors.count(a) for a in set(authors)) / len(authors)
        if top_author_share > 0.8:
            log.info(f"skip {topic!r} — dominated by single author ({top_author_share:.0%} share, spam)")
            return None

    pf_sentiment = get_platform_sentiment(signals)
    prompt = build_prompt(topic, signals, pf_sentiment)
    if not prompt:
        log.debug(f"skip {topic!r} — no usable signal text")
        return None

    t0 = time.monotonic()
    try:
        raw, prompt_tok, completion_tok = await call_llm(prompt, client, api_key=api_key)
        latency_ms = int((time.monotonic() - t0) * 1000)
    except Exception as e:
        log.warning(f"LLM error for {topic!r}: {e}")
        return None

    try:
        raw_clean = raw.strip()
        if raw_clean.startswith("```"):
            raw_clean = raw_clean.split("```")[1]
            if raw_clean.startswith("json"):
                raw_clean = raw_clean[4:]
        parsed = json.loads(raw_clean.strip())
    except json.JSONDecodeError:
        log.warning(f"bad JSON from LLM for {topic!r}: {raw[:100]}")
        return None

    model_name = {
        "openai": "gpt-4o-mini",
        "anthropic": "claude-haiku-4-5-20251001",
        "groq": "llama-3.3-70b-versatile",
        "gemini": "gemini-2.0-flash",
    }.get(LLM_PROVIDER, LLM_PROVIDER)

    await asyncio.to_thread(
        write_summary, conn, topic, topic_meta, parsed, model_name,
        prompt_tok, completion_tok, latency_ms,
    )

    log.info(
        f"summarised {topic!r} | {topic_meta['signal_count']} signals "
        f"| {latency_ms}ms | {prompt_tok}+{completion_tok} tokens"
    )
    return parsed


# Gemini free tier: 15 req/min per key.
# With 2 keys and concurrency=2: each worker owns one key, processes topics
# serially within that key. Inter-topic sleep of 5s = 12 req/min per key
# (well under the 15 limit), 24 req/min total across both keys.
LLM_CONCURRENCY = int(os.getenv("SUMMARISER_CONCURRENCY", 2))


async def run_once(conn, client: httpx.AsyncClient):
    # Brief pause at start of every run — gives Gemini's per-minute window
    # time to recover if the previous run left the keys partially exhausted.
    _maybe_reset_daily_counters()   # reset counters if it's a new UTC day

    if LLM_PROVIDER == "gemini":
        _daily_quota_log()
        if _total_daily_requests() >= DAILY_REQUEST_LIMIT:
            log.warning(
                f"daily quota already at limit ({DAILY_REQUEST_LIMIT}) before run started. "
                f"Skipping. Resets at midnight UTC."
            )
            return

    log.info("waiting 65s for Gemini rate-limit window to recover...")
    await asyncio.sleep(65)

    # Preflight — probe all keys before burning quota on 20 topics.
    # If every key is still 429 after the wait, daily quota is likely exhausted.
    # Abort the run and sleep until the next poll interval.
    if not await gemini_keys_available(client):
        log.error(
            "preflight failed — all Gemini keys are rate-limited. "
            "Daily quota may be exhausted. Check https://aistudio.google.com/apikey "
            "Skipping this run entirely."
        )
        return

    topics = await asyncio.to_thread(get_trending_topics, conn)
    log.info(f"found {len(topics)} topics with >={MIN_SIGNALS} signals")

    async def get_age(t):
        fresh = await asyncio.to_thread(summary_is_fresh, conn, t['topic'])
        return (1 if fresh else 0, -t.get('signal_count', 0))

    import asyncio as _aio
    ages = await _aio.gather(*[get_age(t) for t in topics])
    topics = [t for _, t in sorted(zip(ages, topics), key=lambda x: x[0])]
    log.info(f"topics reordered — stale first, by signal count")

    semaphore = asyncio.Semaphore(LLM_CONCURRENCY)

    # ── Key pool + daily quota gate ───────────────────────────────────────────
    # For Gemini: 4 keys, each owned by a different Google account.
    # Worker assignment: topics are distributed round-robin across available keys
    # so each key handles ~1/4 of the topics per run.
    # Before each topic, check the combined daily total against the 2000 limit.
    _key_pool = {
        "groq": GROQ_KEYS,
        "gemini": GEMINI_KEYS,
    }.get(LLM_PROVIDER, [])

    # Round-robin counter — increments per topic so consecutive topics
    # go to different keys, distributing load evenly across all 4.
    rr_counter = 0
    rr_lock = asyncio.Lock()

    async def process_with_sem(topic_meta: dict) -> Optional[dict]:
        nonlocal rr_counter

        # Hard daily quota gate — checked before every topic
        if LLM_PROVIDER == "gemini" and _total_daily_requests() >= DAILY_REQUEST_LIMIT:
            log.warning(
                f"daily quota reached ({DAILY_REQUEST_LIMIT} requests) — "
                f"skipping remaining topics for this run"
            )
            return None

        async with rr_lock:
            # Pick next key in round-robin order, skip any that are individually
            # over their fair share (DAILY_REQUEST_LIMIT / num_keys)
            n_keys = len(_key_pool) if _key_pool else 1
            per_key_soft_limit = DAILY_REQUEST_LIMIT // n_keys
            # Try each key starting from round-robin position
            chosen_key = ""
            for offset in range(n_keys):
                idx = (rr_counter + offset) % n_keys
                key = _key_pool[idx] if _key_pool else ""
                key_count = _daily_counts[idx] if LLM_PROVIDER == "gemini" and _daily_counts else 0
                if key_count < per_key_soft_limit:
                    chosen_key = key
                    rr_counter = (idx + 1) % n_keys  # advance past chosen key
                    break
            if not chosen_key:
                # All keys over soft limit — use strict round-robin as fallback
                chosen_key = _key_pool[rr_counter % n_keys] if _key_pool else ""
                rr_counter = (rr_counter + 1) % max(n_keys, 1)

        async with semaphore:
            result = await process_topic(topic_meta, conn, client, api_key=chosen_key)
            # 10s between topics = 6 req/min per key, well under 15/min — slow drip, no IP ban risk
            await asyncio.sleep(20.0)
            return result

    results = await asyncio.gather(
        *[process_with_sem(t) for t in topics],
        return_exceptions=True,
    )

    skipped = 0
    processed = 0
    errors = 0

    for topic_meta, result in zip(topics, results):
        if isinstance(result, Exception):
            log.error(f"unexpected error for {topic_meta['topic']!r}: {result}")
            errors += 1
        elif result is None:
            skipped += 1
        else:
            processed += 1

    log.info(f"run complete — processed={processed} skipped={skipped} errors={errors}")
    if LLM_PROVIDER == "gemini":
        _daily_quota_log()

    # ── RETRY PASS ────────────────────────────────────────────────────────────
    errored_topics = [
        topics[i] for i, r in enumerate(results)
        if isinstance(r, Exception)
    ]
    missing_topics = [
        topics[i] for i, r in enumerate(results)
        if r is None and topics[i].get("signal_count", 0) >= 50
    ]
    retry_queue = (errored_topics + missing_topics)[:10]

    if retry_queue:
        log.info(f"retry pass: {len(retry_queue)} topics after 65s rate-limit reset")
        await asyncio.sleep(65)  # wait for Gemini 1-min window to fully reset
        retry_processed = 0
        for topic_meta in retry_queue:
            try:
                result = await process_topic(topic_meta, conn, client)
                if result is not None:
                    retry_processed += 1
                await asyncio.sleep(5)
            except Exception as e:
                log.warning(f"retry failed for {topic_meta['topic']!r}: {e}")
        log.info(f"retry pass complete — processed={retry_processed}/{len(retry_queue)}")


async def main():
    log.info(f"starting topic_summariser | provider={LLM_PROVIDER} | interval={POLL_INTERVAL}s")

    if LLM_PROVIDER == "groq":
        if not GROQ_KEYS:
            log.error("no Groq keys found. Set GROQ_API_KEY_1 (and optionally _2, _3) in .env")
            return
        log.info(f"Groq key pool: {len(GROQ_KEYS)} key(s) — effective limit: {len(GROQ_KEYS) * 30} req/min")
    elif LLM_PROVIDER == "gemini":
        if not GEMINI_KEYS:
            log.error("no Gemini keys found. Set GEMINI_API_KEY_1 (and optionally _2) in .env")
            return
        log.info(
            f"Gemini key pool: {len(GEMINI_KEYS)} key(s) — "
            f"effective limit: {len(GEMINI_KEYS) * 15} req/min | "
            f"daily cap: {DAILY_REQUEST_LIMIT} req/day across all keys "
            f"(~{DAILY_REQUEST_LIMIT // len(GEMINI_KEYS)} per key)"
        )
        _init_daily_counters()
    else:
        key_map = {"openai": OPENAI_KEY, "anthropic": ANTHROPIC_KEY}
        if not key_map.get(LLM_PROVIDER, ""):
            log.error(f"no API key found for provider={LLM_PROVIDER}. Set the env var and restart.")
            return

    conn = get_connection()
    await asyncio.to_thread(ensure_schema, conn)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                await run_once(conn, client)
            except psycopg2.OperationalError as e:
                log.error(f"DB connection lost: {e} — reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_connection()
            except Exception as e:
                log.error(f"run_once failed: {e}")

            log.info(f"sleeping {POLL_INTERVAL}s until next run")
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
