"""
storage/migrate.py
==================
PHASE 1: Flyway-style schema migration runner.

PROBLEM:
  The original schema.sql was loaded once at container init via Docker's
  /docker-entrypoint-initdb.d/ mechanism. Any schema change required either:
  (a) manual ALTER TABLE on the live database — error-prone, undocumented
  (b) dropping and recreating the entire database — loses all data

SOLUTION:
  A versioned migration system modelled on Flyway:
  - Migration files live in storage/migrations/ and are named V{N}__{desc}.sql
  - A schema_migrations table tracks which versions have been applied
  - This runner applies only new migrations, in version order, on startup
  - All migrations run in a transaction — a failure rolls back cleanly

USAGE:
  python -m storage.migrate                    # apply all pending migrations
  python -m storage.migrate --dry-run          # show what would be applied
  python -m storage.migrate --status           # show applied vs pending

  Add to docker-compose.yml as a one-shot init container:
    migrate:
      build: { context: ., dockerfile: processing/Dockerfile }
      command: python -m storage.migrate
      depends_on: { postgres: { condition: service_healthy } }
      restart: "no"

  Or call from your entrypoint before starting the main service.
"""

import argparse
import glob
import logging
import os
import re
import sys
import time

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("migrate")

DSN = (
    f"dbname={os.environ.get('POSTGRES_DB', 'reddit')} "
    f"user={os.environ.get('POSTGRES_USER', 'reddit')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'reddit')} "
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')}"
)

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")

# Filename pattern: V1__description.sql, V2__add_foo.sql, …
_FILENAME_RE = re.compile(r"^V(\d+)__(.+)\.sql$")


def _connect(retries: int = 10) -> psycopg2.extensions.connection:
    """Connect to Postgres with exponential back-off retry."""
    delay = 3.0
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(DSN)
            conn.autocommit = False
            logger.info("Connected to Postgres.")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("Postgres not ready (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    raise RuntimeError("Cannot connect to Postgres after retries.")


def _ensure_migrations_table(cur) -> None:
    """Create schema_migrations table if it doesn't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     TEXT PRIMARY KEY,
            description TEXT,
            applied_at  TIMESTAMP DEFAULT NOW()
        )
    """)


def _applied_versions(cur) -> set[str]:
    """Return the set of already-applied migration versions."""
    cur.execute("SELECT version FROM schema_migrations")
    return {row[0] for row in cur.fetchall()}


def _discover_migrations() -> list[tuple[int, str, str, str]]:
    """
    Scan the migrations directory and return a sorted list of
    (version_int, version_str, description, filepath) tuples.
    """
    files = glob.glob(os.path.join(MIGRATIONS_DIR, "V*.sql"))
    migrations = []
    for filepath in files:
        filename = os.path.basename(filepath)
        m = _FILENAME_RE.match(filename)
        if not m:
            logger.warning("Skipping non-standard migration file: %s", filename)
            continue
        version_int = int(m.group(1))
        description = m.group(2).replace("_", " ")
        version_str = f"V{version_int}"
        migrations.append((version_int, version_str, description, filepath))
    return sorted(migrations, key=lambda x: x[0])


def run_migrations(dry_run: bool = False) -> int:
    """
    Apply all pending migrations. Returns the number of migrations applied.
    Raises on any failure (with automatic rollback).
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            _ensure_migrations_table(cur)
            conn.commit()

            applied = _applied_versions(cur)
            all_migrations = _discover_migrations()
            pending = [(vi, vs, desc, fp) for vi, vs, desc, fp in all_migrations
                       if vs not in applied]

        if not pending:
            logger.info("All migrations already applied. Schema is up to date.")
            return 0

        logger.info(
            "Found %d pending migration(s): %s",
            len(pending),
            [vs for _, vs, _, _ in pending],
        )

        if dry_run:
            logger.info("DRY RUN — no changes applied.")
            return len(pending)

        applied_count = 0
        for version_int, version_str, description, filepath in pending:
            logger.info("Applying %s — %s …", version_str, description)
            with open(filepath, "r", encoding="utf-8") as f:
                sql = f.read()

            with conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (version, description) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (version_str, description),
                    )
                    conn.commit()
                    logger.info("  ✓ %s applied successfully.", version_str)
                    applied_count += 1
                except Exception as exc:
                    conn.rollback()
                    logger.error("  ✗ %s failed: %s — rolled back.", version_str, exc)
                    raise

        logger.info("Migration complete. %d migration(s) applied.", applied_count)
        return applied_count

    finally:
        conn.close()


def show_status() -> None:
    """Print a table of applied and pending migrations."""
    conn = _connect()
    try:
        with conn.cursor() as cur:
            _ensure_migrations_table(cur)
            conn.commit()
            applied = _applied_versions(cur)
    finally:
        conn.close()

    all_migrations = _discover_migrations()
    print(f"\n{'Version':<10} {'Status':<12} {'Description'}")
    print("-" * 60)
    for _, version_str, description, _ in all_migrations:
        status = "APPLIED" if version_str in applied else "PENDING"
        print(f"{version_str:<10} {status:<12} {description}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Pulse schema migration runner")
    parser.add_argument("--dry-run",  action="store_true", help="Show pending migrations without applying")
    parser.add_argument("--status",   action="store_true", help="Show migration status and exit")
    args = parser.parse_args()

    if args.status:
        show_status()
        sys.exit(0)

    count = run_migrations(dry_run=args.dry_run)
    sys.exit(0 if count >= 0 else 1)
