#!/bin/bash
# replica-init.sh
# ================
# Mounted at /docker-entrypoint-initdb.d/00_replica_init.sh
#
# docker-entrypoint.sh only runs initdb scripts when PGDATA is empty.
# When PGDATA already contains PG_VERSION (i.e. pg_basebackup already ran),
# the entrypoint skips initdb entirely and goes straight to `postgres`,
# so this script is a no-op on subsequent restarts.
#
# On first boot (empty volume):
#   1. docker-entrypoint.sh sees empty PGDATA and would normally run initdb.
#   2. This script runs BEFORE initdb via the 00_ prefix, wipes the temp
#      initdb state, and replaces it with a full pg_basebackup clone.
#   3. -R writes standby.signal + primary_conninfo automatically.
#   4. postgres then starts in standby (hot_standby) mode.

set -e

PRIMARY_HOST="${POSTGRES_PRIMARY_HOST:-postgres}"
PRIMARY_USER="${POSTGRES_USER:-reddit}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"

echo "==> replica-init: checking if PGDATA is already seeded..."

if [ -f "$PGDATA/PG_VERSION" ]; then
    echo "==> replica-init: PGDATA already contains a database, skipping pg_basebackup."
    exit 0
fi

echo "==> replica-init: PGDATA is empty. Running pg_basebackup from $PRIMARY_HOST..."

# Clear anything docker-entrypoint wrote before calling us
rm -rf "$PGDATA"/*

PGPASSWORD="$POSTGRES_PASSWORD" pg_basebackup \
    -h "$PRIMARY_HOST" \
    -U "$PRIMARY_USER" \
    -D "$PGDATA" \
    -Fp \
    -Xs \
    -R \
    -P \
    --checkpoint=fast

echo "==> replica-init: pg_basebackup complete. standby.signal written by -R flag."
echo "==> replica-init: postgres will start in hot_standby mode."

