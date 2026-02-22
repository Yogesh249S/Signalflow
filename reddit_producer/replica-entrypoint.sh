#!/bin/bash
# replica-entrypoint.sh
# =====================
# Custom entrypoint for the postgres-replica container.
#
# WHY NOT initdb.d:
#   The TimescaleDB image ships its own initdb.d scripts
#   (000_install_timescaledb.sh, 001_timescaledb_tune.sh) that run BEFORE
#   any user-supplied initdb.d scripts. By the time our script would run,
#   initdb has already created PG_VERSION, so pg_basebackup refuses to
#   write into a non-empty directory.
#
# HOW THIS WORKS:
#   We are set as the container ENTRYPOINT. We run pg_basebackup into PGDATA
#   BEFORE handing off to the normal docker-entrypoint.sh. Since PGDATA is
#   already populated with a full clone of the primary, docker-entrypoint.sh
#   sees PG_VERSION and skips initdb entirely, going straight to:
#       exec postgres -c shared_buffers=... -c max_connections=...
#   The -R flag on pg_basebackup wrote standby.signal + primary_conninfo, so
#   postgres starts in hot_standby mode and begins streaming WAL.
#
# IDEMPOTENT:
#   On restart (PGDATA already has replication data), we skip pg_basebackup
#   and hand straight off to docker-entrypoint.sh.

set -e

PRIMARY_HOST="${POSTGRES_PRIMARY_HOST:-postgres}"
PRIMARY_USER="${POSTGRES_USER:-reddit}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"

echo "==> replica-entrypoint: checking PGDATA at $PGDATA"

if [ -f "$PGDATA/standby.signal" ]; then
    echo "==> replica-entrypoint: standby.signal found — already a replica, skipping pg_basebackup."
elif [ -f "$PGDATA/PG_VERSION" ]; then
    # PGDATA exists but no standby.signal — was initialised as a primary.
    # Wipe and reseed.
    echo "==> replica-entrypoint: PG_VERSION found but no standby.signal — wiping and reseeding."
    rm -rf "$PGDATA"/*
    echo "==> replica-entrypoint: running pg_basebackup from $PRIMARY_HOST..."
    PGPASSWORD="$POSTGRES_PASSWORD" pg_basebackup \
        -h "$PRIMARY_HOST" \
        -U "$PRIMARY_USER" \
        -D "$PGDATA" \
        -Fp -Xs -R -P \
        --checkpoint=fast
    echo "==> replica-entrypoint: pg_basebackup complete."
else
    echo "==> replica-entrypoint: PGDATA empty — running pg_basebackup from $PRIMARY_HOST..."
    PGPASSWORD="$POSTGRES_PASSWORD" pg_basebackup \
        -h "$PRIMARY_HOST" \
        -U "$PRIMARY_USER" \
        -D "$PGDATA" \
        -Fp -Xs -R -P \
        --checkpoint=fast
    echo "==> replica-entrypoint: pg_basebackup complete."
fi

echo "==> replica-entrypoint: handing off to docker-entrypoint.sh"
exec docker-entrypoint.sh "$@"
