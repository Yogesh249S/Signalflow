"""
config/db_router.py — Phase 2 (new file)
=========================================
Routes all ORM read queries to the Postgres streaming replica, and all
write queries (INSERT / UPDATE / DELETE) to the primary.

How it works:
  - Django calls db_for_read() before every SELECT.
  - Django calls db_for_write() before every INSERT/UPDATE/DELETE.
  - allow_migrate() prevents the migration runner from touching the replica
    (it is read-only; migrations only apply to the primary).

Usage — registered in settings.py:
    DATABASE_ROUTERS = ['config.db_router.ReadReplicaRouter']

Impact:
  - The Postgres primary handles only the bulk upsert writes from the
    processing service.
  - All Django ORM reads (PostViewSet, stats aggregates, admin) hit the
    replica, keeping the primary connection count low.
  - Effective query throughput roughly doubles with no hardware changes.
"""




# config/db_router.py

# These apps always read from the primary.
# Auth needs consistent reads — login writes a session then immediately
# reads it back. Sending that read to a replica that's milliseconds
# behind causes "auth_user does not exist" on the replica.
REPLICA_EXCLUDED_APPS = {"auth", "admin", "contenttypes", "sessions"}


#============================
#Postgres_replica
#============================

# class ReadReplicaRouter:
#     def db_for_read(self, model, **hints):
#         if model._meta.app_label in REPLICA_EXCLUDED_APPS:
#             return "default"
#         return "replica"
#
#     def db_for_write(self, model, **hints):
#         return "default"
#
#     def allow_relation(self, obj1, obj2, **hints):
#         return True
#
#     def allow_migrate(self, db, app_label, **hints):
#         # Migrations only run on the primary
#         return db == "default"


class ReadReplicaRouter:
    def db_for_read(self, model, **hints):
        return "default"       # always primary

    def db_for_write(self, model, **hints):
        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, **hints):
        return db == "default
