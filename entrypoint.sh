#!/bin/sh
set -e

dbmate --url "$DATABASE_URL" --migrations-dir /app/db/migrations up

exec "$@"
