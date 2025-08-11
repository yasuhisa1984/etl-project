#!/usr/bin/env bash
set -euo pipefail
# Postgresへ接続
docker compose exec db psql -U etluser -d etldb
