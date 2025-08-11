#!/usr/bin/env bash
set -euo pipefail
# Pythonコンテナへ入る
docker compose exec python-transform bash
