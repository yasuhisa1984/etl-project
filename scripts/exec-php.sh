#!/usr/bin/env bash
set -euo pipefail
# PHPコンテナへ入る（vimやpingで疎通確認可能）
docker compose exec php-scraper bash
