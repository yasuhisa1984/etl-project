# ベース: PHP CLI
FROM php:8.2-cli

# 便利ツール: vim（タブ移動/編集）, ping（疎通確認）, tini（PID1）
RUN apt-get update && apt-get install -y \
    unzip git curl vim iputils-ping tini \
 && rm -rf /var/lib/apt/lists/*

# composer をコピー
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

# 作業ディレクトリ
WORKDIR /app

# 先に composer.json を用意してから install
# ここで必要な依存（aws-sdk-php, guzzle）を固定
RUN printf '%s\n' \
'{' \
'  "name": "etl/scraper",' \
'  "type": "project",' \
'  "require": {' \
'    "aws/aws-sdk-php": "^3",' \
'    "guzzlehttp/guzzle": "^7"' \
'  }' \
'}' > /app/composer.json \
 && composer install --no-dev --no-interaction --prefer-dist --no-progress

# スクリプト配置
COPY scraper.php /app/scraper.php

# tini を PID1 に
ENTRYPOINT ["tini","--"]
CMD ["php","/app/scraper.php"]
