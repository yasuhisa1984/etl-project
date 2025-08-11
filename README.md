ETL Template (LocalStack + PHP + Python + Postgres)

このテンプレートは、Extract → Transform → Load の一連の処理をローカルで再現できる環境です。
LocalStack を使って AWS S3 / SQS をエミュレートし、PHP でスクレイピングしたデータを Python で加工して PostgreSQL に格納します。

⸻

構成概要

[PHP Scraper] –(S3 Put + SQS Send)–> [LocalStack S3/SQS] –(Receive)–> [Python Transform] –> [Postgres]
	•	php-scraper: WebやAPIからデータ取得 → S3に保存 → SQSメッセージ送信
	•	python-transform: SQSメッセージを受信 → S3から取得 → データ加工 → Postgres保存
	•	localstack: AWSサービス（S3 / SQS）のローカルエミュレータ
	•	postgres: 加工済みデータの永続化

⸻

ディレクトリ構成

etl-template/
├── docker-compose.yml
├── php-scraper/           # Extract
│   ├── Dockerfile
│   └── scraper.php
├── python-transform/      # Transform & Load
│   ├── Dockerfile
│   └── transform.py
├── scripts/               # DB接続やテスト用
│   ├── psql.sh
│   └── run_scraper.sh
├── sql/                   # スキーマ管理
│   └── init.sql
├── .env.example           # 環境変数サンプル
└── README.md              # このファイル

⸻

セットアップ

1. 環境変数設定

cp .env.example .env

2. コンテナ起動

docker compose up -d

3. Postgresに接続

bash scripts/psql.sh

⸻

データの流れ（ETL）
	1.	Extract
docker compose run –rm php-scraper
→ データ取得 → S3保存 → SQSメッセージ送信
	2.	Transform & Load
python-transform サービスが SQS からメッセージを受信し、データを加工して Postgres に保存

⸻

よく使うコマンド

Postgres接続

bash scripts/psql.sh

S3内のファイル確認

docker compose exec localstack awslocal s3 ls s3://etl-bucket

SQSメッセージ数確認

docker compose exec localstack awslocal sqs get-queue-attributes 
–queue-url $(docker compose exec -T localstack awslocal sqs get-queue-url –queue-name etl-queue –query QueueUrl –output text) 
–attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

⸻

注意点
	•	.env には AWS 認証情報やDB接続情報を設定してください（.env.exampleを参考に）
	•	LocalStackはAWS完全互換ではないため、本番AWS環境とは一部挙動が異なる場合があります

⸻

