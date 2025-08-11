"""
ETL - Transform/Load（変換/格納）ワーカー（LocalStack向け）

やること:
- SQS をロングポーリングしてメッセージ受信
- S3 から JSON を取得
- データを少し変換（priceに10%上乗せ）
- PostgreSQL に保存
  - 複合主キー (id, batch_id) でバッチごとに履歴を蓄積
    * batch_id は S3 オブジェクトのキー（＝ファイル名）を流用

注意:
- LocalStack のエンドポイントは http://localstack:4566 固定
- エラー時は簡易リトライ。実運用では DLQ/アラート等を追加推奨
"""

import json
import os
import time
from typing import List, Dict, Any

import boto3
import psycopg2
import psycopg2.extras

AWS_ENDPOINT = "http://localstack:4566"
AWS_REGION = "ap-northeast-1"
QUEUE_NAME = "etl-queue"

# LocalStack向けクライアント
s3 = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=AWS_REGION,
)
sqs = boto3.client(
    "sqs",
    endpoint_url=AWS_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=AWS_REGION,
)

def wait_queue(name: str, tries: int = 60, sleep: float = 1.0) -> str:
    """キューURLが取れるまで待つ"""
    for _ in range(tries):
        try:
            return sqs.get_queue_url(QueueName=name)["QueueUrl"]
        except Exception:
            time.sleep(sleep)
    raise RuntimeError("Queue not ready")

queue_url = wait_queue(QUEUE_NAME)

# DB接続情報（docker compose のサービス名で名前解決）
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "etldb")
DB_USER = os.getenv("DB_USER", "etluser")
DB_PASS = os.getenv("DB_PASS", "etlpass")

def connect_db():
    """PostgreSQL に接続（起動待ちの簡易リトライ付き）"""
    for _ in range(60):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            print(f"[Transform] DB接続待ち: {e}")
            time.sleep(1)
    raise RuntimeError("DB not ready")

conn = connect_db()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def transform(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    変換処理: 価格に10%上乗せ（税込の例）
    実務では正規化・型変換・重複除去・マスタ紐付け等を実施
    """
    out: List[Dict[str, Any]] = []
    for it in items:
        price = int(it["price"])
        out.append(
            {
                "id": int(it["id"]),
                "name": str(it["name"]),
                "price": int(price * 1.1),
            }
        )
    return out

print("[Transform] Worker started. Polling SQS...")

while True:
    try:
        # ロングポーリング（最長10秒待つ）
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            VisibilityTimeout=30,
        )

        messages = resp.get("Messages", [])
        if not messages:
            # キューが空なら少し待機
            time.sleep(1)
            continue

        for msg in messages:
            body = json.loads(msg["Body"])
            bucket = body["bucket"]
            key = body["key"]          # ← これを batch_id に利用

            # S3からJSONを取得
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read())

            # 変換
            rows = transform(data)

            # Load（複合PK: id, batch_id）
            for r in rows:
                cur.execute(
                    "INSERT INTO products (id, name, price, batch_id) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (id, batch_id) DO UPDATE "
                    "SET name = EXCLUDED.name, price = EXCLUDED.price",
                    (r["id"], r["name"], r["price"], key),
                )
            conn.commit()

            print(f"[Transform] Loaded {len(rows)} rows from {key}")

            # 正常時はメッセージ削除
            sqs.delete_message(
                QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
            )

    except Exception as e:
        # 簡易エラーハンドリング（本番はDLQ/リトライ/監視などを整備）
        print(f"[Transform] Error: {e}")
        time.sleep(2)
