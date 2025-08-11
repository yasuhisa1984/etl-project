# ベース: Python 3.11
FROM python:3.11

# 便利ツール: vim（タブ移動/編集）, ping（疎通確認）, tini（PID1）
RUN apt-get update && apt-get install -y vim iputils-ping tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依存ライブラリをインストール
RUN pip install --no-cache-dir boto3 psycopg2-binary

# スクリプト配置
COPY transform.py /app/transform.py

# tiniをPID1に据える
ENTRYPOINT ["tini","--"]
CMD ["python","/app/transform.py"]
