<?php

/**
 * ETL - Extract (抽出)
 * 変更点:
 * - S3のcreateBucketで409(BucketAlreadyOwnedByYou / BucketAlreadyExists)は「既存なのでOK」として無視。
 * - それ以外のエラーはリトライしてから失敗。
 * - キーは毎回ユニーク（time()）で衝突しない。
 */

require __DIR__ . '/vendor/autoload.php';

use Aws\S3\S3Client;
use Aws\Sqs\SqsClient;
use Aws\S3\Exception\S3Exception;

// LocalStackエンドポイント
$endpoint = 'http://localstack:4566';
$region   = 'ap-northeast-1';

$s3 = new S3Client([
    'version' => 'latest',
    'region'  => $region,
    'endpoint' => $endpoint,
    'use_path_style_endpoint' => true, // LocalStackでは推奨
    'credentials' => ['key' => 'test', 'secret' => 'test'],
]);

$sqs = new SqsClient([
    'version' => 'latest',
    'region'  => $region,
    'endpoint' => $endpoint,
    'credentials' => ['key' => 'test', 'secret' => 'test'],
]);

$bucket    = 'etl-bucket';
$queueName = 'etl-queue';

/**
 * シンプルなリトライヘルパ
 * @param callable $fn 実行関数
 * @param int $tries 試行回数
 * @param int $sleepMs スリープms
 * @return mixed
 * @throws Throwable
 */
function retry(callable $fn, int $tries = 10, int $sleepMs = 300)
{
    $last = null;
    for ($i = 0; $i < $tries; $i++) {
        try {
            return $fn();
        } catch (\Throwable $e) {
            $last = $e;
            usleep($sleepMs * 1000);
        }
    }
    throw $last;
}

// --- S3: バケット作成（既存ならOKとして無視） ---
try {
    retry(function () use ($s3, $bucket) {
        $s3->createBucket(['Bucket' => $bucket]);
        return true;
    });
    echo "[Extract] Bucket created or exists: {$bucket}\n";
} catch (S3Exception $e) {
    // 409系は既存なのでOK
    $code = $e->getAwsErrorCode();
    if ($code === 'BucketAlreadyOwnedByYou' || $code === 'BucketAlreadyExists') {
        echo "[Extract] Bucket already exists (ok): {$bucket}\n";
    } else {
        // それ以外は再スロー
        throw $e;
    }
}

// --- ダミーデータ（実務ではここがスクレイピング） ---
$data = [
    ['id' => 1, 'name' => 'Apple',  'price' => 100],
    ['id' => 2, 'name' => 'Banana', 'price' =>  50],
];

// --- S3へアップロード ---
$fileKey = 'data-' . time() . '.json';
retry(function () use ($s3, $bucket, $fileKey, $data) {
    $s3->putObject([
        'Bucket' => $bucket,
        'Key'    => $fileKey,
        'Body'   => json_encode($data, JSON_UNESCAPED_UNICODE),
    ]);
    return true;
});
echo "[Extract] PutObject: {$fileKey}\n";

// --- SQS: キュー作成＆メッセージ送信（createQueueは基本冪等） ---
$queueUrl = retry(function () use ($sqs, $queueName) {
    $res = $sqs->createQueue(['QueueName' => $queueName]);
    return $res['QueueUrl'];
});
retry(function () use ($sqs, $queueUrl, $bucket, $fileKey) {
    $payload = ['bucket' => $bucket, 'key' => $fileKey];
    $sqs->sendMessage([
        'QueueUrl'    => $queueUrl,
        'MessageBody' => json_encode($payload, JSON_UNESCAPED_UNICODE),
    ]);
    return true;
});
echo "[Extract] Sent SQS message for {$fileKey}\n";
