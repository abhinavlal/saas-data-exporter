"""Thread-safe S3 client wrapper for upload, download, and checkpoint storage."""

import json
import io
import logging
from pathlib import Path

import boto3
import boto3.session
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

MB = 1024 * 1024

SMALL_FILE_CONFIG = TransferConfig(
    multipart_threshold=128 * MB,
    use_threads=False,
)

LARGE_FILE_CONFIG = TransferConfig(
    multipart_threshold=64 * MB,
    multipart_chunksize=64 * MB,
    max_concurrency=20,
    use_threads=True,
)


class S3Store:
    """
    Thread-safe S3 storage backend.

    Create once, pass to all threads. The underlying boto3 client
    instance is thread-safe once created.
    """

    def __init__(self, bucket: str, prefix: str = ""):
        session = boto3.session.Session()
        self._client = session.client(
            "s3",
            config=BotocoreConfig(
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=50,
            ),
        )
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _key(self, path: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def upload_file(self, local_path: str | Path, s3_path: str,
                    content_type: str | None = None) -> None:
        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        size = Path(local_path).stat().st_size
        config = LARGE_FILE_CONFIG if size > 64 * MB else SMALL_FILE_CONFIG
        self._client.upload_file(
            Filename=str(local_path),
            Bucket=self.bucket,
            Key=self._key(s3_path),
            Config=config,
            ExtraArgs=extra or None,
        )

    def upload_bytes(self, data: bytes, s3_path: str,
                     content_type: str = "application/octet-stream") -> None:
        self._client.put_object(
            Bucket=self.bucket,
            Key=self._key(s3_path),
            Body=data,
            ContentType=content_type,
        )

    def upload_json(self, obj: dict | list, s3_path: str) -> None:
        self.upload_bytes(
            json.dumps(obj, indent=2, default=str).encode(),
            s3_path,
            content_type="application/json",
        )

    def download_json(self, s3_path: str) -> dict | list | None:
        try:
            resp = self._client.get_object(
                Bucket=self.bucket, Key=self._key(s3_path),
            )
            return json.loads(resp["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def exists(self, s3_path: str) -> bool:
        try:
            self._client.head_object(
                Bucket=self.bucket, Key=self._key(s3_path),
            )
            return True
        except ClientError:
            return False

    def upload_stream(self, stream: io.IOBase, s3_path: str,
                      content_type: str = "application/octet-stream") -> None:
        self._client.upload_fileobj(
            Fileobj=stream,
            Bucket=self.bucket,
            Key=self._key(s3_path),
            Config=LARGE_FILE_CONFIG,
            ExtraArgs={"ContentType": content_type},
        )
