"""Thread-safe S3 client wrapper for upload, download, and checkpoint storage."""

import json
import io
import logging
import os
import tempfile
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

class NDJSONWriter:
    """Disk-backed NDJSON writer that avoids memory accumulation.

    Records are written to a temp file on disk.  The file is uploaded to S3
    periodically (every ``upload_every`` records) so that progress survives
    crashes, and once more on ``close()``.

    Usage:
        writer = NDJSONWriter(s3_store, "path/to/data.ndjson")
        for item in items:
            writer.append(item)
        all_items = writer.read_all()   # read back for sort / CSV
        writer.close()                  # final upload + temp cleanup
    """

    def __init__(self, s3: 'S3Store', s3_path: str, upload_every: int = 500):
        self._s3 = s3
        self._s3_path = s3_path
        self._upload_every = upload_every
        # Prefer a real-disk temp dir over /tmp which may be tmpfs (RAM-backed).
        # Fall back to the default temp directory if the preferred path doesn't exist.
        tmp_dir = None
        for candidate in ("/var/tmp",):
            if os.path.isdir(candidate):
                tmp_dir = candidate
                break
        self._tmpfile = tempfile.NamedTemporaryFile(
            mode="w", suffix=".ndjson", delete=False, dir=tmp_dir,
        )
        self._tmppath = self._tmpfile.name
        self.count = 0

    def append(self, record: dict) -> None:
        self._tmpfile.write(json.dumps(record, default=str) + "\n")
        self.count += 1
        if self.count % self._upload_every == 0:
            self._upload()

    def _upload(self) -> None:
        self._tmpfile.flush()
        self._s3.upload_file(
            self._tmppath, self._s3_path, content_type="application/json",
        )

    def read_all(self) -> list[dict]:
        """Read all records back from disk (for sorting / CSV generation)."""
        self._tmpfile.flush()
        items = []
        with open(self._tmppath) as f:
            for line in f:
                line = line.strip()
                if line:
                    items.append(json.loads(line))
        return items

    def close(self) -> None:
        """Final upload and temp-file cleanup."""
        self._upload()
        self._tmpfile.close()
        try:
            os.unlink(self._tmppath)
        except OSError:
            pass
