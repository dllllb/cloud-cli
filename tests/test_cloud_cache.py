import hashlib
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import cloud_cache


class FakeS3Key:
    def __init__(self, etag='"etag-1"', payload=b"data"):
        self.etag = etag
        self.payload = payload
        self.get_contents_to_filename = MagicMock(side_effect=self._write)

    def _write(self, filename):
        Path(filename).write_bytes(self.payload)


class FakeS3Bucket:
    def __init__(self, key):
        self.get_key = MagicMock(return_value=key)


class FakeS3Conn:
    def __init__(self, bucket):
        self.get_bucket = MagicMock(return_value=bucket)


class FakeBlob:
    def __init__(self, etag="etag-1", payload=b"blob"):
        self.etag = etag
        self.payload = payload
        self.download_to_filename = MagicMock(side_effect=self._write)

    def _write(self, filename):
        Path(filename).write_bytes(self.payload)


class FakeGcsBucket:
    def __init__(self, blob):
        self.get_blob = MagicMock(return_value=blob)


class FakeGcsClient:
    def __init__(self, bucket):
        self.bucket = MagicMock(return_value=bucket)


class FakeHttpResponse:
    def __init__(self, status_code, chunks=None):
        self.status_code = status_code
        self._chunks = chunks or []

    def iter_content(self, chunk_size=128):
        del chunk_size
        return iter(self._chunks)


@pytest.fixture(autouse=True)
def _identity_tqdm(monkeypatch):
    monkeypatch.setattr(cloud_cache, "tqdm", lambda it: it)


@pytest.fixture
def cache_home(tmp_path, monkeypatch):
    monkeypatch.setattr(
        cloud_cache.os.path,
        "expanduser",
        lambda path: path.replace("~", str(tmp_path)),
    )
    return tmp_path


def install_fake_google_storage(monkeypatch, client):
    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    storage_module = types.ModuleType("google.cloud.storage")
    storage_module.Client = MagicMock(return_value=client)
    cloud_module.storage = storage_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)


def test_s3cache_download_missing_file_downloads_and_writes_digest(cache_home, monkeypatch):
    key = FakeS3Key(etag='"abc123"', payload=b"hello")
    conn = FakeS3Conn(FakeS3Bucket(key))
    monkeypatch.setattr(cloud_cache.boto, "connect_s3", MagicMock(return_value=conn))

    path = cloud_cache.s3cache_download("bucket", "a/b.txt", cache_prefix="cache")

    expected = cache_home / ".cache" / "bucket" / "a" / "b.txt"
    assert path == str(expected)
    assert expected.read_bytes() == b"hello"
    assert Path(f"{path}.digest").read_text(encoding="utf-8") == "abc123"
    key.get_contents_to_filename.assert_called_once_with(path)


def test_s3cache_check_update_up_to_date_does_not_download(cache_home, monkeypatch):
    local = cache_home / ".cache" / "bucket" / "obj.txt"
    local.parent.mkdir(parents=True)
    local.write_bytes(b"old")
    Path(f"{local}.digest").write_text("same", encoding="utf-8")

    key = FakeS3Key(etag='"same"', payload=b"new")
    conn = FakeS3Conn(FakeS3Bucket(key))
    monkeypatch.setattr(cloud_cache.boto, "connect_s3", MagicMock(return_value=conn))

    cloud_cache.s3cache_download("bucket", "obj.txt", cache_prefix="cache", check_update=True)

    key.get_contents_to_filename.assert_not_called()
    assert local.read_bytes() == b"old"


def test_s3cache_raises_when_key_missing(cache_home, monkeypatch):
    conn = FakeS3Conn(FakeS3Bucket(None))
    monkeypatch.setattr(cloud_cache.boto, "connect_s3", MagicMock(return_value=conn))

    with pytest.raises(RuntimeError):
        cloud_cache.s3cache_download("bucket", "missing.txt", cache_prefix="cache")


def test_gcs_download_missing_file_downloads_and_writes_digest(cache_home, monkeypatch):
    blob = FakeBlob(etag="etag-x", payload=b"gcs")
    client = FakeGcsClient(FakeGcsBucket(blob))
    install_fake_google_storage(monkeypatch, client)

    path = cloud_cache.gcs_cache_download("gcs-bucket", "dir/file", cache_prefix="cache")

    expected = cache_home / ".cache" / "gcs-bucket" / "dir" / "file"
    assert path == str(expected)
    assert expected.read_bytes() == b"gcs"
    assert Path(f"{path}.digest").read_text(encoding="utf-8") == "etag-x"
    blob.download_to_filename.assert_called_once_with(path)


def test_gcs_raises_when_blob_missing(cache_home, monkeypatch):
    client = FakeGcsClient(FakeGcsBucket(None))
    install_fake_google_storage(monkeypatch, client)

    with pytest.raises(RuntimeError):
        cloud_cache.gcs_cache_download("gcs-bucket", "missing", cache_prefix="cache")


def test_http_missing_file_downloads(cache_home, monkeypatch):
    response = FakeHttpResponse(200, chunks=[b"a", b"b"])
    monkeypatch.setattr(cloud_cache.requests, "get", MagicMock(return_value=response))

    path = cloud_cache.http_cache_download("https://example.com/f.txt", cache_prefix="cache")

    expected = cache_home / ".cache" / "example.com" / "f.txt"
    assert path == str(expected)
    assert expected.read_bytes() == b"ab"


def test_http_check_update_uses_if_none_match_header(cache_home, monkeypatch):
    cached = cache_home / ".cache" / "example.com" / "f.txt"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"payload")

    mocked_get = MagicMock(return_value=FakeHttpResponse(304))
    monkeypatch.setattr(cloud_cache.requests, "get", mocked_get)

    cloud_cache.http_cache_download("https://example.com/f.txt", cache_prefix="cache", check_update=True)

    expected_etag = hashlib.sha1(b"payload").hexdigest()
    assert mocked_get.call_args.kwargs["headers"]["If-None-Match"] == expected_etag
    assert cached.read_bytes() == b"payload"


def test_http_check_update_raises_on_bad_status_by_default(cache_home, monkeypatch):
    cached = cache_home / ".cache" / "example.com" / "f.txt"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"payload")

    monkeypatch.setattr(cloud_cache.requests, "get", MagicMock(return_value=FakeHttpResponse(500)))

    with pytest.raises(RuntimeError):
        cloud_cache.http_cache_download("https://example.com/f.txt", cache_prefix="cache", check_update=True)


def test_http_check_update_non_failing_mode_does_not_raise(cache_home, monkeypatch):
    cached = cache_home / ".cache" / "example.com" / "f.txt"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"payload")

    monkeypatch.setattr(cloud_cache.requests, "get", MagicMock(return_value=FakeHttpResponse(500)))

    cloud_cache.http_cache_download(
        "https://example.com/f.txt",
        cache_prefix="cache",
        check_update=True,
        fail_on_check_failure=False,
    )


def test_http_root_path_defaults_to_index_html(cache_home, monkeypatch):
    response = FakeHttpResponse(200, chunks=[b"x"])
    monkeypatch.setattr(cloud_cache.requests, "get", MagicMock(return_value=response))

    path = cloud_cache.http_cache_download("https://example.com", cache_prefix="cache")

    expected = cache_home / ".cache" / "example.com" / "index.html"
    assert path == str(expected)
    assert expected.read_bytes() == b"x"


def test_s3cache_dry_run_when_file_exists_with_check_update(cache_home, monkeypatch):
    item = cache_home / ".cache" / "bucket" / "obj.txt"
    item.parent.mkdir(parents=True)
    item.write_text("x", encoding="utf-8")

    connect = MagicMock()
    monkeypatch.setattr(cloud_cache.boto, "connect_s3", connect)

    path = cloud_cache.s3cache_download(
        "bucket", "obj.txt", cache_prefix="cache", dry_run=True, check_update=True
    )

    assert path == str(item)
    connect.assert_not_called()


def test_s3cache_dry_run_when_file_missing(cache_home, monkeypatch):
    connect = MagicMock()
    monkeypatch.setattr(cloud_cache.boto, "connect_s3", connect)

    path = cloud_cache.s3cache_download("bucket", "missing.txt", cache_prefix="cache", dry_run=True)

    assert path.endswith("/.cache/bucket/missing.txt")
    connect.assert_not_called()


def test_s3cache_check_update_downloads_when_digest_differs(cache_home, monkeypatch):
    local = cache_home / ".cache" / "bucket" / "obj.txt"
    local.parent.mkdir(parents=True)
    local.write_bytes(b"old")
    Path(f"{local}.digest").write_text("old-digest", encoding="utf-8")

    key = FakeS3Key(etag='"new-digest"', payload=b"new")
    conn = FakeS3Conn(FakeS3Bucket(key))
    monkeypatch.setattr(cloud_cache.boto, "connect_s3", MagicMock(return_value=conn))

    cloud_cache.s3cache_download("bucket", "obj.txt", cache_prefix="cache", check_update=True)

    assert local.read_bytes() == b"new"
    assert Path(f"{local}.digest").read_text(encoding="utf-8") == "new-digest"


def test_gcs_dry_run_paths_do_not_hit_api(cache_home, monkeypatch):
    called = {"client": 0}

    class DummyClient:
        def __init__(self):
            called["client"] += 1

    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    storage_module = types.ModuleType("google.cloud.storage")
    storage_module.Client = DummyClient
    cloud_module.storage = storage_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)

    existing = cache_home / ".cache" / "gcs" / "a.txt"
    existing.parent.mkdir(parents=True)
    existing.write_text("x", encoding="utf-8")

    p1 = cloud_cache.gcs_cache_download("gcs", "a.txt", cache_prefix="cache", dry_run=True)
    p2 = cloud_cache.gcs_cache_download("gcs", "b.txt", cache_prefix="cache", dry_run=True, check_update=True)

    assert p1.endswith("/.cache/gcs/a.txt")
    assert p2.endswith("/.cache/gcs/b.txt")
    assert called["client"] == 0


def test_gcs_check_update_up_to_date_does_not_download(cache_home, monkeypatch):
    local = cache_home / ".cache" / "bucket" / "obj.txt"
    local.parent.mkdir(parents=True)
    local.write_bytes(b"same")
    Path(f"{local}.digest").write_text("etag-1", encoding="utf-8")

    blob = FakeBlob(etag="etag-1", payload=b"new")
    client = FakeGcsClient(FakeGcsBucket(blob))
    install_fake_google_storage(monkeypatch, client)

    cloud_cache.gcs_cache_download("bucket", "obj.txt", cache_prefix="cache", check_update=True)

    blob.download_to_filename.assert_not_called()
    assert local.read_bytes() == b"same"


def test_gcs_check_update_downloads_when_digest_differs(cache_home, monkeypatch):
    local = cache_home / ".cache" / "bucket" / "obj.txt"
    local.parent.mkdir(parents=True)
    local.write_bytes(b"old")
    Path(f"{local}.digest").write_text("old", encoding="utf-8")

    blob = FakeBlob(etag="etag-new", payload=b"new")
    client = FakeGcsClient(FakeGcsBucket(blob))
    install_fake_google_storage(monkeypatch, client)

    cloud_cache.gcs_cache_download("bucket", "obj.txt", cache_prefix="cache", check_update=True)

    assert local.read_bytes() == b"new"
    assert Path(f"{local}.digest").read_text(encoding="utf-8") == "etag-new"


def test_http_dry_run_existing_and_missing(cache_home, monkeypatch):
    existing = cache_home / ".cache" / "example.com" / "f.txt"
    existing.parent.mkdir(parents=True)
    existing.write_text("x", encoding="utf-8")

    mocked = MagicMock()
    monkeypatch.setattr(cloud_cache.requests, "get", mocked)

    p1 = cloud_cache.http_cache_download("https://example.com/f.txt", cache_prefix="cache", dry_run=True)
    p2 = cloud_cache.http_cache_download("https://example.com/missing.txt", cache_prefix="cache", dry_run=True, check_update=True)

    assert p1.endswith("/.cache/example.com/f.txt")
    assert p2.endswith("/.cache/example.com/missing.txt")
    mocked.assert_not_called()


def test_http_update_downloads_when_status_200(cache_home, monkeypatch):
    local = cache_home / ".cache" / "example.com" / "f.txt"
    local.parent.mkdir(parents=True)
    local.write_bytes(b"old")

    monkeypatch.setattr(
        cloud_cache.requests,
        "get",
        MagicMock(return_value=FakeHttpResponse(200, chunks=[b"n", b"e", b"w"])),
    )

    cloud_cache.http_cache_download("https://example.com/f.txt", cache_prefix="cache", check_update=True)

    assert local.read_bytes() == b"new"


def test_http_missing_download_raises_on_non_200(cache_home, monkeypatch):
    monkeypatch.setattr(cloud_cache.requests, "get", MagicMock(return_value=FakeHttpResponse(503)))

    with pytest.raises(RuntimeError, match="can't download file"):
        cloud_cache.http_cache_download("https://example.com/f.txt", cache_prefix="cache")


def test_http_with_local_path_and_empty_dirname(cache_home, monkeypatch):
    del cache_home
    monkeypatch.setattr(
        cloud_cache.requests,
        "get",
        MagicMock(return_value=FakeHttpResponse(200, chunks=[b"a"])),
    )

    path = cloud_cache.http_cache_download("https://example.com/file", local_path="local.bin")

    assert path == "local.bin"
    assert Path("local.bin").read_bytes() == b"a"
    Path("local.bin").unlink()


def test_cli_commands_echo_result_paths(monkeypatch, capsys):
    monkeypatch.setattr(cloud_cache, "s3cache_download", lambda *args, **kwargs: "/tmp/s3")
    monkeypatch.setattr(cloud_cache, "gcs_cache_download", lambda *args, **kwargs: "/tmp/gcs")
    monkeypatch.setattr(cloud_cache, "http_cache_download", lambda *args, **kwargs: "/tmp/http")

    cloud_cache.s3cache("b", "k")
    cloud_cache.gcs_cache("b", "k")
    cloud_cache.http_cache("https://example.com")

    out = capsys.readouterr().out
    assert "/tmp/s3" in out
    assert "/tmp/gcs" in out
    assert "/tmp/http" in out


def test_main_invokes_app(monkeypatch):
    called = MagicMock()
    monkeypatch.setattr(cloud_cache, "app", called)

    cloud_cache.main()

    called.assert_called_once_with()
