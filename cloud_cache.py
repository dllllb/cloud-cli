#!/usr/bin/env python3
import hashlib
import os
from typing import Iterable, Optional
from urllib.parse import urlparse

import boto
import requests
import typer

app = typer.Typer(no_args_is_help=True)


def iter_with_progress(chunks: Iterable[bytes]):
    try:
        from tqdm import tqdm

        return tqdm(chunks)
    except ImportError:
        return chunks


def s3cache_download(
    bucket_name: str,
    object_key: str,
    cache_prefix: str = "s3cache",
    check_update: bool = False,
    dry_run: bool = False,
):
    cache = os.path.expanduser(f"~/.{cache_prefix}")

    key_parts = [part for part in object_key.split("/") if part]
    item = os.path.join(cache, bucket_name, *key_parts)
    parent = os.path.dirname(item)
    digest_file = f"{item}.digest"

    if dry_run:
        if os.path.exists(item):
            if check_update:
                typer.echo(f"file {item} would be checked for updates")
            else:
                typer.echo(f"file {item} is already stored locally")
        else:
            typer.echo(f"file {item} is missing, would be downloaded")
        return item

    if os.path.exists(item):
        if check_update:
            digest = "none"
            if os.path.exists(digest_file):
                with open(digest_file, "r", encoding="utf-8") as file_obj:
                    digest = file_obj.read()

            conn = boto.connect_s3()
            bucket = conn.get_bucket(bucket_name)
            key = bucket.get_key(object_key)
            if key is None:
                raise RuntimeError(
                    f's3://{bucket_name}/{object_key} does not exist or is inaccessible'
                )
            remote_digest = key.etag.strip('"')

            typer.echo(f"local digest: {digest}")
            typer.echo(f"remote digest: {remote_digest}")

            if remote_digest != digest:
                typer.echo(f"file {item} is outdated, downloading...")
                key.get_contents_to_filename(item)
                with open(digest_file, "w", encoding="utf-8") as file_obj:
                    file_obj.write(remote_digest)
            else:
                typer.echo(f"file {item} is up to date")
        else:
            typer.echo(f"file {item} is already stored locally")
    else:
        typer.echo(f"file {item} is missing, downloading...")

        os.makedirs(parent, exist_ok=True)

        conn = boto.connect_s3()
        bucket = conn.get_bucket(bucket_name)
        key = bucket.get_key(object_key)
        if key is None:
            raise RuntimeError(
                f's3://{bucket_name}/{object_key} does not exist or is inaccessible'
            )

        remote_digest = key.etag.strip('"')
        key.get_contents_to_filename(item)
        with open(digest_file, "w", encoding="utf-8") as file_obj:
            file_obj.write(remote_digest)

    return item


def gcs_cache_download(
    bucket_name: str,
    object_key: str,
    cache_prefix: str = "gcs",
    check_update: bool = False,
    dry_run: bool = False,
):
    from google.cloud import storage

    cache = os.path.expanduser(f"~/.{cache_prefix}")
    key_parts = [part for part in object_key.split("/") if part]
    item = os.path.join(cache, bucket_name, *key_parts)
    parent = os.path.dirname(item)
    digest_file = f"{item}.digest"

    if dry_run:
        if os.path.exists(item):
            if check_update:
                typer.echo(f"file {item} would be checked for updates")
            else:
                typer.echo(f"file {item} is already stored locally")
        else:
            typer.echo(f"file {item} is missing, would be downloaded")
        return item

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(object_key)
    if blob is None:
        raise RuntimeError(
            f'gs://{bucket_name}/{object_key} does not exist or is inaccessible'
        )

    remote_digest = blob.etag

    if os.path.exists(item):
        if check_update:
            digest = "none"
            if os.path.exists(digest_file):
                with open(digest_file, "r", encoding="utf-8") as file_obj:
                    digest = file_obj.read()

            typer.echo(f"local digest: {digest}")
            typer.echo(f"remote digest: {remote_digest}")

            if remote_digest != digest:
                typer.echo(f"file {item} is outdated, downloading...")
                blob.download_to_filename(item)
                with open(digest_file, "w", encoding="utf-8") as file_obj:
                    file_obj.write(remote_digest)
            else:
                typer.echo(f"file {item} is up to date")
        else:
            typer.echo(f"file {item} is already stored locally")
    else:
        typer.echo(f"file {item} is missing, downloading...")

        os.makedirs(parent, exist_ok=True)
        blob.download_to_filename(item)
        with open(digest_file, "w", encoding="utf-8") as file_obj:
            file_obj.write(remote_digest)

    return item


def http_cache_download(
    url: str,
    local_path: Optional[str] = None,
    cache_prefix: str = "http",
    check_update: bool = False,
    fail_on_check_failure: bool = True,
    dry_run: bool = False,
):
    if local_path is None:
        cache = os.path.expanduser(f"~/.{cache_prefix}")
        parsed = urlparse(url)
        host = parsed.hostname or "unknown-host"
        path_parts = [part for part in parsed.path.strip("/").split("/") if part]
        if not path_parts:
            path_parts = ["index.html"]
        cache_file = os.path.join(cache, host, *path_parts)
    else:
        cache_file = local_path

    if dry_run:
        if os.path.exists(cache_file):
            if check_update:
                typer.echo(f"file {cache_file} would be checked for updates")
            else:
                typer.echo(f"file {cache_file} is already stored locally")
        else:
            typer.echo(f"file {cache_file} is missing, would be downloaded")
        return cache_file

    if os.path.exists(cache_file):
        if check_update:
            sha1 = hashlib.sha1()
            with open(cache_file, "rb") as file_obj:
                while True:
                    data = file_obj.read(1000)
                    if not data:
                        break
                    sha1.update(data)

            etag = sha1.hexdigest()
            headers = {"If-None-Match": etag}

            response = requests.get(url, headers=headers, stream=True, timeout=30)

            if response.status_code == 304:
                typer.echo(f"file {cache_file} is up to date")
            elif response.status_code != 200:
                msg = (
                    f"file {cache_file} update check failed, status code: "
                    f"{response.status_code}"
                )
                if fail_on_check_failure:
                    raise RuntimeError(msg)
                typer.echo(msg)
            else:
                typer.echo(f"file {cache_file} is changed, updating...")
                with open(cache_file, "wb") as file_obj:
                    for chunk in iter_with_progress(response.iter_content(chunk_size=128)):
                        if chunk:
                            file_obj.write(chunk)
                typer.echo(f"file {cache_file} is updated")
        else:
            typer.echo(f"file {cache_file} is already stored locally")
    else:
        typer.echo(f"file {cache_file} is missing, downloading...")

        parent = os.path.dirname(cache_file)
        if parent:
            os.makedirs(parent, exist_ok=True)

        response = requests.get(url, stream=True, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"can't download file {cache_file}, status code: {response.status_code}"
            )

        with open(cache_file, "wb") as file_obj:
            for chunk in iter_with_progress(response.iter_content(chunk_size=128)):
                if chunk:
                    file_obj.write(chunk)

        typer.echo(f"file {cache_file} is downloaded")

    return cache_file


@app.command("s3cache")
def s3cache(
    bucket: str = typer.Argument(..., help="S3 bucket name."),
    key: str = typer.Argument(..., help="S3 object key."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print actions only."),
    check_update: bool = typer.Option(
        False, "--check-update", help="Compare local and remote digest before download."
    ),
    cache_prefix: str = typer.Option("s3cache", "--cache-prefix", help="Cache directory prefix."),
):
    """Download and cache an object from S3."""
    local_path = s3cache_download(
        bucket,
        key,
        cache_prefix=cache_prefix,
        check_update=check_update,
        dry_run=dry_run,
    )
    typer.echo(local_path)


@app.command("gcs_cache")
def gcs_cache(
    bucket: str = typer.Argument(..., help="GCS bucket name."),
    key: str = typer.Argument(..., help="GCS object key."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print actions only."),
    check_update: bool = typer.Option(
        False, "--check-update", help="Compare local and remote digest before download."
    ),
    cache_prefix: str = typer.Option("gcs", "--cache-prefix", help="Cache directory prefix."),
):
    """Download and cache an object from Google Cloud Storage."""
    local_path = gcs_cache_download(
        bucket,
        key,
        cache_prefix=cache_prefix,
        check_update=check_update,
        dry_run=dry_run,
    )
    typer.echo(local_path)


@app.command("http_cache")
def http_cache(
    url: str = typer.Argument(..., help="File URL to cache."),
    local_path: Optional[str] = typer.Option(None, "--local-path", help="Explicit local destination path."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print actions only."),
    check_update: bool = typer.Option(
        False, "--check-update", help="Use If-None-Match check for local file updates."
    ),
    fail_on_check_failure: bool = typer.Option(
        True,
        "--fail-on-check-failure/--no-fail-on-check-failure",
        help="Fail when update check returns non-200/non-304.",
    ),
    cache_prefix: str = typer.Option("http", "--cache-prefix", help="Cache directory prefix."),
):
    """Download and cache a file from HTTP(S)."""
    cache_file = http_cache_download(
        url,
        local_path=local_path,
        cache_prefix=cache_prefix,
        check_update=check_update,
        fail_on_check_failure=fail_on_check_failure,
        dry_run=dry_run,
    )
    typer.echo(cache_file)


def main():
    app()


if __name__ == "__main__":
    main()
