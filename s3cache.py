def s3cache(bucket, key, cache_prefix='s3cache', check_update=False, dry_run=False):
    import os
    import boto

    cache = os.path.expanduser("~/.{prefix}".format(prefix=cache_prefix))

    path_parts = [cache, bucket] + key.split("/")
    item = "/".join(path_parts)
    parent = "/".join(path_parts[:-1])
    digest_file = item + ".digest"

    if dry_run:
        pass
    if os.path.exists(item):
        if check_update:
            digest = "none"
            if os.path.exists(digest_file):
                with open(digest_file) as f:
                    digest = f.read()

            conn = boto.connect_s3()

            bucket = conn.get_bucket(bucket)
            key = bucket.get_key(key)
            remote_digest = key.etag.strip('"')

            print("local digest: {}".format(digest))
            print("remote digest: {}".format(remote_digest))

            if remote_digest != digest:
                print("file %s is outdated, downloading...".format(item))
                key.get_contents_to_filename(item)
                with open(digest_file, 'w') as f:
                    f.write(remote_digest)
            else:
                print("file {} is up to date".format(item))
        else:
            print("file {} is already stored locally".format(item))
    else:
        print("file {} is missing, downloading...".format(item))

        if not os.path.exists(parent):
            os.makedirs(parent)

        conn = boto.connect_s3()

        bucket = conn.get_bucket(bucket)
        key = bucket.get_key(key)
        remote_digest = key.etag.strip('"')

        key.get_contents_to_filename(item)
        with open(digest_file, 'w') as f:
            f.write(remote_digest)

    return item


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--check-update', action='store_true')
    parser.add_argument('bucket')
    parser.add_argument('key')
    args = parser.parse_args()

    print(s3cache(args.bucket, args.key, check_update=args.check_update, dry_run=args.dry_run))

if __name__ == "__main__":
    main()
