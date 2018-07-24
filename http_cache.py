

def http_cache(url,
               local_path=None,
               cache_prefix='http',
               check_update=False,
               fail_on_check_failure=True,
               dry_run=False):
    import os
    import hashlib
    import requests

    try:
        from tqdm import tqdm
    except ImportError:
        def tqdm(arg):
            return arg

    if local_path is None:
        cache = os.path.expanduser("~/.{}".format(cache_prefix))

        from urlparse import urlparse

        up = urlparse(url)

        path_parts = [cache, up.hostname] + up.path.strip('/').split("/")
        cache_file = "/".join(path_parts)
    else:
        cache_file = local_path

    if dry_run:
        pass
    elif os.path.exists(cache_file):
        if check_update:
            sha1 = hashlib.sha1()

            with open(cache_file, 'rb') as f:
                while True:
                    data = f.read(1000)
                    if not data:
                        break

                    sha1.update(data)

            etag = sha1.hexdigest()

            headers = {
                'If-None-Match': etag
            }

            r = requests.get(url, headers=headers, stream=True)

            if r.status_code == 304:
                print("file {} is up to date".format(cache_file))
            elif r.status_code != 200:
                if fail_on_check_failure:
                    raise RuntimeError(
                        "can't download file {}, status code: {}".format(
                            cache_file,
                            r.status_code))
                else:
                    print(
                        "file {} update check is failed, status code: {}".format(
                            cache_file,
                            r.status_code))
            else:
                print("file {} is changed, updating...".format(cache_file))

                with open(cache_file, 'wb') as f:
                    for chunk in tqdm(r.iter_content(chunk_size=128)):
                        f.write(chunk)

                print("file {} is updated".format(cache_file))
        else:
            print("file {} is already stored locally".format(cache_file))
    else:
        print("file {} is missing, downloading...".format(cache_file))

        if os.path.dirname(cache_file) != '':
            if not os.path.exists(os.path.dirname(cache_file)):
                os.makedirs(os.path.dirname(cache_file))

        r = requests.get(url, stream=True)

        with open(cache_file, 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=128)):
                f.write(chunk)

        print("file {} is downloaded".format(cache_file))
    return cache_file


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--check-update', action='store_true')
    parser.add_argument('url')
    parser.add_argument('--local-path', default=None)
    args = parser.parse_args()

    print(http_cache(args.url, args.local_path, check_update=args.check_update, dry_run=args.dry_run))

if __name__ == "__main__":
    main()
