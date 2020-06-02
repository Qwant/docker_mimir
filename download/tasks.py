import fnmatch
import hashlib
import invoke
import json
import os
import re
import requests
import sys
import tempfile
from datetime import datetime, timedelta
from os import path, walk

from invoke import task


STATUS_FILE_NAME = "_files_status.json"


def get_md5_from_url(url):
    try:
        res = requests.get(url)
        res.raise_for_status()
    except Exception as err:
        print(f"failed to get hash from {url}: {err}", file=sys.stderr)
        return None

    return res.text.split()[0]


def raw_files_status(ctx):
    files_status_path = path.join(ctx.cache_dir, STATUS_FILE_NAME)

    if path.isfile(files_status_path):
        with open(files_status_path) as data:
            return json.load(data)

    return dict()


def get_file_status(ctx, filename):
    res = raw_files_status(ctx).get(filename)

    if res is None:
        res = {"last_update": None, "md5": None}

    if res["last_update"] is not None:
        res["last_update"] = datetime.utcfromtimestamp(res["last_update"])

    return res


def save_file_status(ctx, filename, status):
    files_status_path = path.join(ctx.cache_dir, STATUS_FILE_NAME)

    if status is not None:
        status["last_update"] = status["last_update"].timestamp()

    files_status = raw_files_status(ctx)
    files_status[filename] = status

    with open(files_status_path, "w") as data:
        json.dump(files_status, data)


def needs_to_download(ctx, filename, max_age=None, md5_url=None):
    """
    Check if "filename" should be downloaded because it doesn't exist or is
    older than "max_age" or its checksum has changed.
    """
    if not path.isfile(filename):
        return True

    file_status = get_file_status(ctx, filename)

    if max_age is not None and (
        file_status["last_update"] is None
        or datetime.utcnow() - file_status["last_update"] > max_age
    ):
        return True

    if md5_url is not None:
        expt_md5 = get_md5_from_url(md5_url)
        curr_md5 = file_status["md5"]

        if expt_md5 is None or expt_md5 != curr_md5:
            return True

    if ctx.force_downloads:
        print(f"'force_downloads' is set to true: existing filename {filename} will be ignored")
        return True

    print(f"'{filename}' already exists, we don't need to download it again")
    return False


def download_file(ctx, filename, url, max_age=None, md5_url=None):
    if not needs_to_download(ctx, filename, max_age, md5_url):
        return

    # Forget current informations about file in case the download fails.
    save_file_status(ctx, filename, None)

    ctx.run(f"mkdir -p {path.dirname(filename)}/")
    ctx.run(f"wget --progress=dot:giga -O {filename} {url}")

    with open(filename, "rb") as f:
        md5 = hashlib.md5()
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
        md5 = md5.hexdigest()

    if md5_url is not None:
        expt_md5 = get_md5_from_url(md5_url)

        if md5 != expt_md5:
            raise Exception(f"md5 at {md5_url} didn't match for {url}")

    save_file_status(ctx, filename, {"last_update": datetime.utcnow(), "md5": md5})


@task
def file_exists(ctx, path):
    """
    Exit with code 0 if a the provided path exists and is a file, overwise exit
    with code 1.
    """
    if not os.path.isfile(path):
        raise invoke.Exit(code=1)


@task
def remove_directory(ctx, path):
    ctx.run(f"rm -rf {path}")


@task
def download_osm(ctx, osm_url, output_file):
    download_file(
        ctx, output_file, osm_url, md5_url=osm_url + ".md5",
    )


@task
def download_bano(ctx, bano_url, output_file):
    src_file = path.join(ctx.cache_dir, "bano.csv.gz")
    download_file(ctx, src_file, bano_url, max_age=timedelta(days=7))
    ctx.run(f"gunzip -c {src_file} > {output_file}")


@task
def download_oa(ctx, filename, oa_url, oa_filter, output_dir):
    src_file = path.join(ctx.cache_dir, filename)
    download_file(ctx, src_file, oa_url, max_age=timedelta(days=7))

    oa_tmp_dir = path.join(ctx.tmp_dir, "oa")
    print(f"Unzipping {src_file}...")
    ctx.run(f"unzip -o -qq -d {oa_tmp_dir} {src_file}")
    print("Done unzipping!")

    # Collect the list of files to include in the output.
    keep_patterns = (fnmatch.translate(pat) for pat in oa_filter.split(","))
    pattern = re.compile("|".join(keep_patterns))
    included_files = []

    for dirname, _, files in walk(oa_tmp_dir):
        for filename in files:
            full_path = path.join(dirname, filename)
            relt_path = path.relpath(full_path, oa_tmp_dir)

            if pattern.match(relt_path):
                included_files.append(full_path)

    # Flatten all .csv into output directory.
    print("Collect OpenAddresses data")
    ctx.run(f"mkdir -p {output_dir}")

    for piece in included_files:
        flat_name = path.relpath(path.join(filename, piece), oa_tmp_dir).replace("/", "__")
        print(f" -> add {flat_name}")
        ctx.run(f"mv {piece} {output_dir}/{flat_name}")
