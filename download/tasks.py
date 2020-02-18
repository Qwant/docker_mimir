import fnmatch
import re
import tempfile
from os import path, walk

from invoke import task


@task
def download_osm(ctx, osm_url, output_file):
    ctx.run(f"mkdir -p {path.dirname(output_file)}")
    ctx.run(f"wget --progress=dot:giga {osm_url} -O {output_file}")


@task
def download_bano(ctx, bano_url, output_file):
    ctx.run(f"mkdir -p {path.dirname(output_file)}")
    gzip_file = f"{output_file}.gz"
    ctx.run(f"wget --progress=dot:giga {bano_url} -O {gzip_file}")
    ctx.run(f"gunzip -f {gzip_file}")


@task
def download_oa(ctx, oa_filter):
    tmp_dir = tempfile.mkdtemp()

    tmp_file = path.join(tmp_dir, "oa.zip")
    ctx.run(f"mkdir -p {path.dirname(tmp_file)}")
    ctx.run(f"wget --progress=dot:giga {ctx.oa_url} -O {tmp_file}")

    oa_tmp_dir = path.join(tmp_dir, "oa")
    ctx.run(f"unzip -o -qq -d {oa_tmp_dir} {tmp_file}")
    ctx.run(f"rm {tmp_file}")

    # Collect the list of files to include in the output
    keep_patterns = (fnmatch.translate(pat) for pat in oa_filter.split(","))
    pattern = re.compile("|".join(keep_patterns))
    included_files = []

    for dirname, _, files in walk(oa_tmp_dir):
        for filename in files:
            full_path = path.join(dirname, filename)
            relt_path = full_path.lstrip(oa_tmp_dir)

            if pattern.match(relt_path):
                included_files.append(full_path)

    # Flatten all .csv to the root of a single director
    output_dir = path.join(ctx.data_dir, "addresses", "oa")
    ctx.run(f"mkdir -p {output_dir}")
    ctx.run(f"rm -rf {output_dir}/*")

    for filename in included_files:
        flat_name = filename.lstrip(oa_tmp_dir).replace("/", "__")
        print(f"{flat_name:>40} ...", flush=True)
        ctx.run(f"mv {filename} {output_dir}/{flat_name}")
