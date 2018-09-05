import glob
from os import path
import tempfile

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
def download_oa(ctx, oa_files):
    temp_dir = tempfile.mkdtemp()

    oa_files = oa_files.split(',')
    oa_temp_dir = path.join(temp_dir, 'oa')

    for oa_filename in oa_files:
        source_url = f"{ctx.oa_base_url}{oa_filename}.zip"
        tmp_file = path.join(temp_dir, f"{oa_filename}.zip")
        ctx.run(f"mkdir -p {path.dirname(tmp_file)}")
        ctx.run(f"wget --progress=dot:giga {source_url} -O {tmp_file}")
        ctx.run(f"unzip -o -qq -d {oa_temp_dir} {tmp_file}")
        ctx.run(f"rm {tmp_file}")

    output_dir = path.join(ctx.data_dir, 'addresses', 'oa')
    ctx.run(f"rm -rf {output_dir}")
    ctx.run(f"mkdir -p {output_dir}")

    # Flatten all .csv to the root of a single directory
    for csv_file in glob.glob(f"{oa_temp_dir}/**/*.csv", recursive=True):
        flat_csv_filename = csv_file.lstrip(oa_temp_dir).replace('/', '__')
        ctx.run(f"mv {csv_file} {output_dir}/{flat_csv_filename}")

