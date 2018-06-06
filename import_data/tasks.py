
from invoke import task
import logging


def run_rust_binary(ctx, bin, params):
    logging.debug(f'**************** running: {bin} {params}')
    ctx.run(f'{bin} {params}')


@task()
def generate_cosmogony(ctx):
    with ctx.cd(ctx.cosmogony.directory):
        ctx.run(f'mkdir -p {ctx.cosmogony.output_dir}')
        cosmogony_file = f'{ctx.cosmogony.output_dir}/cosmogony.json'
        run_rust_binary(ctx, 'cosmogony', 
        f'--input {ctx.osm_file} \
        --output {cosmogony_file}')
        ctx.cosmogony.file = cosmogony_file


@task()
def load_cosmogony(ctx):
    run_rust_binary(ctx, 'cosmogony2mimir', 
        f'--input {ctx.cosmogony.file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}')


@task()
def load_osm(ctx):
    import_admin = '' if _use_cosmogony(ctx) else '--import-admin'

    run_rust_binary(ctx, 'osm2mimir', 
        f'--input {ctx.osm_file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}\
        --import-way \
        {import_admin}')


@task()
def load_addresses(ctx):
    if 'bano_file' in ctx:
        run_rust_binary(ctx, 'bano2mimir', 
            f'--input {ctx.bano_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}')
    if 'oa_file' in ctx:
        # TODO take multiples oa files ?
        run_rust_binary(ctx, 'openaddresses2mimir', 
            f'--input {ctx.oa_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}')


@task()
def load_pois(ctx):
    if 'poi' not in ctx:
        logging.info("no poi to import")
        return

    if 'fafnir' in ctx.poi:
        if ctx.poi.fafnir.get('load_db') == True:
            # TODO import data in PG
            logging.warn("for the moment we can't load data in postgres for fafnir")

        run_rust_binary(ctx, 'fafnir', 
            f'--es {ctx.es} \
            --pg {ctx.poi.fafnir.pg}')
    else:
        # TODO take a custom poi_config
        run_rust_binary(ctx, 'osm2mimir', 
            f'--input {ctx.osm_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}\
            --import-poi')


def _use_cosmogony(ctx):
    return 'cosmogony' in ctx


@task(default=True)
def load_all(ctx):
    """
    default task called if `invoke` is run without args
    This is the main tasks that import all the datas into mimir
    """
    if _use_cosmogony(ctx):
        if 'file' not in ctx.cosmogony:
            generate_cosmogony(ctx)
        load_cosmogony(ctx)

    load_osm(ctx)

    load_addresses(ctx)

    load_pois(ctx)
