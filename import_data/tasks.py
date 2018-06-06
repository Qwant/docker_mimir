
from invoke import task
import logging


def run_rust_binary(ctx, bin, params):
    logging.debug('running: {bin} {params}'.format(**locals()))
    ctx.run('{bin} {params}'.format(**locals()))


@task()
def generate_cosmogony(ctx):
    with ctx.cd(ctx.cosmogony.directory):
        ctx.run('mkdir -p {ctx.cosmogony.output_dir}'.format(**locals()))
        cosmogony_file = '{ctx.cosmogony.output_dir}/cosmogony.json'.format(**locals())
        run_rust_binary(ctx, 'cosmogony', 
        '--input {ctx.osm_file} \
        --output {cosmogony_file}'.format(**locals()))
        ctx.cosmogony.file = cosmogony_file


@task()
def load_cosmogony(ctx):
    run_rust_binary(ctx, 'cosmogony2mimir', 
        '--input {ctx.cosmogony.file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}'.format(**locals()))


@task()
def load_osm(ctx):
    import_admin = '' if _use_cosmogony(ctx) else '--import-admin'

    run_rust_binary(ctx, 'osm2mimir', 
        '--input {ctx.osm_file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}\
        --import-way \
        {import_admin}'.format(**locals()))


@task()
def load_addresses(ctx):
    if 'bano_file' in ctx:
        run_rust_binary(ctx, 'bano2mimir', 
            '--input {ctx.bano_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}'.format(**locals()))
    if 'oa_file' in ctx:
        # TODO take multiples oa files ?
        run_rust_binary(ctx, 'openaddresses2mimir', 
            '--input {ctx.oa_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}'.format(**locals()))


@task()
def load_pois(ctx):
    if 'poi' not in ctx:
        logging.info("no poi to import")
        return

    if 'fafnir' in ctx.poi:
        if ctx.poi.fafnir.get('load_db') is True:
            # TODO import data in PG
            logging.warn("for the moment we can't load data in postgres for fafnir")

        run_rust_binary(ctx, 'fafnir', 
            '--es {ctx.es} \
            --pg {ctx.poi.fafnir.pg}'.format(**locals()))
    else:
        # TODO take a custom poi_config
        run_rust_binary(ctx, 'osm2mimir', 
            '--input {ctx.osm_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}\
            --import-poi'.format(**locals()))


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
