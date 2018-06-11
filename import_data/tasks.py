
from invoke import task
from invoke.config import DataProxy
import logging

logging.basicConfig(level=logging.INFO)

def run_rust_binary(ctx, bin, params):
    logging.debug('running: {bin} {params}'.format(**locals()))
    ctx.run('{bin} {params}'.format(**locals()))


@task()
def generate_cosmogony(ctx):
    logging.info("generating cosmogony file")
    with ctx.cd(ctx.admin.cosmogony.directory):
        ctx.run('mkdir -p {ctx.admin.cosmogony.output_dir}'.format(ctx=ctx))
        cosmogony_file = '{ctx.admin.cosmogony.output_dir}/cosmogony.json'.format(ctx=ctx)
        run_rust_binary(ctx, 'cosmogony', 
        '--input {ctx.osm_file} \
        --output {cosmogony_file}'.format(ctx=ctx, cosmogony_file=cosmogony_file))
        ctx.admin.cosmogony.file = cosmogony_file


@task()
def load_cosmogony(ctx):
    logging.info("loading cosmogony")
    run_rust_binary(ctx, 'cosmogony2mimir', 
        '--input {ctx.admin.cosmogony.file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}'.format(ctx=ctx))


@task()
def load_osm(ctx):
    logging.info("importing data from osm")
    import_admin = '' if _use_cosmogony(ctx) else '--import-admin'

    run_rust_binary(ctx, 'osm2mimir', 
        '--input {ctx.osm_file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}\
        --import-way \
        {import_admin}'.format(ctx=ctx, import_admin=import_admin))


@task()
def load_addresses(ctx):
    addr_config = ctx.get('addresses')
    if not _is_config_object(addr_config):
        logging.info("no addresses to import")
        return

    if 'bano_file' in addr_config:
        logging.info("importing bano addresses")
        run_rust_binary(ctx, 'bano2mimir', 
            '--input {ctx.addresses.bano_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}'.format(ctx=ctx))
    if 'oa_file' in addr_config:
        logging.info("importing oa addresses")
        # TODO take multiples oa files ?
        run_rust_binary(ctx, 'openaddresses2mimir', 
            '--input {ctx.addresses.oa_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}'.format(ctx=ctx))


@task()
def load_pois(ctx):
    poi_conf = ctx.get('poi')
    if not _is_config_object(poi_conf):
        logging.info("no poi to import")
        return

    fafnir_conf = poi_conf.get('fafnir')
    if _is_config_object(fafnir_conf):
        if fafnir_conf.get('load_db') is True:
            # TODO import data in PG
            logging.warn("for the moment we can't load data in postgres for fafnir")

        logging.info("importing poi with fafnir")
        run_rust_binary(ctx, 'fafnir', 
            '--es {ctx.es} \
            --pg {fafnir_conf.pg}'.format(ctx=ctx, fafnir_conf=fafnir_conf))
            
    if 'osm' in poi_conf:
        logging.info("importing poi from osm")
        # TODO take a custom poi_config
        run_rust_binary(ctx, 'osm2mimir', 
            '--input {ctx.osm_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}\
            --import-poi'.format(ctx=ctx))


def _use_cosmogony(ctx):
    admin_conf = ctx.get('admin')
    return _is_config_object(admin_conf) and 'cosmogony' in admin_conf


def _is_config_object(obj):
    return obj is not None and (isinstance(obj, DataProxy) or isinstance(obj, dict))


@task(default=True)
def load_all(ctx):
    """
    default task called if `invoke` is run without args
    This is the main tasks that import all the datas into mimir
    """
    if _use_cosmogony(ctx):
        logging.info('using cosmogony')
        if not ctx.admin.cosmogony.get('file'):
            generate_cosmogony(ctx)
        load_cosmogony(ctx)

    load_osm(ctx)

    load_addresses(ctx)

    load_pois(ctx)
