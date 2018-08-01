
from invoke import task
from invoke.config import DataProxy
import logging
from retrying import retry
from datetime import timedelta
from time import time

logging.basicConfig(level=logging.INFO)


def run_rust_binary(ctx, container, bin, files, params):
    cmd = "{} {}".format(bin, params)
    if ctx.get("run_on_docker_compose"):
        files_args = _build_docker_files_args(files)
        cmd = "docker-compose {files} run --rm {container} {cmd}".format(
            files=files_args, container=container, cmd=cmd
        )

    logging.info("{sep} {msg} {sep}".format(sep="*" * 15, msg=bin))
    logging.info("running: {}".format(cmd))
    start = time()
    ctx.run(cmd)
    logging.info(
        "{sep} {bin} ran in {time} {sep}".format(
            sep="*" * 15, bin=bin, time=timedelta(seconds=time() - start)
        )
    )


@task()
def generate_cosmogony(ctx, files=[]):
    logging.info("generating cosmogony file")
    if not ctx.get("run_on_docker_compose"):
        # cosmogony needs to be run in the cosmogony directory to access libpostal rules
        cosmogony_dir = ctx.admin.cosmogony.directory
        ctx.run("mkdir -p {ctx.admin.cosmogony.output_dir}".format(ctx=ctx))
    else:
        # for docker the rules are embeded in the docker, we don't have to move
        cosmogony_dir = "."

    with ctx.cd(cosmogony_dir):
        cosmogony_file = "{ctx.admin.cosmogony.output_dir}/cosmogony.json".format(
            ctx=ctx
        )
        run_rust_binary(
            ctx,
            "",
            "cosmogony",
            files,
            "--input {ctx.osm_file} \
        --output {cosmogony_file}".format(
                ctx=ctx, cosmogony_file=cosmogony_file
            ),
        )
        ctx.admin.cosmogony.file = cosmogony_file


@task()
def load_cosmogony(ctx, files=[]):
    logging.info("loading cosmogony")
    run_rust_binary(
        ctx,
        "mimir",
        "cosmogony2mimir",
        files,
        "--input {ctx.admin.cosmogony.file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}".format(
            ctx=ctx
        ),
    )


@task()
def load_osm(ctx, files=[]):
    logging.info("importing data from osm")
    admin_args = ""
    if not _use_cosmogony(ctx):
        osm_args = ctx.admin.get("osm")
        if _is_config_object(osm_args):
            admin_args = "--import-admin"
            for lvl in osm_args["levels"]:
                admin_args += " --level {}".format(lvl)

    poi_conf = ctx.get("poi")
    import_poi = ""
    if poi_conf and "osm" in poi_conf:
        import_poi = "--import-poi"

    run_rust_binary(
        ctx,
        "mimir",
        "osm2mimir",
        files,
        "--input {ctx.osm_file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}\
        --import-way \
        {import_admin} \
        {import_poi}".format(
            ctx=ctx, import_admin=admin_args, import_poi=import_poi
        ),
    )


@task()
def load_addresses(ctx, files=[]):
    addr_config = ctx.get("addresses")
    if not _is_config_object(addr_config):
        logging.info("no addresses to import")
        return

    if "bano_file" in addr_config:
        logging.info("importing bano addresses")
        run_rust_binary(
            ctx,
            "mimir",
            "bano2mimir",
            files,
            "--input {ctx.addresses.bano_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}".format(
                ctx=ctx
            ),
        )
    if "oa_file" in addr_config:
        logging.info("importing oa addresses")
        # TODO take multiples oa files ?
        run_rust_binary(
            ctx,
            "mimir",
            "openaddresses2mimir",
            files,
            "--input {ctx.addresses.oa_file} \
            --connection-string {ctx.es} \
            --dataset {ctx.dataset}".format(
                ctx=ctx
            ),
        )


@task()
def load_fafnir_pois(ctx, files=[]):
    poi_conf = ctx.get("poi")
    if not _is_config_object(poi_conf):
        logging.info("no poi to import")
        return

    fafnir_conf = poi_conf.get("fafnir")
    if not _is_config_object(fafnir_conf):
        return
    logging.info("fafnir {}".format(fafnir_conf))

    if fafnir_conf.get("load_db") is True:
        # TODO import data in PG
        logging.warn("for the moment we can't load data in postgres for fafnir")

    nb_threads_conf = fafnir_conf.get("nb_threads")
    nb_threads = "--nb-threads {}".format(nb_threads_conf) if nb_threads_conf else ""

    logging.info("importing poi with fafnir")
    run_rust_binary(
        ctx,
        "",
        "fafnir",
        files,
        "--es {ctx.es} \
        {nb_threads} \
        --dataset {ctx.dataset}\
        --pg {pg}".format(
            ctx=ctx, pg=fafnir_conf["pg"], nb_threads=nb_threads
        ),
    )


def _use_cosmogony(ctx):
    admin_conf = ctx.get("admin")
    return _is_config_object(admin_conf) and "cosmogony" in admin_conf


def _is_config_object(obj):
    return obj is not None and isinstance(obj, (DataProxy, dict))


@task(default=True)
def load_all(ctx, files=[]):
    """
    default task called if `invoke` is run without args
    This is the main tasks that import all the datas into mimir
    """
    if _use_cosmogony(ctx):
        logging.info("using cosmogony")
        if not ctx.admin.cosmogony.get("file"):
            generate_cosmogony(ctx, files)
        load_cosmogony(ctx, files)

    load_addresses(ctx, files)

    load_osm(ctx, files)

    load_fafnir_pois(ctx, files)


@task(iterable=["files"])
def compose_up(ctx, files=[]):
    """
    pop all the necessary dockers for mimir

    you can specify additional docker-compose file to the command with the --files parameters
    """
    logging.info("running in docker-compose mode")
    ctx.run_on_docker_compose = True
    files_args = _build_docker_files_args(files)

    ctx.run("docker-compose {files} pull".format(files=files_args))
    ctx.run("docker-compose {files} up -d --build".format(files=files_args))

    _wait_for_es(ctx, files)


@task(iterable=["files"])
def compose_down(ctx, files=[]):
    files_args = _build_docker_files_args(files)

    ctx.run("docker-compose {files} stop".format(files=files_args))


@task(iterable=["files"])
def test(ctx, files=None):
    """
    Run some tests on mimir with geocoder tester.

    The docker-compose must have been set up before running this command

    you can specify additional docker-compose file to the command with the --files parameters

    The tests results are written in the ./result directory (defined in tester_docker-compose.yml)
    """
    if not ctx.run_on_docker_compose:
        raise Exception("geocoder-tester can run only in docker-compose mode")
    logging.info("running geocoder-tester")

    # we update the images in tester_docker-compose
    ctx.run("docker-compose -f tester_docker-compose.yml pull")
    ctx.run("docker-compose -f tester_docker-compose.yml build")
    files_args = _build_docker_files_args(["tester_docker-compose.yml"] + files)

    ctx.run("docker-compose {files} run --rm geocoder-tester-runner".format(files=files_args))


@retry(stop_max_delay=60000, wait_fixed=100)
def _wait_for_es(ctx, files):
    logging.info("waiting for es")
    logging.info("waiting for es {}".format(ctx.es))
    files_args = _build_docker_files_args(["tester_docker-compose.yml"] + files)
    ctx.run(
        "docker-compose {files_args} run --rm pinger {url}".format(
            files_args=files_args, url=ctx.es
        )
    )


@task(iterable=["files"])
def load_in_docker_and_test(ctx, files=[]):
    compose_up(ctx, files)
    load_all(ctx, files)
    test(ctx, files)
    compose_down(ctx, files)


def _build_docker_files_args(files):
    compose_files = ["docker-compose.yml"] + files
    return "".join([" -f {}".format(f) for f in compose_files])
