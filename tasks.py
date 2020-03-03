import os
from invoke import task
from invoke.config import DataProxy
import logging
from retrying import retry
from datetime import timedelta
from time import time

logging.basicConfig(level=logging.INFO)


def run_rust_binary(ctx, container, bin, files, params):
    cmd = "{} {}".format(bin, params)
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
def download(ctx, files=[]):
    files_args = _build_docker_files_args(files)

    if ctx.get("osm", {}).get("url"):
        file_name = os.path.basename(ctx.osm.url)
        ctx.osm.file = os.path.join("/data/osm", file_name)
        ctx.run(
            "docker-compose {files} run --rm download"
            " download-osm --osm-url={osm_url} --output-file={output_file}".format(
                files=files_args, osm_url=ctx.osm.url, output_file=ctx.osm.file
            )
        )
    if ctx.addresses.get("bano", {}).get("url"):
        ctx.addresses.bano.file = "/data/addresses/bano.csv"
        ctx.run(
            "docker-compose {files} run --rm download"
            " download-bano --bano-url={bano_url} --output-file={output_file}".format(
                files=files_args,
                bano_url=ctx.addresses.bano.url,
                output_file=ctx.addresses.bano.file,
            )
        )
    if ctx.addresses.get("oa", {}).get("url") and ctx.addresses.get("oa", {}).get("include"):
        ctx.addresses.oa.path = "/data/addresses/oa"
        ctx.run(
            "docker-compose {files} run --rm download"
            " download-oa --oa-url={oa_url} --output-dir={output_dir} --oa-filter={oa_filter}".format(
                files=files_args,
                oa_url=ctx.addresses.oa.url,
                output_dir=ctx.addresses.oa.path,
                oa_filter=",".join(ctx.addresses.oa.include),
            )
        )


@task()
def generate_cosmogony(ctx, files=[]):
    logging.info("generating cosmogony file")

    additional_params = ""
    if ctx.admin.cosmogony.get("langs", ""):
        langs_codes = ctx.admin.cosmogony.langs.split(",")
        for code in langs_codes:
            additional_params += _get_cli_param(code, "--filter-langs")

    if ctx.admin.cosmogony.get("disable_voronoi"):
        additional_params += " --disable-voronoi"

    with ctx.cd("."):
        cosmogony_file = "{ctx.admin.cosmogony.output_dir}/cosmogony.jsonl.gz".format(
            ctx=ctx
        )
        run_rust_binary(
            ctx,
            "cosmogony",
            "",
            files,
            "--input {ctx.osm.file} \
            {additional_params} \
            --output {cosmogony_file}".format(
                ctx=ctx,
                cosmogony_file=cosmogony_file,
                additional_params=additional_params,
            ),
        )
        ctx.admin.cosmogony.file = cosmogony_file


@task()
def load_cosmogony(ctx, files=[]):
    logging.info("loading cosmogony")
    conf = ctx.admin.cosmogony
    additional_params = _get_cli_param(conf.get("nb_shards"), "--nb-shards")
    additional_params += _get_cli_param(conf.get("nb_replicas"), "--nb-replicas")

    langs_params = ""
    if ctx.admin.cosmogony.get("langs", ""):
        langs_codes = ctx.admin.cosmogony.langs.split(",")
        for code in langs_codes:
            langs_params += _get_cli_param(code, "--lang")

    run_rust_binary(
        ctx,
        "mimir",
        "cosmogony2mimir",
        files,
        "--input {ctx.admin.cosmogony.file} \
        {langs_params} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset} \
        {additional_params} \
        ".format(
            ctx=ctx, langs_params=langs_params, additional_params=additional_params
        ),
    )


def _get_cli_param(conf_value, cli_param_name):
    if conf_value is not None and conf_value != "":
        return ' {param}="{value}"'.format(param=cli_param_name, value=conf_value)
    return ""


@task()
def load_osm_admins(ctx, files=[]):
    logging.info("importing admins from osm")
    osm_param = ctx.admin.get("osm")
    if not osm_param:
        return
    args = "--import-admin"
    if _is_config_object(osm_param):
        for lvl in osm_param["levels"]:
            args += " --level {}".format(lvl)
        args += _get_cli_param(osm_param.get("nb_shards"), "--nb-admin-shards")
        args += _get_cli_param(osm_param.get("nb_replicas"), "--nb-admin-replicas")

    run_rust_binary(
        ctx,
        "mimir",
        "osm2mimir",
        files,
        "--input {ctx.osm.file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}\
        {args} \
        ".format(
            ctx=ctx, args=args
        ),
    )


@task()
def load_osm_pois(ctx, files=[]):
    logging.info("importing poi from osm")
    poi_conf = ctx.get("poi", {}).get("osm")
    poi_args = "--import-poi"
    if poi_conf:
        poi_args += _get_cli_param(poi_conf.get("nb_shards"), "--nb-poi-shards")
        poi_args += _get_cli_param(poi_conf.get("nb_replicas"), "--nb-poi-replicas")

    run_rust_binary(
        ctx,
        "mimir",
        "osm2mimir",
        files,
        "--input {ctx.osm.file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset}\
        {poi_args} \
        ".format(
            ctx=ctx, poi_args=poi_args
        ),
    )


@task()
def load_osm_streets(ctx, files=[]):
    logging.info("importing data from osm")

    street_conf = ""
    street_conf += _get_cli_param(
        ctx.get("street", {}).get("nb_shards"), "--nb-street-shards"
    )
    street_conf += _get_cli_param(
        ctx.get("street", {}).get("nb_replicas"), "--nb-street-replicas"
    )

    street_conf += _get_cli_param(ctx.get("street", {}).get("osm_db_file"), "--db-file")

    run_rust_binary(
        ctx,
        "mimir",
        "osm2mimir",
        files,
        "--input {ctx.osm.file} \
        --connection-string {ctx.es} \
        --dataset {ctx.dataset} \
        --import-way \
        {street_conf} \
        ".format(
            ctx=ctx, street_conf=street_conf
        ),
    )


@task()
def load_addresses(ctx, files=[]):
    if not _is_config_object(ctx.get("addresses")):
        logging.info("no addresses to import")
        return

    if not ctx.get("new-importer", False):
        addr_config = ctx.get("addresses")
        if addr_config.get("bano", {}).get("file"):
            logging.info("importing bano addresses")
            conf = ctx.addresses.bano
            additional_params = _get_cli_param(conf.get("nb_threads"), "--nb-threads")
            additional_params += _get_cli_param(conf.get("nb_shards"), "--nb-shards")
            additional_params += _get_cli_param(conf.get("nb_replicas"), "--nb-replicas")
            run_rust_binary(
                ctx,
                "mimir",
                "bano2mimir",
                files,
                "--input {ctx.addresses.bano.file} \
                --connection-string {ctx.es} \
                {additional_params} \
                --dataset {ctx.dataset}".format(
                    ctx=ctx, additional_params=additional_params
                ),
            )
        if addr_config.get("oa", {}).get("path"):
            logging.info("importing oa addresses")
            conf = ctx.addresses.oa
            additional_params = _get_cli_param(conf.get("nb_threads"), "--nb-threads")
            additional_params += _get_cli_param(conf.get("nb_shards"), "--nb-shards")
            additional_params += _get_cli_param(conf.get("nb_replicas"), "--nb-replicas")
            run_rust_binary(
                ctx,
                "mimir",
                "openaddresses2mimir",
                files,
                "--input {ctx.addresses.oa.path} \
                --connection-string {ctx.es} \
                {additional_params} \
                --dataset {ctx.dataset}".format(
                    ctx=ctx, additional_params=additional_params
                ),
            )
        return

    output_csv = "output.csv.gz"

    options = []
    if ctx.addresses.get("bano", {}).get("file"):
        options.append("--bano")
        options.append(ctx.addresses.bano.file)
    if ctx.addresses.get("oa", {}).get("file"):
        options.append("--openaddresses")
        options.append(ctx.addresses.oa.file)
    if ctx.get("osm", {}).get("file"):
        options.append("--osm")
        options.append(ctx.osm.file)
    if len(options) == 0:
        return
    options.append("--output_compressed_csv")
    options.append("/addr/{}".format(output_csv))

    logging.info("Running addresses importer/deduplicator")

    run_rust_binary(ctx, "addresses-importer", "deduplicator", options)

    logging.info("Import addresses into ES instance")

    run_rust_binary(
        ctx,
        "mimir",
        "openaddresses2mimir",
        files,
        "--input {output_csv} \
        --connection-string {ctx.es} \
        {additional_params} \
        --dataset {ctx.dataset}".format(
            ctx=ctx, additional_params=additional_params
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

    langs_params = ""
    if fafnir_conf.get("langs", ""):
        langs_codes = fafnir_conf.get("langs").split(",")
        for code in langs_codes:
            langs_params += _get_cli_param(code, "--lang")

    additional_params = _get_cli_param(fafnir_conf.get("nb_threads"), "--nb-threads")
    additional_params += _get_cli_param(
        fafnir_conf.get("bounding-box"), "--bounding-box"
    )
    additional_params += _get_cli_param(fafnir_conf.get("nb_shards"), "--nb-shards")
    additional_params += _get_cli_param(fafnir_conf.get("nb_replicas"), "--nb-replicas")

    logging.info("importing poi with fafnir")
    run_rust_binary(
        ctx,
        "",
        "fafnir",
        files,
        "--es {ctx.es} \
        {langs_params} \
        {additional_params} \
        --dataset {ctx.dataset}\
        --pg {pg}".format(
            ctx=ctx,
            pg=fafnir_conf["pg"],
            langs_params=langs_params,
            additional_params=additional_params,
        ),
    )


def _use_cosmogony(ctx):
    admin_conf = ctx.get("admin")
    return _is_config_object(admin_conf) and "cosmogony" in admin_conf


def _is_config_object(obj):
    return obj is not None and isinstance(obj, (DataProxy, dict))


def load_admins(ctx, files):
    if _use_cosmogony(ctx):
        logging.info("using cosmogony")
        if not ctx.admin.cosmogony.get("file"):
            generate_cosmogony(ctx, files)
        load_cosmogony(ctx, files)
    else:
        load_osm_admins(ctx, files)


def load_pois(ctx, files):
    poi_conf = ctx.get("poi")
    if not _is_config_object(poi_conf):
        logging.info("no poi to import")
        return

    if "fafnir" in poi_conf:
        load_fafnir_pois(ctx, files)
    elif "osm" in poi_conf:
        load_osm_pois(ctx, files)


@task(default=True)
def load_all(ctx, files=[]):
    """
    default task called if `invoke` is run without args
    This is the main tasks that import all the datas into mimir
    """
    download(ctx, files)

    load_admins(ctx, files)

    load_addresses(ctx, files)

    load_osm_streets(ctx, files)

    load_pois(ctx, files)


@task(iterable=["files"])
def compose_up(ctx, files=[]):
    """
    pop all the necessary dockers for mimir

    you can specify additional docker-compose file to the command with the --files parameters
    """
    logging.info("running in docker-compose mode")
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
    logging.info("running geocoder-tester")

    # we update the images in tester_docker-compose
    ctx.run("docker-compose -f tester_docker-compose.yml pull")
    ctx.run("docker-compose -f tester_docker-compose.yml build")
    files_args = _build_docker_files_args(["tester_docker-compose.yml"] + files)

    region = ctx.get("geocoder_tester_region")
    if region:
        additional_args = "run-all --regions {}".format(region)
    else:
        additional_args = ""
    ctx.run(
        "docker-compose {files} run --rm geocoder-tester-runner {args}".format(
            files=files_args, args=additional_args
        )
    )


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
