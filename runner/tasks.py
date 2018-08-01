from invoke import task
import os
import datetime
import re
import logging
import requests
import csv


logging.basicConfig(level=logging.INFO)


def _get_remaining_tests(ctx):
    selectors = [c["selector"] for c in ctx.categories if "selector" in c]

    return " and ".join([f"not {s}" for s in selectors])


def _init_output_dir(ctx, test_name):
    if "output_dir" not in ctx:
        dt = datetime.datetime.now().isoformat()
        dir_name = f"{ctx.base_output_dir}/{test_name}_{dt}"
        os.makedirs(dir_name)
        ctx.output_dir = dir_name
    return ctx.output_dir


def _pretty_print(dicts, keys):
    """
    pretty print an array of dict as a table

    shamelessly taken and adapted from geocoder-tester
    """
    if not dicts:
        return []
    # Compute max length for each column.
    lengths = {}
    for key in keys:
        lengths[key] = len(key) + 2  # Surrounding spaces.
    for d in dicts:
        for key in keys:
            i = len(str(d.get(key, "")))
            if i > lengths[key]:
                lengths[key] = i + 2  # Surrounding spaces.
    out = [""]
    cell = "{{{key}:^{length}}}"
    tpl = "|".join(cell.format(key=key, length=lengths[key]) for key in keys)
    # Headers.
    out.append(tpl.format(**dict(zip(keys, keys))))
    # Separators line.
    out.append(tpl.format(**dict(zip(keys, ["—" * lengths[k] for k in keys]))))
    for d in dicts:
        row = {}
        l = lengths.copy()
        for key in keys:
            value = d.get(key) or "_"
            row[key] = value
        # Recompute tpl with lengths adapted to failed rows (and thus ansi
        # extra chars).
        tpl = "|".join(cell.format(key=key, length=l[key]) for key in keys)
        out.append(tpl.format(**row))
    return out


RESULT_LINE_PATTERN = re.compile("===+.*===+")

RESULT_PATTERN = re.compile(
    "===+ (?P<failed>\d+) failed, (?P<success>\d+) passed.*in (?P<time>.*) seconds.*"
)


def _safe_cast(val, to_type):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return None


def _get_results(region, category, pytest_logs):
    res = {"region": region, "category": category}
    for l in reversed(pytest_logs.split("\n")):
        if not RESULT_LINE_PATTERN.match(l):
            continue
        logging.info(l)
        match = RESULT_PATTERN.match(l)
        if match:
            failed = _safe_cast(match.group("failed"), int)
            success = _safe_cast(match.group("success"), int)
            time = _safe_cast(match.group("time"), float)
            duration = datetime.timedelta(seconds=time) if time else None
            total = failed + success
            ratio = success / total
            res.update(
                {
                    "failed": failed,
                    "total": total,
                    "duration": str(duration),
                    "ratio": f"{ratio:.0%}",
                }
            )
            return res
        logging.error(f"impossible to parse results: {l}")
        return res
    return res


@task
def run_pytest(ctx, url, name, region, category):
    _init_output_dir(ctx, name)
    directory = f"{ctx.geocoder_sources}/geocoder_tester/world/{region}"
    category_name = category["name"]

    if category.get("remaining_tests"):
        selector = _get_remaining_tests(ctx)
    else:
        selector = category["selector"]

    additional_args = " ".join(ctx.additional_pytest_args)
    test_name = f"{region}_{category_name}"
    report_file = os.path.join(ctx.output_dir, f"{test_name}_report.txt")
    py_test = " ".join(
        [
            f"pytest {directory}",
            f"--api-url {url}",
            f'-k "{selector}"',
            "--loose-compare",
            f"--save-report={report_file}",
            f"--tb=short {additional_args}",
        ]
    )

    logging.info(f"runnning {py_test}")
    log_file = os.path.join(ctx.output_dir, f"{test_name}.log")
    logging.info(f"runnning {log_file} ----- {os.path.dirname(log_file)}")
    if not os.path.exists(os.path.dirname(log_file)):
        # we create the parent dir if needed
        os.makedirs(os.path.dirname(log_file))

    with open(log_file, "w") as log_file:
        res = ctx.run(py_test, out_stream=log_file, warn=True)

        # print(f'res: {res.stdout}')
        res = _get_results(region, category_name, res.stdout)
        logging.info(f"result = {res}")
        return res


REPORT_COLUMN = ["region", "category", "failed", "total", "ratio", "duration"]


def _get_version(url):
    """
    uggly hack to get the version of mimir used
    if works only for mimir (but addock does not expose a /status endpoint)
    """
    if "/autocomplete" not in url:
        return None

    status_url = url.replace("/autocomplete", "/status")
    status_resp = requests.get(status_url)
    return status_resp.json().get("version")


@task(default=True)
def run_all(ctx, url=None, name="geocoder-tester"):
    _init_output_dir(ctx, name)
    url = url or ctx.url
    if not url:
        raise Exception("no url provided")

    logging.info(f'testing {name} on {url}')

    res = []

    version = _get_version(url)
    if version:
        logging.info(f"testing version {version}")
    for region in ctx.regions:
        for category in ctx.categories:
            logging.info(f"running tests on {region} / {category}")
            r = run_pytest(ctx, url, name, region, category)
            res.append(r)

    report = "\n".join(_pretty_print(res, REPORT_COLUMN))
    logging.info(report)

    report_file = os.path.join(ctx.output_dir, f"report.log")
    with open(report_file, "w") as log_file:
        log_file.writelines(f"report on '{name}'\n")
        log_file.writelines(f"queries make on {url} | version = {version} \n")
        log_file.writelines(report)

    # print also a csv to better compare the results
    csv_file = os.path.join(ctx.output_dir, f"report.csv")
    csv_column = REPORT_COLUMN + ["directory", "url", "name", "version"]
    with open(csv_file, "w") as csv_file:
        w = csv.DictWriter(csv_file, csv_column)
        w.writeheader()
        for r in res:
            r.update(
                {
                    "directory": ctx.output_dir,
                    "url": url,
                    "name": name,
                    "version": version,
                }
            )
            w.writerow(r)
