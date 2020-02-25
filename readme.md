[![Python 3.5](https://img.shields.io/badge/python-3.5-blue.svg)](https://www.python.org/downloads/release/python-350/)
![pipenv](https://img.shields.io/badge/pipenv->%3D%209.0.1-brightgreen.svg)

# docker_mimir: Default [Mimir](https://github.com/CanalTP/mimirsbrunn) Import Tool

- This tool is the easiest way to import data for the [Mimir](https://github.com/CanalTP/mimirsbrunn) geocoding system.
- Basically `docker_mimir` is an import pipeline based on [invoke](https://github.com/pyinvoke/invoke). The easiest way to configure `invoke` is to create a custom configuration file. All possible variables are commented in the default configuration file: `invoke.yaml`.
- Running `docker_mimir` requires [pipenv](https://github.com/pypa/pipenv).

## How to use it

To configure the required data directories, some environment variable can be set (in cli or in a `.env` file in the directory). The env variables that can be set as follows:

```env
OSM_DIR=<path to osm data dir>
ADDR_DIR=<path to addresses data dir>
COSMOGONY_DIR=<path to cosmogony data dir>
```

- Note that you don't need to set the variable that are not used in the configuration file.
- Once the variables are set, to run mimir you need to do:

```shell
pipenv run inv -f docker_settings.yaml load-in-docker-and-test
```

- The file `docker_settings.yaml` contains an example configuration to download and import data about Luxembourg.
- This will:
  * run a `docker-compose up`.
  * import some data in [Mimir](https://github.com/CanalTP/mimirsbrunn).
  * run [geocoder-tester](https://github.com/QwantResearch/geocoder-tester) and write results in a local directory.

#### About Tests

- If you don't want to run the tests you can also use `invoke` chaining calls:
```
pipenv run inv -f docker_settings.yaml compose-up load-all compose-down
```
- Some other `docker-compose` files can also be given (this will use [the docker compose override mechanism](https://docs.docker.com/compose/extends/#different-environments)). It will for example make it possible to use customly build image to run tests on a given Mimir (or [Fafnir](https://github.com/QwantResearch/fafnir), [Cosmogony](https://github.com/osm-without-borders/cosmogony), ...) branch.

- The file paths are given with the `--files` arguments, as follows:
```
pipenv run inv -f docker_settings.yaml load-in-docker-and-test --files my-docker-compose.yml --files my-other-compose.yml
```
