# import pipeline into mimir

The pipeline is based on [invoke](https://github.com/pyinvoke/invoke).

Invoke can be configured from various way, the easiest would be a create a custom configuration file.

The default configuration file ins `invoke.yaml`.
All possible variables are commented in this file.

To run the pipeline you need [pipenv](https://github.com/pypa/pipenv)

## running 'bare metal'

To run it with a custom configuration file:
`pipenv run inv -f my_settings.yaml`

Note:
All binaries executed need to be in the PATH.
The easiest way to achieve this is to export the path before:

`PATH=$PATH:<path_to_cosmogony>/target/release:<path_to_mimir>/target/release:<path_to_fafnir>/target/release pipenv run inv`

## running with docker-compose

You can also use docker-compose to run mimir.

You need to at least set the environment variable `OSM_DIR` (see below the mean to set it).

To configure some stuff, some environment variable can be set (in cli or in a `.env` file in the directory).

The env variables that can be set are:

```env
OSM_DIR=<path to osm data dir>
ADDR_DIR=<path to addresses data dir>
COSMOGONY_DIR=<path to cosmogony data dir>
```

Note: you don't need to set the variable that are not used in your configuration file.

Once the variables are set, to run mimir you need to do:

`pipenv run inv -f docker_settings.yaml load-in-docker-and-test`

This will:

* run a docker-compose up
* import some data in mimir
* run geocoder-tester in it and write results in a local directory

If you don't want to run the tests you can also use invoke chaining calls:

`pipenv run inv -f docker_settings.yaml compose-up load-all compose-down`

Some other docker-compose files can also be given (this will use [the docker compose override mechanism](https://docs.docker.com/compose/extends/#different-environments)). It will for example makes it possible to use customly build image to run tests on a given mimir (or fafnir, cosmogony, ...) branch.

The file pathes are given with the `--files` arguments.

like:

`pipenv run inv -f docker_settings.yaml load-in-docker-and-test --files my-docker-compose.yml --files my-other-compose.yml`
