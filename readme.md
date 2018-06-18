# import pipeline into mimir

The pipeline is based on [invoke](https://github.com/pyinvoke/invoke).

Invoke can be configured from various way, the easiest would be a create a custom configuration file.

The default configuration file ins `invoke.yaml`.
All possible variables are commented in this file.

To run the pipeline you need [pipenv](https://github.com/pypa/pipenv)

To run it with a custom configuration file:
`pipenv run inv -f my_settings.yaml`

Note:
For the moment all binary executed needs to be in the PATH.
The easiest way to acheive this is to export the path before:
`PATH=$PATH:<path_to_cosmogony>/target/release:<path_to_mimir>/target/release:<path_to_fafnir>/target/release pipenv run inv`

This is not convenient, but I don't think we'll run those command as is. They will likely be wrapped in some container, and I don't yet know how we'll do this.

## running with docker-compose

You can use docker-compose to run mimir.

The easiest way to do this is:

`pipenv run inv -f docker_settings.yaml load-in-docker-and-test`

This will:

* run a docker-compose up
* import some data in mimir
* run geocoder-tester in it and write results in a local directory

To configure some stuff, some enviroment can be set in a `.env` file in the directory.

The env variables that can be set are:

```env
OSM_DIR=<path to osm data dir>
ADDR_DIR=<path to addresses data dir>
COSMOGONY_DIR=<path to cosmogony data dir>
```

Some other docker-compose files can also be given (this will use [the docker compose override mechanism](https://docs.docker.com/compose/extends/#different-environments)). It will for example makes it possible to use customly build image to run tests on a given mimir (or fafnir, cosmogony, ...) branch.

The file pathes are given with the `--files` arguments.

like:

`pipenv run inv -f docker_settings.yaml load-in-docker-and-test --files my-docker-compose.yml --files my-other-compose.yml`
