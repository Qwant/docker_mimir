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