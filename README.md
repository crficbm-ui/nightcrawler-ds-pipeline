# Nightcrawler Datascience Pipeline Repository

This repo provides the Nightcrawler pipeline as well as a CLI to run it.

## Setting the CLI up in your local environment

1. Pull the latest changes from the git repository.

```bash
git pull
```

2. In the [pyproject.tml](./pyproject.toml) on line 14 specify the path to `nigthcralwer-ds-helpers` directory on your machine (if you need to develop in that repository) or use a tagged version from GitHub.

```bash
helpers = { path = "../nightcrawler-ds-helpers/", develop = true }  #for using a local version of nigthcrawler-ds-helpers
helpers = {git = "https://github.com/smc40/nightcrawler-ds-helpers", tag = "v0.0.0"} #for using a tagged version from GitHub
```

3. Create a virtual environment with Poetry and activate it.

```bash
poetry shell
```

> **_NOTE:_**  Run the following command to install Poetry if it was not installed:
>```sh
>curl -sSL https://install.python-poetry.org | python3 -
>```

4. Install the project dependencies.

```bash
poetry install --directory ./pyproject.toml
```

5. Copy `.env_template` to `.env`. Fill with your credentials and source it by running `source .env`.

## Basic CLI usage
First, activate the venv inside the `nightcrawler` directory:

```
poetry shell
```

Then from the root directory you can use the `nightcrawler cli` as follows:
```
python -m nightcrawler <processing_step> <param_1> <param_n> 

```

You can always use the `-h` to see all available options for the cli.


### Processing steps
The processing steps are one of the following:

- extract -> getting data from serpapi, diffbot, zyte
- process -> merge all sources into one single file
- ...


### Configuration
Whatever configuration is needed, should be added in the `context.py` file.

### Logging
The default logging level is set to `INFO`, and by default, logs are not stored in a file but are output to the console. 
If you want to change the default behavior, you can use the following command-line options:

- **Change the log level**: Use the `--log-level` option to set the desired log level. Available levels include `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`. For example:
  ```bash
  python -m nightcrawler extract serpapi aspirin --log-level DEBUG
  ```
- **Log to a file**: Use the --log-file option to specify a file where the logs should be stored. For example:
  ```bash
  python -m nightcrawler extract serpapi aspirin --log-file logs/output.log
  ```


> **_NOTE:_**  For simplicity, the full CLI documentation can be found on [Confluence](https://swissmedic.atlassian.net/wiki/spaces/N/pages/7475365463/CLI).


## [Code] decision log (temp and too be deleted or documented elsewhere)

1. argparse vs click -> we go with argparse
2. how do we bring code that was writen outside of the 'nightcralwer' dir (i.e. helpers) into nc?
    - we will use the 'helpers' dir and make sure that whenever a change is done in that dir, it does not affect the prod. code (PR to Nico / Alex)
3. Reusability of MediCrawl code
    - "steal with pride"

