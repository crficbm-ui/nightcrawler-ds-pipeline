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
helpers = {git = "https://github.com/smc40/nightcrawler-ds-helpers", tag = "v0.0.2"} #for using a tagged version from GitHub
```

> **_NOTE:_**  As of today, 23.08.2025 the current and tested helpers tag is v0.1.2. When updating the tag in the pyproject.toml, you need to delete the poetry.lock file.

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

```bash
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

### Full Pipeline Run
To perform a full pipeline run end-to-end (and what ultimatelly will be deploey via Azure functions), run:

```bash
python -m nightcrawler fullrun viagra -n=3 --country=CH
```

### Extraction
To run the full extraction pipeline you can use any of the following commands:
```bash
python -m nightcrawler extract aspirin #full extraction with keyword 'aspirin'
python -m nightcrawler extract aspirin -n=3 #full extraction with keyword 'aspirin' for the first 3 entries

```
Running the extraction pipeline will log the results in the terminal (with default logging which is `--log-level INFO`) and store the scraped content into `./data/output/<extraction_step>_<timespamp>_<user>.json`.


If you prefer, you can run the pipeline for a single extraction step:
1. Collect only the URLs from serpapi: 
```bash
python -m nightcrawler extract triofan -n=3 serpapi #collect only the 3 first URLs from serpapi for the keyword triofan
```

2. Collect only the parsed results from zyte (line 2 below):

```bash
python -m nightcrawler extract triofan -n=3 zyte <url_path>-- #collect the parsed results for keyword triofan from zyte. the url path should reference to the results of the previous step, typically in ./data/output/<timestamp>_<keyword>_<user>
```
### Processing

```bash
python -m nightcrawler process country CH zyte_urls_2026-09-08_12-23-10 --log-level DEBUG
```



## Development settings
### Configuration
The  [**settings**](nightcrawler/settings.py) component is designed to store all variables that are not tied to specific commands and do not change throughout the runtime of the CLI. On the other hand, [**context**](nightcrawler/context.py) is intended to include variables that originate from the command line, as well as any helpers or long-lived objects (e.g., database connections). While it’s possible to consolidate everything into the settings, having a separate Context object can be beneficial as the codebase expands. This distinction helps maintain organization and scalability as the project grows.

> **_NOTE:_**  The same two components exist in the [helper repository](https://github.com/smc40/nightcrawler-ds-helpers), so be careful when importing.


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



### Linting and Formatting
Based on Thomas recommendation we will be using [ruff](https://docs.astral.sh/ruff/) as linting and formatting tool.
For linting preview run:
```bash
ruff check
```
For linting run:
```bash
ruff check --fix
```

Ruff also provides a formatting tool that should be run prior commiting changes:
```bash
ruff format
``` 


## Git Tag History
So far, no tags have been created (alho, 12.08.2024).