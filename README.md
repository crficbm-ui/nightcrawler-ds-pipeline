# Nightcrawler Datascience Pipeline Repository

This repo is provides the Nightcrawler pipeline as well as a CLI to run it.

## Simple start

1. Pull the latest changes from the git repository.

```bash
git pull
```

2. Initiate and install the project dependencies.

```bash
poetry install --directory ./pyproject.toml
```

## Basic CLI usage
First, activat the venv inside the `nightcrawler` directory:

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


> **_NOTE:_**  For simplicity, the full CLI documentation can be found on [Confluence](https://swissmedic.atlassian.net/wiki/spaces/N/pages/7475365463/CLI).


## Discussions with Nico (temp)

### [Code] decision log

1. argparse vs click -> we go with argparse
2. how do we bring code that was writen outside of the 'nightcralwer' dir (i.e. helpers) into nc?
    - we will use the 'helpers' dir and make sure that whenever a change is done in that dir, it does not affect the prod. code (PR to Nico / Alex)
3. Reusability of MediCrawl code
    - "steal with pride"


### Next steps
1. Review CLI if okay in general
2. Write tests with HTML from blob storage.



for helpers
1. copy the helpers from medicrawl-master
2. in repo `git clone helpers`
3. 




# Python Virtual Environment


## Step 1: Install Pyenv (Python Version Management)

Run the following command to install Pyenv:
```bash
curl https://pyenv.run | bash
```

Install the necessary dependencies:
```sh
sudo apt update
sudo apt install build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev curl git libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
```

Install the desired Python version using Pyenv:
```sh
pyenv install 3.10
```

## Step 2: Configure Shell Initialization Files

Create a local Zsh configuration file ~/.local_zshrc and insert the following code:
```sh
export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
export PATH="/home/bkopin/.local/bin:$PATH"
echo "Local zshrc loaded!"
```

Update your ~/.zshrc file to source the local Zsh configuration file by adding:
```sh
source ~/.local_zshrc
```

Source the ~/.local_zshrc file to apply the changes:
```sh
source ~/.local_zshrc
```

Hint: The similar steps should be done for the Bash shell. Just replace the ~/.zshrc file with the ~/.bashrc file.

## Step 3: Install Poetry (Python Dependency Management)

Run the following command to install Poetry:
```sh
curl -sSL https://install.python-poetry.org | python3 -
```

## Step 4: Create and Activate a Virtual Environment

Create a virtual environment using Pyenv:
```sh
pyenv virtualenv 3.10 env_nightcrawler
```

Navigate to your project directory:
```sh
cd IN_YOUR_PROJECT
```

Set the local Python version to use the newly created virtual environment every time you navigate to the project directory:
```sh
pyenv local env_nightcrawler
```

## Step 5: Install Dependencies

Install the project dependencies using Poetry:
```sh
poetry install
```
