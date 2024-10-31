# To enable ssh & remote debugging on app service change the base image to the one below
#FROM mcr.microsoft.com/azure-functions/python:4-python3.10-appservice
# For production mode
FROM mcr.microsoft.com/azure-functions/python:4-python3.10

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=0 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

ADD poetry.lock pyproject.toml README.md /usr/local/nightcrawler/
ADD nightcrawler/ /usr/local/nightcrawler/nightcrawler/
ADD nightcrawler-library /usr/local/nightcrawler/nightcrawler-library/

WORKDIR /usr/local/nightcrawler
RUN apt-get update && apt-get install -y libpq-dev build-essential
RUN pip install --upgrade pip && pip install poetry==1.8.3 && poetry install && rm -rf $POETRY_CACHE_DIR

COPY docker/requirements.txt /
RUN pip install -r /requirements.txt

COPY docker/ /home/site/wwwroot
