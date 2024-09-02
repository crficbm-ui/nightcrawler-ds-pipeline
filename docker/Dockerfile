# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:4-python3.11-appservice
FROM mcr.microsoft.com/azure-functions/python:4-python3.10

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=0 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

COPY docker/requirements.txt /
RUN pip install -r /requirements.txt

ADD . /usr/local/nightcrawler

WORKDIR /usr/local/nightcrawler
RUN poetry install && rm -rf $POETRY_CACHE_DIR

COPY docker/ /home/site/wwwroot
