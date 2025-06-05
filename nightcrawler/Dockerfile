# ----------------------------------------------------------------------------
# NOTICE THIS DOCKERFILE HAS BEEN EXPLICITLY CREATED FOR THE CHILE ISP PROJECT
# ----------------------------------------------------------------------------

# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update -y && apt-get install -y \
    python3.10 \
    git \
    curl \
    libpq-dev \
    build-essential \
    unrar \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3.10 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Clone the repository
RUN git clone --branch deployment-chile-isp https://github.com/smc40/nightcrawler-ds-pipeline.git .

# Set environment variables
ENV CLI_VERSION="0.0.5"
ENV SERP_API_TOKEN="..."
ENV SMARTPROXY_USERNAME="1234"
ENV SMARTPROXY_PASSWORD="12312"
ENV SMARTPROXY_PORT="91022"
ENV ZYTE_API_TOKEN="..."
ENV DATAFORSEO_USERNAME="..."
ENV DATAFORSEO_PASSWORD="..."
ENV MISTRAL_API_KEY="1234"
ENV CONTENT_DOMAIN_ENDPOINT="123"
ENV CONTENT_DOMAIN_USERNAME="123"
ENV CONTENT_DOMAIN_PASSWORD="123"
ENV CORRUPTED_CONTENT_ENDPOINT="123"
ENV CORRUPTED_CONTENT_USERNAME="123"
ENV CORRUPTED_CONTENT_PASSWORD="123"

# Configure Poetry to use Python 3.10
RUN poetry env use /usr/local/bin/python3.10

# Install dependencies
RUN poetry lock && poetry install

# Add transformers
RUN poetry add transformers

# Use a shell to keep the container running
SHELL ["/bin/bash", "-c"]

# Copy the model folder into the nightcrawler directory
COPY model /app/nightcrawler/model

# Command to run the script
#CMD ["poetry", "run", "python", "-m", "nightcrawler", "fullrun", "desfibralador", "-n=10", "--unit=ISP"] 