import os

DEFAULT_VERSION = "0.0.0"
__version__ = os.getenv("CLI_VERSION", DEFAULT_VERSION)
# TODO inject github tag to env
