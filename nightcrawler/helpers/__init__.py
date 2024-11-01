import os
import logging
from dotenv import load_dotenv

load_dotenv()

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOGGER_NAME = "nightcrawler_logger"
logger = logging.getLogger(LOGGER_NAME)
logger.propagate = False  # prevent propagation to root logging handler witch caused that the logs were doubles on level "nightcrawler_logger" and on level "root"
logger.setLevel(logging.INFO)

# Check if the logger already has handlers to avoid duplicate logs
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

CACHE_DIR = os.getenv("CACHE_DIR", os.path.join(ROOT_PATH, ".cache"))
if not CACHE_DIR:
    CACHE_DIR = os.path.join(ROOT_PATH, ".cache")

os.makedirs(CACHE_DIR, exist_ok=True)
