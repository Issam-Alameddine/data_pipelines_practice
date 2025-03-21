import logging.config
import yaml
import os


LOGGING_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "logging.yaml")

with open(LOGGING_CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)