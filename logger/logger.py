# Import the required libraries
import logging
from logging.handlers import RotatingFileHandler


def logs(__name__):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set the logging level for this logger

    # Create a rotating file handler
    handler = RotatingFileHandler('./logs/smart-city.log', maxBytes=10000000, backupCount=5)

    # Create a formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger
