import logging
from logging.handlers import RotatingFileHandler
import os

def get_logger(name: str):
    """
    Returns a rotating logger instance for a given name (e.g. 'daily').
    Creates a log file named '<name>.log'.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent duplicate logs from root logger

    # Avoid adding handlers multiple times
    if not logger.hasHandlers():
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - '
            '%(funcName)s() - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Create logs directory if it doesn’t exist
        os.makedirs("logs", exist_ok=True)

        # Rotating file handler (max 5MB per file, keep 3 backups)
        file_handler = RotatingFileHandler(
            f"logs/{name}.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Console handler (optional)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

def log_message(name: str, desc: str, level: str = "info"):
    """
    Logs a message to the logger with the given name.
    - name: logger name (used for filename)
    - desc: message to log
    - level: 'info', 'error', 'warning', 'debug', etc.
    """
    logger = get_logger(name)
    level = level.lower()

    if level == "error":
        logger.error(desc)
    elif level == "warning":
        logger.warning(desc)
    elif level == "debug":
        logger.debug(desc)
    else:
        logger.info(desc)