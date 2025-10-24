import logging
from concurrent_log_handler import ConcurrentRotatingFileHandler as RotatingFileHandler
import os
from dotenv import load_dotenv
load_dotenv()

def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Returns a rotating logger instance for a given name (e.g. 'daily').
    Creates a log file named '<name>.log' in the 'logs' directory.
    """
    logger = logging.getLogger(name)
    
    debug_env = os.getenv("DEBUG", "").lower() == "true"
    
    if debug_env:
        level = logging.DEBUG
    else:
        level = level if level else logging.INFO
        
    logger.propagate = False  # Prevent duplicate logs from root logger

    # Avoid adding handlers multiple times
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - '
            '%(funcName)s() - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        os.makedirs("logs", exist_ok=True)

        file_handler = RotatingFileHandler(
            f"logs/{name}.log", maxBytes=50 * 1024 * 1024, backupCount=3, encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger