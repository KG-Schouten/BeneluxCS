# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
from logging.handlers import RotatingFileHandler

# Create the formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)

# --- API Logger Setup ---
api_logger = logging.getLogger("api_logger")
api_logger.setLevel(logging.INFO)

if not api_logger.hasHandlers():
    # Create console handler
    api_console_handler = logging.StreamHandler()
    api_console_handler.setLevel(logging.CRITICAL)
    
    # Create the file handler
    api_file_handler = RotatingFileHandler("logs/api.log", maxBytes=10*1024*1024, backupCount=2) # 10 MB per file, keep 2 backups
    api_file_handler.setLevel(logging.INFO)
    
    # Set the formatter for the handlers
    api_console_handler.setFormatter(formatter)
    api_file_handler.setFormatter(formatter)
    
    api_logger.addHandler(api_console_handler)
    api_logger.addHandler(api_file_handler)

api_logger.propagate = False  # Prevent logging to root logger


# --- Function Logger Setup ---
function_logger = logging.getLogger("function_logger")
function_logger.setLevel(logging.INFO)

if not function_logger.hasHandlers():
    # Create console handler
    function_console_handler = logging.StreamHandler()
    function_console_handler.setLevel(logging.WARNING)
    
    # Create the file handler
    function_file_handler = RotatingFileHandler("logs/functions.log", maxBytes=10*1024*1024, backupCount=2) # 10 MB per file, keep 2 backups
    function_file_handler.setLevel(logging.INFO)
    
    # Set the formatter for the handlers
    function_console_handler.setFormatter(formatter)
    function_file_handler.setFormatter(formatter)
    
    function_logger.addHandler(function_console_handler)
    function_logger.addHandler(function_file_handler)

function_logger.propagate = False  # Prevent logging to root logger
