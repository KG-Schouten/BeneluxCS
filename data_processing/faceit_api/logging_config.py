import sys
import os
import logging
from logging.handlers import RotatingFileHandler

# Ensure repo root (BeneluxCS) is in sys.path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Dynamically get path to repo root and logs folder
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
LOG_DIR = os.path.join(REPO_ROOT, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)  # Create the logs dir if it doesn't exist

# Create the formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- API Logger Setup ---
api_logger = logging.getLogger("api_logger")
api_logger.setLevel(logging.INFO)

if not api_logger.hasHandlers():
    api_console_handler = logging.StreamHandler()
    api_console_handler.setLevel(logging.CRITICAL)
    
    api_file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "api.log"),
        maxBytes=10*1024*1024,
        backupCount=2
    )
    api_file_handler.setLevel(logging.INFO)
    
    api_console_handler.setFormatter(formatter)
    api_file_handler.setFormatter(formatter)
    
    api_logger.addHandler(api_console_handler)
    api_logger.addHandler(api_file_handler)

api_logger.propagate = False

# --- Function Logger Setup ---
function_logger = logging.getLogger("function_logger")
function_logger.setLevel(logging.INFO)

if not function_logger.hasHandlers():
    function_console_handler = logging.StreamHandler()
    function_console_handler.setLevel(logging.ERROR)
    
    function_file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "functions.log"),
        maxBytes=10*1024*1024,
        backupCount=2
    )
    function_file_handler.setLevel(logging.INFO)
    
    function_console_handler.setFormatter(formatter)
    function_file_handler.setFormatter(formatter)
    
    function_logger.addHandler(function_console_handler)
    function_logger.addHandler(function_file_handler)

function_logger.propagate = False
