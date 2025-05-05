import logging

# Confige logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("faceit_api.log"), # Logs to file
        # logging.StreamHandler() # Logs to console
    ]
)

logger = logging.getLogger(__name__)
