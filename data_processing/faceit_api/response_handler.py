# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing.faceit_api.sliding_window import RateLimitException

from logs.update_logger import get_logger
api_logger = get_logger("api")

async def check_response(response) -> dict | int:
    """ Checks the response from the API and returns the data or raises an exception """
    status = response.status
    url = str(response.url)
    
    if response.status == 200:
        api_logger.info(f"[200] Success: {url}")
        return await response.json()
    
    elif response.status == 429:
        api_logger.info(f"[429] Rate limit reached: {url}")
        raise RateLimitException()
    
    else:
        error_map = {
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            500: "Internal Server Error",
            502: "Bad Gateway",
            503: "Service Unavailable",
        }
        message = error_map.get(response.status, f"HTTP Unknown Error: {response.status}")
        api_logger.error(f"[{status}] {message}: {url}")
        return status