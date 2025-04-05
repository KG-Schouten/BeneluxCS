from logging_config import logger
from rate_limit import RateLimitException

async def check_response(response) -> dict | int:
    """ Checks the response from the API and returns the data or raises an exception """
    status = response.status
    url = str(response.url)
    
    if response.status == 200:
        logger.info(f"[200] Success: {url}")
        return await response.json()
    
    elif response.status == 429:
        logger.info(f"[429] Rate limit reached: {url}")
        raise RateLimitException("Rate limit reached")
    
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
        logger.error(f"[{status}] {message}: {url}")
        return status