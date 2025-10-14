import time
import config.config as config
from utils.paginate import paginatedGet

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1  # seconds

def retry_get(url, params):
    if not url or not isinstance(url, str) or not isinstance(params, dict):
        logger.error("Invalid URL or parameters")
        return None

    retry_count = 0
    while retry_count < MAX_RETRIES:
        logger.info(f"retry_get called with {url}")

        try:
            data = paginatedGet(url, config.HEADERS, params)
            break
        except:
            retry_count += 1
            if retry_count == MAX_RETRIES:
                logger.error(f"Failed to fetch data after {MAX_RETRIES} attempts")
                return None
            delay = INITIAL_RETRY_DELAY * (2 ** (retry_count - 1))
            time.sleep(delay)
            continue
    return data