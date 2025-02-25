import requests
from datetime import datetime
import time
from typing import Generator, Dict, Any
from config import settings
import logging
from requests.exceptions import RequestException, ConnectionError

logger = logging.getLogger(__name__)

class LokiClient:
    def __init__(self):
        self.base_url = settings.LOKI_URL.rstrip('/')  # Remove trailing slash if present
        self.max_retries = settings.MAX_RETRIES
        self.retry_delay = settings.RETRY_DELAY  # seconds
        self._validate_connection()

    def _validate_connection(self):
        """Validate Loki connection and query"""
        try:
            # Test basic connectivity
            response = requests.get(
                f"{self.base_url}/ready",
                timeout=settings.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            logger.info("Successfully connected to Loki")

            # Validate query format
            logger.info(f"Using Loki query: {settings.LOKI_QUERY}")
            test_response = self._make_request(
                f"{self.base_url}/loki/api/v1/query",
                params={
                    "query": settings.LOKI_QUERY,
                    "limit": 1,
                    "time": int(datetime.now().timestamp())
                }
            )
            if test_response.get("status") == "success":
                logger.info("Loki query format validated successfully")
            else:
                logger.warning("Loki query validation response: %s", test_response)
        except Exception as e:
            logger.error(f"Loki validation failed: {str(e)}")
            raise

    def _get_unix_timestamp(self, dt: datetime) -> int:
        return int(dt.timestamp() * 1e9)

    def _make_request(self, url: str, params: Dict[str, Any], retry_count: int = 0) -> Dict:
        try:
            logger.debug(f"Making request to Loki: {url}")
            logger.debug(f"Query parameters: {params}")
            response = requests.get(
                url, 
                params=params, 
                timeout=settings.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except ConnectionError as e:
            if retry_count < self.max_retries:
                logger.warning(f"Connection to Loki failed. Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
                return self._make_request(url, params, retry_count + 1)
            raise Exception(f"Failed to connect to Loki after {self.max_retries} attempts: {str(e)}")
        except RequestException as e:
            logger.error(f"Error querying Loki: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
            raise

    def query_range(self, start_time: datetime, end_time: datetime) -> Generator[Dict[str, Any], None, None]:
        url = f"{self.base_url}/loki/api/v1/query_range"

        params = {
            "query": settings.LOKI_QUERY,
            "start": self._get_unix_timestamp(start_time),
            "end": self._get_unix_timestamp(end_time),
            "limit": settings.BATCH_SIZE,
        }

        while True:
            try:
                logger.debug(f"Querying Loki from {start_time} to {end_time}")
                data = self._make_request(url, params)

                if not data.get("data", {}).get("result", []):
                    logger.info("No more results to process")
                    break

                for stream in data["data"]["result"]:
                    for value in stream.get("values", []):
                        yield {"timestamp": value[0], "log": value[1]}

                # Update start time for next batch
                last_timestamp = int(data["data"]["result"][-1]["values"][-1][0])
                params["start"] = last_timestamp + 1

                if params["start"] >= params["end"]:
                    break

            except Exception as e:
                logger.error(f"Error in query_range: {str(e)}")
                raise