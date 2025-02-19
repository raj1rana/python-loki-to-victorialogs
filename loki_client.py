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
        self.max_retries = 3
        self.retry_delay = 5  # seconds

    def _get_unix_timestamp(self, dt: datetime) -> int:
        return int(dt.timestamp() * 1e9)

    def _make_request(self, url: str, params: Dict[str, Any], retry_count: int = 0) -> Dict:
        try:
            logger.debug(f"Making request to Loki: {url}")
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
                data = self._make_request(url, params)

                if not data.get("data", {}).get("result", []):
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