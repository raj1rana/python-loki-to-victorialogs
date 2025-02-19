import requests
from typing import Dict, Any
import logging
import time
from requests.exceptions import RequestException, ConnectionError, Timeout, SSLError
from config import settings
import urllib.parse
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class VictoriaClient:
    def __init__(self):
        self.base_url = settings.VICTORIA_URL.rstrip('/')  # Remove trailing slash if present
        self.max_retries = settings.MAX_RETRIES
        self.retry_delay = settings.RETRY_DELAY
        self.session = self._create_session()
        self._validate_url()

    def _create_session(self):
        """Create a requests session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _validate_url(self):
        """Validate the Victoria Logs URL format and accessibility"""
        try:
            parsed = urllib.parse.urlparse(self.base_url)
            if not all([parsed.scheme, parsed.netloc]):
                raise ValueError(f"Invalid Victoria Logs URL format: {self.base_url}")

            # Test connection with correct Victoria Logs health endpoint
            self._make_request('/health', method='get', validate_only=True)
            logger.info(f"Victoria Logs URL validated and accessible: {self.base_url}")
        except Exception as e:
            logger.error(f"Victoria Logs URL validation failed: {str(e)}")
            raise

    def _make_request(self, endpoint: str, method: str = "post", data: Any = None, 
                     params: Dict[str, Any] = None, json: Dict[str, Any] = None,
                     validate_only: bool = False) -> requests.Response:
        """Make HTTP request to Victoria Logs with retries and detailed logging"""
        # Ensure endpoint starts with / and clean any double slashes
        endpoint = '/' + endpoint.lstrip('/')
        url = urllib.parse.urljoin(self.base_url, endpoint)

        try:
            logger.debug(f"Making {method.upper()} request to: {url}")
            if params:
                logger.debug(f"Request params: {params}")
            if json:
                logger.debug(f"Request JSON: {json}")

            response = self.session.request(
                method=method,
                url=url,
                data=data,
                params=params,
                json=json,
                timeout=settings.REQUEST_TIMEOUT,
                verify=True  # Enforce SSL verification
            )

            logger.debug(f"Response status code: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"Response text: {response.text}")
            response.raise_for_status()

            if validate_only:
                logger.info("Connection test successful")
                return response

            return response

        except SSLError as e:
            logger.error(f"SSL verification failed: {str(e)}")
            raise
        except Timeout:
            logger.error(f"Request to Victoria Logs timed out after {settings.REQUEST_TIMEOUT} seconds")
            raise
        except ConnectionError as e:
            logger.error(f"Connection to Victoria Logs failed: {str(e)}")
            raise
        except RequestException as e:
            logger.error(f"Error communicating with Victoria Logs: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
            raise

    def test_connection(self) -> bool:
        """Test connection to Victoria Logs"""
        try:
            self._make_request('/health', method='get')
            logger.info("Successfully connected to Victoria Logs")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Victoria Logs: {str(e)}")
            return False

    def create_schema(self):
        """Create the audit logs schema in Victoria Logs"""
        schema = """
        CREATE TABLE IF NOT EXISTS audit_logs (
            timestamp DateTime,
            event_record_id UInt64,
            error_code Int32,
            severity Int32,
            state Int32,
            start_time DateTime,
            trace_type String,
            event_class_desc String,
            login_name String,
            host_name String,
            text_data String,
            application_name String,
            database_name String,
            object_name String,
            role_name String,
            computer String,
            event_id String,
            source String,
            environment String,
            region String
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, event_record_id);
        """

        try:
            self._make_request(
                endpoint='/execute',
                data=schema.encode('utf-8')
            )
            logger.info("Victoria Logs schema created successfully")
        except Exception as e:
            logger.error(f"Error creating schema in Victoria Logs: {str(e)}")
            raise

    def insert_log(self, log_data: Dict[str, Any]):
        """Insert a log entry into Victoria Logs"""
        try:
            self._make_request(
                endpoint='/write',  # Use Victoria's native write endpoint
                params={"query": "INSERT INTO audit_logs FORMAT JSONEachRow"},
                json=log_data
            )
            logger.debug(f"Successfully inserted log with event_record_id: {log_data.get('event_record_id')}")
        except Exception as e:
            logger.error(f"Error inserting log to Victoria Logs: {str(e)}")
            raise