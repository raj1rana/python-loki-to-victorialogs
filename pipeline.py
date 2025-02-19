from loki_client import LokiClient
from victoria_client import VictoriaClient
from log_parser import LogParser
from config import settings
import logging
from typing import Set
import json
import time

logger = logging.getLogger(__name__)

class LogPipeline:
    def __init__(self):
        self.loki_client = LokiClient()
        self.victoria_client = VictoriaClient()
        self.log_parser = LogParser()
        self.processed_records: Set[int] = set()

    def setup(self):
        """Initialize Victoria Logs schema and test connections"""
        # Test Victoria Logs connection
        if not self.victoria_client.test_connection():
            raise ConnectionError("Failed to connect to Victoria Logs during setup")

        # Create schema
        self.victoria_client.create_schema()
        logger.info("Victoria Logs schema initialized successfully")

    def is_duplicate(self, event_record_id: int) -> bool:
        """Check if log entry is duplicate based on EventRecordID"""
        if event_record_id in self.processed_records:
            return True
        self.processed_records.add(event_record_id)
        return False

    def process_logs(self, start_time, end_time):
        """Process logs from Loki and send to Victoria Logs"""
        total_processed = 0
        total_duplicates = 0

        try:
            for log_entry in self.loki_client.query_range(start_time, end_time):
                try:
                    # Parse the log entry
                    log_data = json.loads(log_entry['log'])

                    # Check for duplicate
                    if self.is_duplicate(log_data['fields']['EventRecordID']):
                        total_duplicates += 1
                        logger.debug(f"Skipping duplicate log with EventRecordID: {log_data['fields']['EventRecordID']}")
                        continue

                    # Parse and transform the log entry
                    parsed_log = self.log_parser.parse_log_entry(log_data)

                    # Send to Victoria Logs
                    self.victoria_client.insert_log(parsed_log)

                    total_processed += 1

                    if total_processed % 100 == 0:
                        logger.info(f"Processed {total_processed} logs ({total_duplicates} duplicates)")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse log entry: {str(e)}")
                except Exception as e:
                    logger.error(f"Error processing log entry: {str(e)}")

        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            raise

        logger.info(f"Processed {total_processed} logs, skipped {total_duplicates} duplicates")
        return total_processed, total_duplicates