import logging
from datetime import datetime, timedelta
import time
from config import settings
from pipeline import LogPipeline

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    pipeline = LogPipeline()
    
    try:
        # Initialize Victoria Logs schema
        pipeline.setup()
        logger.info("Victoria Logs schema initialized")
        
        while True:
            try:
                end_time = datetime.now()
                start_time = end_time - timedelta(seconds=settings.QUERY_INTERVAL)
                
                logger.info(f"Processing logs from {start_time} to {end_time}")
                
                total_processed, total_duplicates = pipeline.process_logs(
                    start_time=start_time,
                    end_time=end_time
                )
                
                logger.info(
                    f"Completed processing cycle. "
                    f"Processed: {total_processed}, "
                    f"Duplicates: {total_duplicates}"
                )
                
            except Exception as e:
                logger.error(f"Error in processing cycle: {str(e)}")
                
            # Wait for next interval
            time.sleep(settings.QUERY_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("Shutting down log pipeline")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
