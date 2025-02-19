LOKI_URL=http://your-loki-instance:3100
VICTORIA_URL=http://your-victoria-instance:8428
QUERY_INTERVAL=300
BATCH_SIZE=1000
LOG_LEVEL=INFO
MAX_RETRIES=3
RETRY_DELAY=5
REQUEST_TIMEOUT=30
```

### Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| LOKI_URL | URL of your Loki instance | http://localhost:3100 |
| VICTORIA_URL | URL of your Victoria Logs instance | http://localhost:8428 |
| QUERY_INTERVAL | Time interval for querying logs (seconds) | 300 |
| BATCH_SIZE | Number of logs to process in each batch | 1000 |
| LOG_LEVEL | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |
| MAX_RETRIES | Maximum number of retry attempts | 3 |
| RETRY_DELAY | Delay between retries (seconds) | 5 |
| REQUEST_TIMEOUT | Request timeout (seconds) | 30 |

## Docker Deployment

1. Build the Docker image:
```bash
docker build -t log-pipeline .
```

2. Run the container:
```bash
docker run -d \
  --name log-pipeline \
  --env-file .env \
  log-pipeline
```

## Manual Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd log-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the pipeline:
```bash
python main.py
```

## Logging

The utility provides detailed logging with the following levels:
- DEBUG: Detailed information for debugging
- INFO: General operational information
- WARNING: Warning messages for potential issues
- ERROR: Error messages for failed operations

Logs include:
- Connection attempts and status
- Processing statistics
- Error details with stack traces
- Duplicate detection information

## Schema

Victoria Logs schema for audit logs:

```sql
CREATE TABLE audit_logs (
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
)
```

## Troubleshooting

### Common Issues

1. Connection Failures
- Verify URLs are correct and accessible
- Check network connectivity
- Verify SSL certificates if using HTTPS
- For Victoria Logs, test the health endpoint directly:
  ```bash
  curl -v http://your-victoria-instance:8428/health
  ```
- For Loki, verify query endpoint:
  ```bash
  curl -v "http://your-loki-instance:3100/loki/api/v1/query_range"
  ```

2. Authentication Issues
- Ensure proper credentials are set
- Check access permissions

3. Performance Issues
- Adjust BATCH_SIZE based on your requirements
- Monitor memory usage
- Review QUERY_INTERVAL settings

4. Schema Issues
- Verify Victoria Logs schema creation:
  ```sql
  SELECT * FROM system.tables WHERE name = 'audit_logs';