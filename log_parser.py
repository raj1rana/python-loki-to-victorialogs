import json
from typing import Dict, Any
from datetime import datetime
import re
from schema import ParsedData

class LogParser:
    @staticmethod
    def parse_data_field(data: str) -> Dict[str, Any]:
        pattern = {
            'error_code': r'Error: (\d+)',
            'severity': r'Severity: (\d+)',
            'state': r'State: (\d+)',
            'start_time': r'StartTime:([^\n]+)',
            'trace_type': r'TraceType:([^\n]+)',
            'event_class_desc': r'EventClassDesc:([^\n]+)',
            'login_name': r'LoginName:([^\n]+)',
            'host_name': r'HostName:([^\n]+)',
            'text_data': r'TextData:([^\n]+)',
            'application_name': r'ApplicationName:([^\n]+)',
            'database_name': r'DatabaseName:([^\n]+)',
            'object_name': r'ObjectName:([^\n]+)',
            'role_name': r'RoleName:([^\n]+)'
        }
        
        parsed = {}
        for key, regex in pattern.items():
            match = re.search(regex, data)
            if match:
                value = match.group(1).strip()
                if key == 'start_time':
                    try:
                        value = datetime.strptime(value, '%m/%d/%Y %H:%M:%S')
                    except ValueError:
                        value = None
                elif key in ['error_code', 'severity', 'state']:
                    try:
                        value = int(value)
                    except ValueError:
                        value = None
                parsed[key] = value
            else:
                parsed[key] = None
                
        return ParsedData(**parsed).dict()

    @staticmethod
    def parse_log_entry(log_entry: Dict[str, Any]) -> Dict[str, Any]:
        parsed_data = LogParser.parse_data_field(log_entry['fields']['Data'])
        
        return {
            'timestamp': datetime.fromtimestamp(log_entry['timestamp']),
            'event_record_id': log_entry['fields']['EventRecordID'],
            **parsed_data,
            'computer': log_entry['tags']['Computer'],
            'event_id': log_entry['tags']['EventID'],
            'source': log_entry['tags']['Source'],
            'environment': log_entry['tags']['environment'],
            'region': log_entry['tags']['region']
        }
