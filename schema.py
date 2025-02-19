from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel

class LogFields(BaseModel):
    Data: str
    EventRecordID: int
    Message: str
    ProcessName: str
    UserID: str
    Version: int

class LogTags(BaseModel):
    Channel: str
    Computer: str
    EventID: str
    Keywords: str
    Level: str
    LevelText: str
    Opcode: str
    OpcodeText: str
    Source: str
    Task: str
    TaskText: str
    app_id: str
    app_name: str
    environment: str
    host: str
    hosting_env: str
    instance: str
    job: str
    operational_status: str
    os: str
    os_version: str
    region: str
    server_location: str
    service_level: str
    vpcx_accountid: str

class LogEntry(BaseModel):
    fields: LogFields
    name: str
    tags: LogTags
    timestamp: int

class ParsedData(BaseModel):
    error_code: Optional[int]
    severity: Optional[int]
    state: Optional[int]
    start_time: Optional[datetime]
    trace_type: Optional[str]
    event_class_desc: Optional[str]
    login_name: Optional[str]
    host_name: Optional[str]
    text_data: Optional[str]
    application_name: Optional[str]
    database_name: Optional[str]
    object_name: Optional[str]
    role_name: Optional[str]