from enum import Enum
from typing import Dict
from pydantic import BaseModel


class MateoStateEnum(Enum):
    PROCESSING = 'PROCESSING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class MateoInboundMessage (BaseModel):
    table_name: str
    correlation_id: str
    receipt_handle: str = None
    parameters: Dict[str, str] = None


class MateoState (BaseModel):
    table_name: str
    correlation_id: str
    state: MateoStateEnum
    start_of_processing_timestamp: float = None
    end_of_processing_timestamp: float = None
