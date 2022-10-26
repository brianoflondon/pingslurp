from pydantic import BaseModel
from datetime import datetime

class Podping(BaseModel):
    block: int
    timestamp: datetime
    version: str
    id: str
    medium: str = None
    reason: str = None