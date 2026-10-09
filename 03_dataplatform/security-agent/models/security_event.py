from datetime import datetime

from pydantic import BaseModel


class SecurityEvent(BaseModel):

    event_id: str

    timestamp: datetime

    source: str

    event_type: str

    username: str | None = None

    user_id: str | None = None

    ip_address: str | None = None

    client_id: str | None = None

    realm: str | None = None

    details: dict = {}