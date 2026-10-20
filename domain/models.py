from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Item:
    id: int
    name: str
    created_at: datetime


@dataclass(frozen=True)
class Event:
    event_id: str
    name: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
