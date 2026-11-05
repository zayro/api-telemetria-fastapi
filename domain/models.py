from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class ProtocolDocument:
    id: str
    title: str
    content: str
    source_file: str
    category: str = "general"
    doc_type: str = "text"
    metadata: dict = field(default_factory=dict)
    embedding: tuple[float, ...] | None = None


@dataclass(frozen=True)
class ProtocolSearchResult:
    document: ProtocolDocument
    score: float


@dataclass(frozen=True)
class RAGAnswer:
    query: str
    answer: str | None
    model: str | None
    sources: list[ProtocolSearchResult]


@dataclass(frozen=True)
class IngestResult:
    source_file: str
    doc_type: str
    chunks_count: int
    protocol_ids: list[str]

