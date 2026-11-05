from typing import Protocol

from domain.models import (
    Event,
    Item,
    ProtocolDocument,
    ProtocolSearchResult,
)


class ItemsRepository(Protocol):
    async def create(self, name: str) -> Item: ...

    async def list_recent(self, limit: int) -> list[Item]: ...


class ItemsCache(Protocol):
    async def get(self) -> list[Item] | None: ...

    async def set(self, items: list[Item], ttl_seconds: int) -> None: ...

    async def invalidate(self) -> None: ...


class EventsRepository(Protocol):
    async def create(self, event: Event) -> Event: ...

    async def get(self, event_id: str) -> Event | None: ...

    async def list_recent(self, limit: int) -> list[Event]: ...

    async def update(self, event_id: str, name: str) -> Event | None: ...

    async def delete(self, event_id: str) -> bool: ...


class EventPublisher(Protocol):
    async def publish(self, event: Event) -> None: ...


class ProtocolRetriever(Protocol):
    async def initialize(self) -> None: ...

    async def search(
        self, query: str, limit: int = 5, category: str | None = None
    ) -> list[ProtocolSearchResult]: ...

    async def list_documents(self) -> list[ProtocolDocument]: ...

    async def get_document(self, document_id: str) -> ProtocolDocument | None: ...

    async def index_document(self, document: ProtocolDocument) -> None: ...

    async def index_documents(
        self,
        documents: list[ProtocolDocument],
        replace_source: str | None = None,
    ) -> None: ...


class ProtocolIndexRepository(Protocol):
    async def load_all(self) -> list[ProtocolDocument]: ...

    async def save_documents(self, documents: list[ProtocolDocument]) -> None: ...

    async def replace_documents(
        self, source_file: str, documents: list[ProtocolDocument]
    ) -> None: ...


class EmbeddingService(Protocol):
    @property
    def model_name(self) -> str: ...

    @property
    def dimension(self) -> int: ...

    async def embed_text(
        self, text: str, task_type: str = "RETRIEVAL_QUERY"
    ) -> list[float]: ...

    async def embed_batch(
        self, texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT"
    ) -> list[list[float]]: ...


class DocumentExtractor(Protocol):
    async def extract_content(
        self, filename: str, content: bytes, mime_type: str | None = None
    ) -> tuple[str, str, str, dict]: ...


class LLMService(Protocol):
    @property
    def model_name(self) -> str: ...

    async def generate_response(
        self, prompt: str, context: list[str], temperature: float = 0.2
    ) -> str: ...
