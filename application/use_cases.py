import asyncio
import uuid
from dataclasses import dataclass

from application.ports import (
    EventPublisher,
    EventsRepository,
    ItemsCache,
    ItemsRepository,
    LLMService,
    ProtocolRetriever,
)
from domain.errors import EventNotFound, ProtocolNotFound
from domain.models import Event, Item, ProtocolDocument, RAGAnswer

CACHE_TTL_SECONDS = 30
ITEMS_LIMIT = 20


@dataclass(frozen=True)
class ItemsResult:
    source: str
    items: list[Item]


class SimulateTest:
    async def execute(self, delay_ms: int) -> None:
        await asyncio.sleep(delay_ms / 1000)


class CreateItem:
    def __init__(self, repository: ItemsRepository, cache: ItemsCache):
        self._repository = repository
        self._cache = cache

    async def execute(self, name: str) -> Item:
        item = await self._repository.create(name)
        await self._cache.invalidate()
        return item


class ListItems:
    def __init__(self, repository: ItemsRepository, cache: ItemsCache):
        self._repository = repository
        self._cache = cache

    async def execute(self) -> ItemsResult:
        cached_items = await self._cache.get()
        if cached_items is not None:
            return ItemsResult(source="cache", items=cached_items)

        items = await self._repository.list_recent(ITEMS_LIMIT)
        await self._cache.set(items, CACHE_TTL_SECONDS)
        return ItemsResult(source="db", items=items)


class PublishEvent:
    def __init__(self, publisher: EventPublisher, repository: EventsRepository):
        self._publisher = publisher
        self._repository = repository

    async def execute(self, name: str) -> str:
        event = await self._repository.create(
            Event(event_id=str(uuid.uuid4()), name=name)
        )
        try:
            await self._publisher.publish(event)
        except Exception:
            # Evita registros de eventos que nunca llegaron al broker
            await self._repository.delete(event.event_id)
            raise
        return event.event_id


class ListEvents:
    def __init__(self, repository: EventsRepository):
        self._repository = repository

    async def execute(self) -> list[Event]:
        return await self._repository.list_recent(ITEMS_LIMIT)


class GetEvent:
    def __init__(self, repository: EventsRepository):
        self._repository = repository

    async def execute(self, event_id: str) -> Event:
        event = await self._repository.get(event_id)
        if event is None:
            raise EventNotFound(event_id)
        return event


class UpdateEvent:
    def __init__(self, repository: EventsRepository):
        self._repository = repository

    async def execute(self, event_id: str, name: str) -> Event:
        event = await self._repository.update(event_id, name)
        if event is None:
            raise EventNotFound(event_id)
        return event


class DeleteEvent:
    def __init__(self, repository: EventsRepository):
        self._repository = repository

    async def execute(self, event_id: str) -> None:
        if not await self._repository.delete(event_id):
            raise EventNotFound(event_id)


class QueryProtocolsRAG:
    def __init__(self, retriever: ProtocolRetriever, llm: LLMService):
        self._retriever = retriever
        self._llm = llm

    async def execute(
        self,
        query: str,
        top_k: int = 3,
        category: str | None = None,
        generate_answer: bool = True,
        temperature: float = 0.2,
    ) -> RAGAnswer:
        sources = await self._retriever.search(
            query=query, limit=top_k, category=category
        )
        if not generate_answer:
            return RAGAnswer(
                query=query,
                answer=None,
                model=None,
                sources=sources,
            )

        context_blocks = []
        for i, res in enumerate(sources, start=1):
            doc = res.document
            context_blocks.append(
                f"[{i}] Título: {doc.title} | Archivo: {doc.source_file} | Categoría: {doc.category}\n"
                f"Relevancia: {res.score:.2f}\n"
                f"Contenido:\n{doc.content}"
            )

        context_text = "\n\n---\n\n".join(context_blocks)
        prompt = (
            "Eres un asistente técnico especializado en protocolos operativos, de seguridad y de sistemas.\n"
            "Tu tarea es responder a la consulta del usuario basándote de manera estricta y fundamentada "
            "en el corpus de protocolos provisto a continuación.\n"
            "Si el corpus no contiene información suficiente para responder, indícalo con claridad.\n"
            "Cita los nombres de los protocolos o archivos utilizados en tu explicación.\n\n"
            f"=== CORPUS DE PROTOCOLOS RECUPERADOS ===\n{context_text or 'No se encontraron fragmentos relevantes en el corpus.'}\n"
            f"=========================================\n\n"
            f"CONSULTA: {query}\n\n"
            "RESPUESTA:"
        )

        answer = await self._llm.generate_response(
            prompt=prompt,
            context=[res.document.content for res in sources],
            temperature=temperature,
        )
        return RAGAnswer(
            query=query,
            answer=answer,
            model=self._llm.model_name,
            sources=sources,
        )


class ListProtocols:
    def __init__(self, retriever: ProtocolRetriever):
        self._retriever = retriever

    async def execute(self) -> list[ProtocolDocument]:
        return await self._retriever.list_documents()


class GetProtocol:
    def __init__(self, retriever: ProtocolRetriever):
        self._retriever = retriever

    async def execute(self, protocol_id: str) -> ProtocolDocument:
        doc = await self._retriever.get_document(protocol_id)
        if doc is None:
            raise ProtocolNotFound(protocol_id)
        return doc

