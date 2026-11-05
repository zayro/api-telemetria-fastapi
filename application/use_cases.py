import asyncio
import hashlib
import re
import uuid
from dataclasses import dataclass

from application.ports import (
    DocumentExtractor,
    EmbeddingService,
    EventPublisher,
    EventsRepository,
    ItemsCache,
    ItemsRepository,
    LLMService,
    ProtocolRetriever,
)
from domain.errors import (
    DocumentExtractionError,
    EmbeddingServiceError,
    EventNotFound,
    FileTooLargeError,
    ProtocolNotFound,
)
from domain.models import Event, IngestResult, Item, ProtocolDocument, RAGAnswer

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
                f"[{i}] Título: {doc.title} | Archivo: {doc.source_file} | "
                f"Categoría: {doc.category} | Tipo: {doc.doc_type}\n"
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


def _chunk_text(text: str, chunk_size: int = 800, overlap: int = 100) -> list[str]:
    """Divide texto en fragmentos con solape, respetando párrafos y frases cuando es posible."""
    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor que cero")
    overlap = max(0, min(overlap, chunk_size - 1))
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []

    chunks: list[str] = []
    start = 0
    length = len(normalized)
    min_boundary = max(1, chunk_size // 2)

    while start < length:
        end = min(length, start + chunk_size)
        if end < length:
            # Preferir cortar en un salto de párrafo y, si no hay, en una frase.
            boundary = normalized.rfind("\n\n", start + min_boundary, end)
            if boundary != -1:
                end = boundary
            else:
                sentence = normalized.rfind(". ", start + min_boundary, end)
                if sentence != -1:
                    end = sentence + 1

        chunk = normalized[start:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= length:
            break
        start = max(end - overlap, start + 1)

    return chunks


class IngestProtocolFile:
    def __init__(
        self,
        extractor: DocumentExtractor,
        embedding_service: EmbeddingService,
        retriever: ProtocolRetriever,
        max_file_bytes: int = 20 * 1024 * 1024,
        chunk_size: int = 800,
        overlap: int = 100,
    ):
        self._extractor = extractor
        self._embedding_service = embedding_service
        self._retriever = retriever
        self._max_file_bytes = max_file_bytes
        self._chunk_size = chunk_size
        self._overlap = overlap

    @property
    def max_file_bytes(self) -> int:
        return self._max_file_bytes

    async def execute(
        self,
        filename: str,
        content: bytes,
        category: str = "general",
        mime_type: str | None = None,
    ) -> IngestResult:
        filename = (filename or "").strip()
        if not filename:
            raise DocumentExtractionError("El nombre del archivo es obligatorio.")
        if not content:
            raise DocumentExtractionError(f"El archivo '{filename}' está vacío.")
        if len(content) > self._max_file_bytes:
            raise FileTooLargeError(
                f"El archivo '{filename}' supera el límite de "
                f"{self._max_file_bytes // (1024 * 1024)} MB."
            )

        title, text_content, doc_type, metadata = await self._extractor.extract_content(
            filename, content, mime_type
        )
        if not text_content or not text_content.strip():
            raise DocumentExtractionError(
                f"No se pudo extraer contenido indexable de '{filename}'."
            )

        chunks = _chunk_text(
            text_content, chunk_size=self._chunk_size, overlap=self._overlap
        )
        if not chunks:
            raise DocumentExtractionError(
                f"No se generaron fragmentos indexables para '{filename}'."
            )

        embeddings = await self._embedding_service.embed_batch(
            chunks, task_type="RETRIEVAL_DOCUMENT"
        )
        if len(embeddings) != len(chunks):
            raise EmbeddingServiceError(
                "El servicio de embeddings devolvió una cantidad de vectores "
                "distinta a la cantidad de fragmentos."
            )

        dimensions = {len(embedding) for embedding in embeddings}
        expected_dimension = getattr(self._embedding_service, "dimension", None)
        if len(dimensions) != 1 or (expected_dimension and dimensions != {expected_dimension}):
            raise EmbeddingServiceError(
                "Los embeddings generados tienen dimensiones inconsistentes: "
                f"{sorted(dimensions)} (esperado: {expected_dimension})."
            )

        content_hash = hashlib.sha256(content).hexdigest()
        base_name = filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
        safe_stem = (
            re.sub(r"[^a-z0-9]+", "-", base_name.lower()).strip("-") or "archivo"
        )
        embedding_model = getattr(self._embedding_service, "model_name", None)
        embedding_dim = len(embeddings[0]) if embeddings else None
        category = (category or "general").strip().lower() or "general"

        documents: list[ProtocolDocument] = []
        for idx, (chunk, embedding) in enumerate(
            zip(chunks, embeddings), start=1
        ):
            chunk_id = f"file-{safe_stem}-{content_hash[:8]}-{idx}"
            chunk_title = f"{title} (Parte {idx})" if len(chunks) > 1 else title
            chunk_meta = {
                **metadata,
                "origin": "upload",
                "content_hash": content_hash,
                "chunk_index": idx,
                "total_chunks": len(chunks),
                "embedding_model": embedding_model,
                "embedding_dim": embedding_dim,
            }
            documents.append(
                ProtocolDocument(
                    id=chunk_id,
                    title=chunk_title,
                    content=chunk,
                    source_file=filename,
                    category=category,
                    doc_type=doc_type,
                    metadata=chunk_meta,
                    embedding=tuple(embedding),
                )
            )

        await self._retriever.index_documents(documents, replace_source=filename)

        return IngestResult(
            source_file=filename,
            doc_type=doc_type,
            chunks_count=len(chunks),
            protocol_ids=[doc.id for doc in documents],
        )
