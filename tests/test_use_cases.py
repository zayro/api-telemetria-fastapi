import unittest
from dataclasses import replace
from datetime import datetime, timezone

from application.use_cases import (
    CreateItem,
    DeleteEvent,
    GetEvent,
    GetProtocol,
    IngestProtocolFile,
    ListEvents,
    ListItems,
    ListProtocols,
    PublishEvent,
    QueryProtocolsRAG,
    UpdateEvent,
    _chunk_text,
)
from domain.errors import (
    DocumentExtractionError,
    EmbeddingServiceError,
    EventNotFound,
    FileTooLargeError,
    GeminiServiceError,
    ProtocolNotFound,
)
from domain.models import Item, ProtocolDocument, ProtocolSearchResult


class FakeRepository:
    def __init__(self, items=None):
        self.items = items or []
        self.created_names = []
        self.requested_limit = None

    async def create(self, name):
        self.created_names.append(name)
        return self.items[0]

    async def list_recent(self, limit):
        self.requested_limit = limit
        return self.items


class FakeCache:
    def __init__(self, value=None):
        self.value = value
        self.set_value = None
        self.invalidated = False

    async def get(self):
        return self.value

    async def set(self, items, ttl_seconds):
        self.set_value = (items, ttl_seconds)
        self.value = items

    async def invalidate(self):
        self.invalidated = True


class FakePublisher:
    def __init__(self):
        self.events = []

    async def publish(self, event):
        self.events.append(event)


class FakeEventsRepository:
    def __init__(self):
        self.events = {}

    async def create(self, event):
        self.events[event.event_id] = event
        return event

    async def get(self, event_id):
        return self.events.get(event_id)

    async def list_recent(self, limit):
        return list(self.events.values())[:limit]

    async def update(self, event_id, name):
        if event_id not in self.events:
            return None
        self.events[event_id] = replace(self.events[event_id], name=name)
        return self.events[event_id]

    async def delete(self, event_id):
        return self.events.pop(event_id, None) is not None


class FailingPublisher:
    async def publish(self, event):
        raise RuntimeError("broker down")


class FakeProtocolRetriever:
    def __init__(self, documents=None):
        self.documents = {d.id: d for d in (documents or [])}
        self.last_query = None
        self.last_category = None
        self.last_replace_source = None
        self.initialized = False

    async def initialize(self):
        self.initialized = True

    async def list_documents(self):
        return list(self.documents.values())

    async def get_document(self, document_id):
        return self.documents.get(document_id)

    async def index_document(self, document):
        self.documents[document.id] = document

    async def index_documents(self, documents, replace_source=None):
        self.last_replace_source = replace_source
        if replace_source:
            self.documents = {
                doc_id: doc
                for doc_id, doc in self.documents.items()
                if doc.source_file != replace_source
            }
        for document in documents:
            self.documents[document.id] = document

    async def search(self, query, limit=5, category=None):
        self.last_query = query
        self.last_category = category
        docs = list(self.documents.values())
        if category:
            docs = [d for d in docs if d.category == category]
        return [
            ProtocolSearchResult(document=d, score=0.95 - (i * 0.1))
            for i, d in enumerate(docs[:limit])
        ]


class FakeDocumentExtractor:
    def __init__(
        self,
        title="Documento Extraído",
        text="Contenido extraído del archivo",
        doc_type="pdf",
        meta=None,
    ):
        self.title = title
        self.text = text
        self.doc_type = doc_type
        self.meta = meta or {"pages": 1}

    async def extract_content(self, filename, content, mime_type=None):
        return self.title, self.text, self.doc_type, self.meta


class FakeEmbeddingService:
    def __init__(self, dim=16, model_name="fake-embedding"):
        self.dim = dim
        self.model_name = model_name
        self.embedded_texts = []
        self.last_task_type = None

    @property
    def dimension(self):
        return self.dim

    async def embed_text(self, text, task_type="RETRIEVAL_QUERY"):
        self.last_task_type = task_type
        self.embedded_texts.append(text)
        return [0.1] * self.dim

    async def embed_batch(self, texts, task_type="RETRIEVAL_DOCUMENT"):
        self.last_task_type = task_type
        self.embedded_texts.extend(texts)
        return [[0.1 * (i + 1)] * self.dim for i, _ in enumerate(texts)]



class FakeLLMService:
    def __init__(self, answer="Respuesta simulada de Gemini basada en protocolos."):
        self._answer = answer
        self.last_prompt = None
        self.last_context = None
        self.last_temperature = None

    @property
    def model_name(self):
        return "gemini-2.5-flash-test"

    async def generate_response(self, prompt, context, temperature=0.2):
        self.last_prompt = prompt
        self.last_context = context
        self.last_temperature = temperature
        return self._answer


class FailingLLMService:
    @property
    def model_name(self):
        return "gemini-error-model"

    async def generate_response(self, prompt, context, temperature=0.2):
        raise GeminiServiceError("Error simulado de cuota de Gemini", status_code=429)



class UseCaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.item = Item(
            id=1,
            name="app",
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    async def test_list_items_loads_database_and_populates_cache(self):
        repository = FakeRepository([self.item])
        cache = FakeCache()

        result = await ListItems(repository, cache).execute()

        self.assertEqual(result.source, "db")
        self.assertEqual(result.items, [self.item])
        self.assertEqual(repository.requested_limit, 20)
        self.assertEqual(cache.set_value, ([self.item], 30))

    async def test_list_items_uses_cached_values(self):
        repository = FakeRepository([self.item])
        cache = FakeCache([self.item])

        result = await ListItems(repository, cache).execute()

        self.assertEqual(result.source, "cache")
        self.assertEqual(result.items, [self.item])
        self.assertIsNone(repository.requested_limit)
        self.assertIsNone(cache.set_value)

    async def test_create_item_invalidates_cache(self):
        repository = FakeRepository([self.item])
        cache = FakeCache()

        result = await CreateItem(repository, cache).execute("app")

        self.assertEqual(result, self.item)
        self.assertEqual(repository.created_names, ["app"])
        self.assertTrue(cache.invalidated)

    async def test_publish_event_stores_and_publishes(self):
        publisher = FakePublisher()
        repository = FakeEventsRepository()

        event_id = await PublishEvent(publisher, repository).execute("app")

        self.assertEqual(len(publisher.events), 1)
        self.assertEqual(publisher.events[0].event_id, event_id)
        self.assertEqual(publisher.events[0].name, "app")
        self.assertIn(event_id, repository.events)

    async def test_publish_event_removes_record_when_broker_fails(self):
        repository = FakeEventsRepository()

        with self.assertRaises(RuntimeError):
            await PublishEvent(FailingPublisher(), repository).execute("app")

        self.assertEqual(repository.events, {})

    async def test_event_crud_use_cases(self):
        repository = FakeEventsRepository()
        event_id = await PublishEvent(FakePublisher(), repository).execute("app")

        self.assertEqual((await GetEvent(repository).execute(event_id)).name, "app")
        self.assertEqual(len(await ListEvents(repository).execute()), 1)

        updated = await UpdateEvent(repository).execute(event_id, "renamed")
        self.assertEqual(updated.name, "renamed")

        await DeleteEvent(repository).execute(event_id)
        with self.assertRaises(EventNotFound):
            await GetEvent(repository).execute(event_id)

    async def test_update_and_delete_missing_event_raise_not_found(self):
        repository = FakeEventsRepository()

        with self.assertRaises(EventNotFound):
            await UpdateEvent(repository).execute("missing", "x")
        with self.assertRaises(EventNotFound):
            await DeleteEvent(repository).execute("missing")

    async def test_protocols_rag_query_with_llm_answer(self):
        doc1 = ProtocolDocument(
            id="proto-1",
            title="Protocolo de Incidentes",
            content="Aislar nodo comprometido y revocar credenciales.",
            source_file="incidents.md",
            category="seguridad",
        )
        retriever = FakeProtocolRetriever([doc1])
        llm = FakeLLMService(answer="Para incidentes, aislar el nodo y revocar credenciales.")

        rag_uc = QueryProtocolsRAG(retriever, llm)
        result = await rag_uc.execute("¿Cómo responder a una brecha?", top_k=2)

        self.assertEqual(result.query, "¿Cómo responder a una brecha?")
        self.assertEqual(result.model, "gemini-2.5-flash-test")
        self.assertIn("aislar", result.answer.lower())
        self.assertEqual(len(result.sources), 1)
        self.assertEqual(result.sources[0].document.id, "proto-1")
        self.assertIn("Aislar nodo comprometido", llm.last_prompt)

    async def test_protocols_rag_query_search_only_without_llm(self):
        doc1 = ProtocolDocument(
            id="proto-1",
            title="Protocolo OAuth",
            content="Tokens JWT con vigencia de 15 min.",
            source_file="oauth.md",
            category="seguridad",
        )
        retriever = FakeProtocolRetriever([doc1])
        llm = FakeLLMService()

        rag_uc = QueryProtocolsRAG(retriever, llm)
        result = await rag_uc.execute("Vigencia de token", top_k=1, generate_answer=False)

        self.assertIsNone(result.answer)
        self.assertIsNone(result.model)
        self.assertEqual(len(result.sources), 1)
        self.assertIsNone(llm.last_prompt)

    async def test_protocols_rag_filter_by_category(self):
        doc_sec = ProtocolDocument(
            id="p-sec",
            title="Seguridad",
            content="Reglas de firewall.",
            source_file="sec.md",
            category="seguridad",
        )
        doc_ops = ProtocolDocument(
            id="p-ops",
            title="Backups",
            content="Snapshots diarios.",
            source_file="ops.md",
            category="operaciones",
        )
        retriever = FakeProtocolRetriever([doc_sec, doc_ops])
        llm = FakeLLMService()

        rag_uc = QueryProtocolsRAG(retriever, llm)
        result = await rag_uc.execute("Snapshots", category="operaciones")

        self.assertEqual(len(result.sources), 1)
        self.assertEqual(result.sources[0].document.category, "operaciones")

    async def test_protocols_rag_propagates_gemini_service_error(self):
        doc = ProtocolDocument(
            id="p-1",
            title="TLS",
            content="TLS 1.3 requerido.",
            source_file="tls.md",
            category="redes",
        )
        retriever = FakeProtocolRetriever([doc])
        llm = FailingLLMService()

        rag_uc = QueryProtocolsRAG(retriever, llm)
        with self.assertRaises(GeminiServiceError):
            await rag_uc.execute("Requisitos de cifrado")

    async def test_list_and_get_protocols(self):
        doc = ProtocolDocument(
            id="p-abc",
            title="Protocolo Alpha",
            content="Contenido técnico.",
            source_file="alpha.md",
            category="general",
        )
        retriever = FakeProtocolRetriever([doc])

        list_uc = ListProtocols(retriever)
        docs = await list_uc.execute()
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].id, "p-abc")

        get_uc = GetProtocol(retriever)
        retrieved_doc = await get_uc.execute("p-abc")
        self.assertEqual(retrieved_doc.title, "Protocolo Alpha")

        with self.assertRaises(ProtocolNotFound):
            await get_uc.execute("inexistente")

    async def test_ingest_protocol_pdf_file_chunks_and_indexes(self):
        long_content = "Primer párrafo de prueba del protocolo en PDF.\n\n" * 10
        extractor = FakeDocumentExtractor(
            title="Manual de Seguridad PDF",
            text=long_content,
            doc_type="pdf",
            meta={"pages": 3},
        )
        embedding_service = FakeEmbeddingService()
        retriever = FakeProtocolRetriever()

        ingest_uc = IngestProtocolFile(extractor, embedding_service, retriever)
        result = await ingest_uc.execute(
            filename="manual_seguridad.pdf",
            content=b"%PDF-mock",
            category="seguridad",
        )

        self.assertEqual(result.source_file, "manual_seguridad.pdf")
        self.assertEqual(result.doc_type, "pdf")
        self.assertGreater(result.chunks_count, 0)
        self.assertEqual(len(result.protocol_ids), result.chunks_count)
        self.assertEqual(len(retriever.documents), result.chunks_count)

        first_doc = list(retriever.documents.values())[0]
        self.assertEqual(first_doc.doc_type, "pdf")
        self.assertIsNotNone(first_doc.embedding)
        self.assertEqual(first_doc.category, "seguridad")

    async def test_ingest_protocol_image_file_indexes_as_image_doc_type(self):
        extractor = FakeDocumentExtractor(
            title="Diagrama Red TLS",
            text="Explicación técnica del flujo TLS entre microservicios y puertos.",
            doc_type="image",
            meta={"mime_type": "image/png"},
        )
        embedding_service = FakeEmbeddingService()
        retriever = FakeProtocolRetriever()

        ingest_uc = IngestProtocolFile(extractor, embedding_service, retriever)
        result = await ingest_uc.execute(
            filename="diagrama_tls.png",
            content=b"\x89PNG-mock",
            category="redes",
        )

        self.assertEqual(result.doc_type, "image")
        self.assertEqual(result.chunks_count, 1)
        doc = list(retriever.documents.values())[0]
        self.assertEqual(doc.doc_type, "image")
        self.assertIn("flujo TLS", doc.content)

    def test_chunk_text_splits_within_boundary(self):
        text = "Bloque de texto A.\n\nBloque de texto B.\n\nBloque de texto C."
        chunks = _chunk_text(text, chunk_size=30, overlap=5)
        self.assertGreaterEqual(len(chunks), 2)
        for chunk in chunks:
            self.assertTrue(len(chunk) > 0)

    def test_chunk_text_empty_returns_no_chunks(self):
        self.assertEqual(_chunk_text(""), [])
        self.assertEqual(_chunk_text("   \n\n  "), [])

    async def test_ingest_replaces_previous_chunks_for_same_source(self):
        retriever = FakeProtocolRetriever(
            [
                ProtocolDocument(
                    id="old-chunk",
                    title="Anterior",
                    content="contenido viejo",
                    source_file="manual.pdf",
                )
            ]
        )
        ingest_uc = IngestProtocolFile(
            FakeDocumentExtractor(text="Contenido nuevo del manual."),
            FakeEmbeddingService(),
            retriever,
        )

        result = await ingest_uc.execute(
            filename="manual.pdf", content=b"nuevo", category="seguridad"
        )

        self.assertEqual(retriever.last_replace_source, "manual.pdf")
        self.assertNotIn("old-chunk", retriever.documents)
        self.assertEqual(len(retriever.documents), result.chunks_count)
        self.assertEqual(result.protocol_ids[0].split("-")[0], "file")

    async def test_ingest_rejects_oversized_file(self):
        ingest_uc = IngestProtocolFile(
            FakeDocumentExtractor(),
            FakeEmbeddingService(),
            FakeProtocolRetriever(),
            max_file_bytes=4,
        )
        with self.assertRaises(FileTooLargeError):
            await ingest_uc.execute(filename="manual.pdf", content=b"12345")

    async def test_ingest_rejects_empty_extracted_content(self):
        ingest_uc = IngestProtocolFile(
            FakeDocumentExtractor(text="   "),
            FakeEmbeddingService(),
            FakeProtocolRetriever(),
        )
        with self.assertRaises(DocumentExtractionError):
            await ingest_uc.execute(filename="manual.pdf", content=b"pdf")

    async def test_ingest_rejects_inconsistent_embedding_dimensions(self):
        class BadEmbeddingService(FakeEmbeddingService):
            async def embed_batch(self, texts, task_type="RETRIEVAL_DOCUMENT"):
                return [[0.1] * (self.dim + 1) for _ in texts]

        ingest_uc = IngestProtocolFile(
            FakeDocumentExtractor(text="Contenido válido."),
            BadEmbeddingService(),
            FakeProtocolRetriever(),
        )
        with self.assertRaises(EmbeddingServiceError):
            await ingest_uc.execute(filename="manual.pdf", content=b"pdf")





if __name__ == "__main__":
    unittest.main()
