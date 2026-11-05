import unittest
from dataclasses import replace
from datetime import datetime, timezone

from application.use_cases import (
    CreateItem,
    DeleteEvent,
    GetEvent,
    GetProtocol,
    ListEvents,
    ListItems,
    ListProtocols,
    PublishEvent,
    QueryProtocolsRAG,
    UpdateEvent,
)
from domain.errors import EventNotFound, GeminiServiceError, ProtocolNotFound
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

    async def list_documents(self):
        return list(self.documents.values())

    async def get_document(self, document_id):
        return self.documents.get(document_id)

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




if __name__ == "__main__":
    unittest.main()
