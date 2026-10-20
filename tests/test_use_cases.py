import unittest
from dataclasses import replace
from datetime import datetime, timezone

from application.use_cases import (
    CreateItem,
    DeleteEvent,
    GetEvent,
    ListEvents,
    ListItems,
    PublishEvent,
    UpdateEvent,
)
from domain.errors import EventNotFound
from domain.models import Item


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


class UseCaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.item = Item(
            id=1,
            name="demo",
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

        result = await CreateItem(repository, cache).execute("demo")

        self.assertEqual(result, self.item)
        self.assertEqual(repository.created_names, ["demo"])
        self.assertTrue(cache.invalidated)

    async def test_publish_event_stores_and_publishes(self):
        publisher = FakePublisher()
        repository = FakeEventsRepository()

        event_id = await PublishEvent(publisher, repository).execute("demo")

        self.assertEqual(len(publisher.events), 1)
        self.assertEqual(publisher.events[0].event_id, event_id)
        self.assertEqual(publisher.events[0].name, "demo")
        self.assertIn(event_id, repository.events)

    async def test_publish_event_removes_record_when_broker_fails(self):
        repository = FakeEventsRepository()

        with self.assertRaises(RuntimeError):
            await PublishEvent(FailingPublisher(), repository).execute("demo")

        self.assertEqual(repository.events, {})

    async def test_event_crud_use_cases(self):
        repository = FakeEventsRepository()
        event_id = await PublishEvent(FakePublisher(), repository).execute("demo")

        self.assertEqual((await GetEvent(repository).execute(event_id)).name, "demo")
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


if __name__ == "__main__":
    unittest.main()
