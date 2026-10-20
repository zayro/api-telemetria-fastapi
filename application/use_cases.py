import asyncio
import uuid
from dataclasses import dataclass

from application.ports import (
    EventPublisher,
    EventsRepository,
    ItemsCache,
    ItemsRepository,
)
from domain.errors import EventNotFound
from domain.models import Event, Item

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
