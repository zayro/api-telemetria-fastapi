import json
import logging
from contextlib import contextmanager
from datetime import datetime

import aio_pika
from opentelemetry import propagate, trace
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from domain.models import Event, Item
from telemetry import structured_log

CACHE_KEY = "test:items"


@contextmanager
def logged_db_write(logger: logging.Logger, table: str, operation: str, **fields):
    """Registra db_write_ok / db_write_failed (con trace_id) para escrituras en Postgres."""
    span = trace.get_current_span()
    span.set_attribute("db.sql.table", table)
    span.set_attribute("db.operation", operation)
    try:
        yield
    except Exception as exc:
        structured_log(
            logger,
            "db_write_failed",
            level=logging.ERROR,
            table=table,
            operation=operation,
            error_type=type(exc).__name__,
            error=str(exc),
            **fields,
        )
        raise
    structured_log(logger, "db_write_ok", table=table, operation=operation, **fields)


class PostgresItemsRepository:
    def __init__(self, pool: AsyncConnectionPool, tracer: trace.Tracer, logger: logging.Logger):
        self._pool = pool
        self._tracer = tracer
        self._logger = logger

    async def create(self, name: str) -> Item:
        with self._tracer.start_as_current_span("test.db_insert") as span:
            span.set_attribute("db.system", "postgresql")
            with logged_db_write(self._logger, "test_items", "INSERT"):
                async with self._pool.connection() as conn:
                    cursor = await conn.execute(
                        "INSERT INTO test_items (name) VALUES (%s) "
                        "RETURNING id, name, created_at",
                        (name,),
                    )
                    row = await cursor.fetchone()
        return Item(id=row[0], name=row[1], created_at=row[2])

    async def list_recent(self, limit: int) -> list[Item]:
        with self._tracer.start_as_current_span("test.db_select") as span:
            span.set_attribute("db.system", "postgresql")
            async with self._pool.connection() as conn:
                cursor = await conn.execute(
                    "SELECT id, name, created_at FROM test_items "
                    "ORDER BY id DESC LIMIT %s",
                    (limit,),
                )
                rows = await cursor.fetchall()
        return [Item(id=row[0], name=row[1], created_at=row[2]) for row in rows]


class PostgresEventsRepository:
    _COLUMNS = "event_id, name, created_at, updated_at"

    def __init__(self, pool: AsyncConnectionPool, tracer: trace.Tracer, logger: logging.Logger):
        self._pool = pool
        self._tracer = tracer
        self._logger = logger

    @staticmethod
    def _to_event(row) -> Event:
        return Event(
            event_id=str(row[0]), name=row[1], created_at=row[2], updated_at=row[3]
        )

    async def _fetch(self, span_name: str, query: str, params=(), write=None):
        """`write=(operation, event_id)` activa el log de escritura para INSERT/UPDATE/DELETE."""
        with self._tracer.start_as_current_span(span_name) as span:
            span.set_attribute("db.system", "postgresql")
            if write is None:
                return await self._execute(query, params)
            operation, event_id = write
            with logged_db_write(self._logger, "test_events", operation, event_id=event_id):
                return await self._execute(query, params)

    async def _execute(self, query: str, params):
        async with self._pool.connection() as conn:
            cursor = await conn.execute(query, params)
            return await cursor.fetchall()

    async def create(self, event: Event) -> Event:
        rows = await self._fetch(
            "test.db_insert_event",
            "INSERT INTO test_events (event_id, name) VALUES (%s, %s) "
            f"RETURNING {self._COLUMNS}",
            (event.event_id, event.name),
            write=("INSERT", event.event_id),
        )
        return self._to_event(rows[0])

    async def get(self, event_id: str) -> Event | None:
        rows = await self._fetch(
            "test.db_select_event",
            f"SELECT {self._COLUMNS} FROM test_events WHERE event_id = %s",
            (event_id,),
        )
        return self._to_event(rows[0]) if rows else None

    async def list_recent(self, limit: int) -> list[Event]:
        rows = await self._fetch(
            "test.db_select_events",
            f"SELECT {self._COLUMNS} FROM test_events "
            "ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        return [self._to_event(row) for row in rows]

    async def update(self, event_id: str, name: str) -> Event | None:
        rows = await self._fetch(
            "test.db_update_event",
            "UPDATE test_events SET name = %s, updated_at = now() "
            f"WHERE event_id = %s RETURNING {self._COLUMNS}",
            (name, event_id),
            write=("UPDATE", event_id),
        )
        return self._to_event(rows[0]) if rows else None

    async def delete(self, event_id: str) -> bool:
        rows = await self._fetch(
            "test.db_delete_event",
            "DELETE FROM test_events WHERE event_id = %s RETURNING event_id",
            (event_id,),
            write=("DELETE", event_id),
        )
        return bool(rows)


class RedisItemsCache:
    def __init__(self, client: Redis, tracer: trace.Tracer):
        self._client = client
        self._tracer = tracer

    async def get(self) -> list[Item] | None:
        with self._tracer.start_as_current_span("test.cache_lookup") as span:
            span.set_attribute("db.system", "redis")
            cached = await self._client.get(CACHE_KEY)
        if cached is None:
            return None
        values = json.loads(cached)
        return [
            Item(
                id=value["id"],
                name=value["name"],
                created_at=datetime.fromisoformat(value["created_at"]),
            )
            for value in values
        ]

    async def set(self, items: list[Item], ttl_seconds: int) -> None:
        payload = json.dumps(
            [
                {
                    "id": item.id,
                    "name": item.name,
                    "created_at": item.created_at.isoformat(),
                }
                for item in items
            ],
            separators=(",", ":"),
        )
        await self._client.set(CACHE_KEY, payload, ex=ttl_seconds)

    async def invalidate(self) -> None:
        await self._client.delete(CACHE_KEY)


class RabbitMQEventPublisher:
    def __init__(
        self,
        channel: aio_pika.abc.AbstractChannel,
        queue_name: str,
        tracer: trace.Tracer,
    ):
        self._channel = channel
        self._queue_name = queue_name
        self._tracer = tracer

    async def publish(self, event: Event) -> None:
        payload = json.dumps(
            {"event_id": event.event_id, "name": event.name},
            separators=(",", ":"),
        ).encode()
        with self._tracer.start_as_current_span("test.rabbitmq_publish") as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute("messaging.destination.name", self._queue_name)
            headers = {}
            propagate.inject(headers)
            await self._channel.default_exchange.publish(
                aio_pika.Message(
                    body=payload,
                    headers=headers,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                ),
                routing_key=self._queue_name,
            )
