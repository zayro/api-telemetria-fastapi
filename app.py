import asyncio
import logging
import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

import aio_pika
import redis.asyncio as redis
import uvicorn
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg_pool import AsyncConnectionPool
from starlette.responses import JSONResponse, Response

from api.router import router
from app_metrics import DURATION, REQUESTS
from application.use_cases import (
    CreateItem,
    DeleteEvent,
    GetEvent,
    ListEvents,
    ListItems,
    PublishEvent,
    SimulateTest,
    UpdateEvent,
)
from infrastructure.adapters import (
    PostgresEventsRepository,
    PostgresItemsRepository,
    RabbitMQEventPublisher,
    RedisItemsCache,
)
from telemetry import setup_telemetry, structured_log

SERVICE_NAME = os.getenv("SERVICE_NAME", "test-fastapi-service")
PORT = int(os.getenv("SERVICE_PORT", "8083"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@postgres:5432/demo")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://demo:demo@rabbitmq:5672/")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "test.events")


@dataclass
class ServiceContainer:
    simulate_test: SimulateTest
    create_item: CreateItem
    list_items: ListItems
    publish_event: PublishEvent
    list_events: ListEvents
    get_event: GetEvent
    update_event: UpdateEvent
    delete_event: DeleteEvent

async def connect_rabbitmq(url: str, logger: logging.Logger) -> aio_pika.RobustConnection:
    while True:
        try:
            return await aio_pika.connect_robust(url)
        except aio_pika.exceptions.AMQPConnectionError as exc:
            structured_log(logger, "rabbitmq_connect_retry", error=str(exc))
            await asyncio.sleep(2)


def create_app() -> FastAPI:
    settings = {
        "database_url": DATABASE_URL,
        "redis_url": REDIS_URL,
        "rabbitmq_url": RABBITMQ_URL,
        "rabbitmq_queue": RABBITMQ_QUEUE,
    }
    tracer, logger = setup_telemetry(SERVICE_NAME)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.ready = False
        async with AsyncExitStack() as stack:
            pool = AsyncConnectionPool(settings["database_url"], open=False)
            await pool.open(wait=True)
            stack.push_async_callback(pool.close)

            async with pool.connection() as conn:
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS test_items ("
                    "id SERIAL PRIMARY KEY, "
                    "name TEXT NOT NULL, "
                    "created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
                )
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS test_events ("
                    "event_id UUID PRIMARY KEY, "
                    "name TEXT NOT NULL, "
                    "created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                    "updated_at TIMESTAMPTZ)"
                )

            redis_client = redis.from_url(settings["redis_url"], decode_responses=True)
            stack.push_async_callback(redis_client.aclose)

            rabbit_connection = await connect_rabbitmq(settings["rabbitmq_url"], logger)
            stack.push_async_callback(rabbit_connection.close)
            rabbit_channel = await rabbit_connection.channel(publisher_confirms=True)
            await rabbit_channel.declare_queue(settings["rabbitmq_queue"], durable=True)

            repository = PostgresItemsRepository(pool, tracer, logger)
            events_repository = PostgresEventsRepository(pool, tracer, logger)
            cache = RedisItemsCache(redis_client, tracer)
            publisher = RabbitMQEventPublisher(
                rabbit_channel,
                settings["rabbitmq_queue"],
                tracer,
            )
            app.state.container = ServiceContainer(
                simulate_test=SimulateTest(),
                create_item=CreateItem(repository, cache),
                list_items=ListItems(repository, cache),
                publish_event=PublishEvent(publisher, events_repository),
                list_events=ListEvents(events_repository),
                get_event=GetEvent(events_repository),
                update_event=UpdateEvent(events_repository),
                delete_event=DeleteEvent(events_repository),
            )
            app.state.pool = pool
            app.state.redis = redis_client
            app.state.rabbit_connection = rabbit_connection
            app.state.ready = True
            try:
                yield
            finally:
                app.state.ready = False

    app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
    app.state.ready = False
    app.state.logger = logger
    app.include_router(router)
    FastAPIInstrumentor.instrument_app(app, excluded_urls="metrics,health")

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health/live", include_in_schema=False)
    def live():
        return {"status": "UP", "service": SERVICE_NAME}

    @app.get("/health/ready", include_in_schema=False)
    async def ready():
        if not app.state.ready:
            return JSONResponse(
                status_code=503,
                content={"status": "DOWN", "service": SERVICE_NAME},
            )

        try:
            async with app.state.pool.connection() as conn:
                await conn.execute("SELECT 1")
            await app.state.redis.ping()
            if app.state.rabbit_connection.is_closed:
                raise RuntimeError("RabbitMQ connection is closed")
        except Exception as exc:
            structured_log(logger, "readiness_check_failed", error=str(exc))
            return JSONResponse(
                status_code=503,
                content={"status": "DOWN", "service": SERVICE_NAME},
            )
        return {"status": "UP", "service": SERVICE_NAME}

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
