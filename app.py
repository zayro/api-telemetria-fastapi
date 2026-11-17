import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
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
    GetProtocol,
    IngestProtocolFile,
    ListEvents,
    ListItems,
    ListProtocols,
    PublishEvent,
    QueryProtocolsRAG,
    SimulateTest,
    UpdateEvent,
)
from infrastructure.adapters import (
    CorpusProtocolRetriever,
    GeminiEmbeddingAdapter,
    GeminiLLMAdapter,
    MultimodalDocumentExtractor,
    PostgresEventsRepository,
    PostgresItemsRepository,
    PostgresProtocolIndexRepository,
    KafkaEventPublisher,
    RedisItemsCache,
)
from telemetry import setup_telemetry, structured_log

SERVICE_NAME = os.getenv("SERVICE_NAME", "test-fastapi-service")
PORT = int(os.getenv("SERVICE_PORT", "8083"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/app")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test.events")
KAFKA_CONSUMER_ENABLED = os.getenv("KAFKA_CONSUMER_ENABLED", "false").lower() in (
    "1",
    "true",
    "yes",
)
KAFKA_CONSUMER_GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP_ID", f"{SERVICE_NAME}-consumer"
)
KAFKA_CONSUMER_TOPICS = [
    topic.strip()
    for topic in os.getenv("KAFKA_CONSUMER_TOPICS", KAFKA_TOPIC).split(",")
    if topic.strip()
] or [KAFKA_TOPIC]
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_EMBEDDING_MODEL = os.getenv(
    "GEMINI_EMBEDDING_MODEL", "gemini-embedding-001"
)
GEMINI_EMBEDDING_DIM = int(os.getenv("GEMINI_EMBEDDING_DIM", "768"))
PROTOCOLS_DIR = os.getenv("PROTOCOLS_DIR", "data/protocols")
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(20 * 1024 * 1024)))


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
    query_protocols_rag: QueryProtocolsRAG
    list_protocols: ListProtocols
    get_protocol: GetProtocol
    ingest_protocol_file: IngestProtocolFile


def create_app() -> FastAPI:
    settings = {
        "database_url": DATABASE_URL,
        "redis_url": REDIS_URL,
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "kafka_topic": KAFKA_TOPIC,
        "gemini_api_key": GEMINI_API_KEY,
        "gemini_model": GEMINI_MODEL,
        "gemini_embedding_model": GEMINI_EMBEDDING_MODEL,
        "gemini_embedding_dim": GEMINI_EMBEDDING_DIM,
        "protocols_dir": PROTOCOLS_DIR,
        "max_upload_bytes": MAX_UPLOAD_BYTES,
    }
    tracer, logger = setup_telemetry(SERVICE_NAME)

    async def _consume_kafka_messages(consumer: AIOKafkaConsumer) -> None:
        """Loop de consumo (apagado por defecto) para tenerlo listo cuando se necesite."""
        structured_log(
            logger,
            "kafka_consumer_started",
            topics=KAFKA_CONSUMER_TOPICS,
            group_id=KAFKA_CONSUMER_GROUP_ID,
        )
        try:
            async for message in consumer:
                # Placeholder: aquÃƒÂ­ normalmente se harÃƒÂ­a dispatch a un handler/use-case
                structured_log(
                    logger,
                    "kafka_message_received",
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            structured_log(
                logger,
                "kafka_consumer_failed",
                error=str(exc),
            )
            raise
        finally:
            structured_log(logger, "kafka_consumer_stopped")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Mantiene readiness desactivado hasta completar toda la inicialización.
        app.state.ready = False
        # Libera los recursos en orden inverso al registro, incluso si falla el arranque.
        async with AsyncExitStack() as stack:
            # Espera a que PostgreSQL tenga conexiones disponibles antes de continuar.
            pool = AsyncConnectionPool(settings["database_url"], open=False)
            await pool.open(wait=True)
            stack.push_async_callback(pool.close)

            # Crea las tablas de negocio si todavía no existen.
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

            # Comparte un cliente Redis; la conexión se establece al usarlo.
            redis_client = redis.from_url(settings["redis_url"], decode_responses=True)
            stack.push_async_callback(redis_client.aclose)

            # Inicia el productor Kafka con idempotencia y confirmación de todas las réplicas ISR.
            producer = AIOKafkaProducer(
                bootstrap_servers=settings["kafka_bootstrap_servers"].split(","),
                client_id=SERVICE_NAME,
                enable_idempotence=True,
                acks="all",
            )
            stack.push_async_callback(producer.stop)
            await producer.start()

            # El consumo en segundo plano solo se inicia si está habilitado por configuración.
            consumer_task: asyncio.Task | None = None
            consumer: AIOKafkaConsumer | None = None
            if KAFKA_CONSUMER_ENABLED:
                consumer = AIOKafkaConsumer(
                    *KAFKA_CONSUMER_TOPICS,
                    bootstrap_servers=settings["kafka_bootstrap_servers"].split(","),
                    client_id=f"{SERVICE_NAME}-consumer",
                    group_id=KAFKA_CONSUMER_GROUP_ID,
                    enable_auto_commit=True,
                    auto_offset_reset="earliest",
                )
                stack.push_async_callback(consumer.stop)
                await consumer.start()
                consumer_task = asyncio.create_task(
                    _consume_kafka_messages(consumer),
                    name=f"{SERVICE_NAME}-kafka-consumer",
                )

                async def _stop_consumer_task() -> None:
                    # Cancela y espera el bucle antes de cerrar el cliente consumidor.
                    if consumer_task is None:
                        return
                    consumer_task.cancel()
                    try:
                        await consumer_task
                    except asyncio.CancelledError:
                        pass

                stack.push_async_callback(_stop_consumer_task)
            # Usa un cliente administrativo independiente para comprobar Kafka en readiness.
            kafka_admin = AIOKafkaAdminClient(
                bootstrap_servers=settings["kafka_bootstrap_servers"].split(","),
                client_id=f"{SERVICE_NAME}-health",
            )
            stack.push_async_callback(kafka_admin.close)
            await kafka_admin.start()

            # Construye los adaptadores compartidos por los casos de uso.
            repository = PostgresItemsRepository(pool, tracer, logger)
            events_repository = PostgresEventsRepository(pool, tracer, logger)
            cache = RedisItemsCache(redis_client, tracer)
            publisher = KafkaEventPublisher(
                producer,
                settings["kafka_topic"],
                tracer,
            )
            # Configura la generación de respuestas, los embeddings y la extracción documental.
            gemini_client = GeminiLLMAdapter(
                api_key=settings["gemini_api_key"],
                model_name=settings["gemini_model"],
                tracer=tracer,
                logger=logger,
            )
            embedding_client = GeminiEmbeddingAdapter(
                api_key=settings["gemini_api_key"],
                model_name=settings["gemini_embedding_model"],
                output_dimensionality=settings["gemini_embedding_dim"],
                tracer=tracer,
                logger=logger,
            )
            extractor = MultimodalDocumentExtractor(gemini_client, logger=logger)
            index_repository = PostgresProtocolIndexRepository(
                pool, tracer=tracer, logger=logger
            )
            # Prepara la persistencia del índice antes de cargar el corpus de protocolos.
            await index_repository.ensure_schema()
            retriever = CorpusProtocolRetriever(
                protocols_dir=settings["protocols_dir"],
                tracer=tracer,
                embedding_service=embedding_client,
                extractor=extractor,
                index_repository=index_repository,
                logger=logger,
            )
            # Recupera el índice persistido e incorpora los documentos nuevos o modificados.
            await retriever.initialize()
            ingest_protocol_file = IngestProtocolFile(
                extractor=extractor,
                embedding_service=embedding_client,
                retriever=retriever,
                max_file_bytes=settings["max_upload_bytes"],
            )
            # Publica los casos de uso para que las dependencias HTTP puedan resolverlos.
            app.state.container = ServiceContainer(
                simulate_test=SimulateTest(),
                create_item=CreateItem(repository, cache),
                list_items=ListItems(repository, cache),
                publish_event=PublishEvent(publisher, events_repository),
                list_events=ListEvents(events_repository),
                get_event=GetEvent(events_repository),
                update_event=UpdateEvent(events_repository),
                delete_event=DeleteEvent(events_repository),
                query_protocols_rag=QueryProtocolsRAG(retriever, gemini_client),
                list_protocols=ListProtocols(retriever),
                get_protocol=GetProtocol(retriever),
                ingest_protocol_file=ingest_protocol_file,
            )
            # Expone los clientes que necesitan las comprobaciones de salud del servicio.
            app.state.pool = pool
            app.state.redis = redis_client
            app.state.kafka_producer = producer
            app.state.kafka_consumer = consumer
            app.state.kafka_admin = kafka_admin
            app.state.ready = True
            try:
                # Entrega el control a FastAPI mientras la aplicación atiende peticiones.
                yield
            finally:
                # Desactiva readiness antes de que AsyncExitStack cierre los recursos.
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
            topics = await asyncio.wait_for(
                app.state.kafka_admin.describe_topics([settings["kafka_topic"]]),
                timeout=3,
            )
            if not topics or topics[0].get("error_code") or not topics[0].get("partitions"):
                raise RuntimeError("Kafka topic unavailable")
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
