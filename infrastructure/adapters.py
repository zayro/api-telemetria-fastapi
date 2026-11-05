from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import math
import os
import re
import zlib
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import datetime
from pathlib import Path

try:
    import aio_pika
except ImportError:
    aio_pika = None

try:
    from opentelemetry import propagate, trace
except ImportError:
    propagate = None
    trace = None

try:
    from psycopg_pool import AsyncConnectionPool
except ImportError:
    AsyncConnectionPool = None

try:
    from redis.asyncio import Redis
except ImportError:
    Redis = None

from domain.errors import (
    DocumentExtractionError,
    EmbeddingServiceError,
    GeminiServiceError,
    UnsupportedFileTypeError,
)
from domain.models import (
    Event,
    Item,
    ProtocolDocument,
    ProtocolSearchResult,
)
try:
    from telemetry import structured_log
except (ImportError, ModuleNotFoundError):
    def structured_log(*args, **kwargs):
        pass


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


def _tokenize(text: str) -> list[str]:
    text = text.lower()
    for a, b in [("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"), ("ñ", "n")]:
        text = text.replace(a, b)
    return re.findall(r"\b[a-z0-9_]{2,}\b", text)


_DEFAULT_PROTOCOLS: list[ProtocolDocument] = [
    ProtocolDocument(
        id="proto-sec-inc-01",
        title="Protocolo de Gestión y Respuesta ante Incidentes de Seguridad",
        category="seguridad",
        source_file="protocolo_seguridad_incidentes.md",
        content=(
            "Fases obligatorias para la contención y resolución de incidentes:\n"
            "1. Identificación: Detección y triage del incidente en menos de 15 minutos (P1 Crítico a P4 Menor).\n"
            "2. Contención Inmediata: Aislamiento de red del contenedor/nodo afectado, revocación de tokens de sesión "
            "y rotación inmediata de credenciales y secretos comprometidos en el gestor de secretos.\n"
            "3. Erradicación: Eliminación de malware, remoción de accesos no autorizados y cierre de vectores de ataque.\n"
            "4. Recuperación: Restauración controlada de servicios mediante imágenes inmutables firmadas, monitoreo estricto durante 72 horas.\n"
            "5. Lecciones Aprendidas: Elaboración de informe post-mortem obligatorio en las primeras 48 horas tras el cierre del incidente."
        ),
        metadata={"version": "2.4", "classification": "confidencial", "urgency": "alta"},
    ),
    ProtocolDocument(
        id="proto-auth-oauth2-02",
        title="Protocolo de Autenticación, Autorización y Manejo de Tokens OAuth 2.0 y OIDC",
        category="seguridad",
        source_file="protocolo_oauth2_autenticacion.md",
        content=(
            "Lineamientos de identidad y control de acceso en la plataforma:\n"
            "- Flujo de autorización: Authorization Code Grant con PKCE (RFC 7636) mandatario en clientes web y móviles.\n"
            "- Access Tokens: Formato JWT con firma asimétrica RS256/EdDSA. Tiempo de vida máximo (TTL) de 15 minutos (900s). "
            "Claims requeridos: iss, sub, aud, exp, iat y roles de usuario.\n"
            "- Refresh Tokens: Rotación estricta de un solo uso (single-use). Su intercambio emite un nuevo refresh token "
            "e invalida inmediatamente el anterior. Expiración total a los 7 días.\n"
            "- Revocación: Uso del endpoint RFC 7009 (/oauth/revoke) ante logout o sospecha de filtración de credenciales."
        ),
        metadata={"version": "3.1", "standards": ["RFC 6749", "RFC 7636", "RFC 7009"]},
    ),
    ProtocolDocument(
        id="proto-ops-dr-03",
        title="Protocolo de Respaldo, Continuidad de Negocio y Disaster Recovery",
        category="operaciones",
        source_file="protocolo_respaldo_recuperacion.md",
        content=(
            "Políticas de respaldo y recuperación ante desastres tecnológicos:\n"
            "- Objetivos de recuperación: RPO (Recovery Point Objective) <= 1 hora; RTO (Recovery Time Objective) <= 4 horas.\n"
            "- Copias de seguridad: Respaldo continuo de logs WAL en PostgreSQL y snapshot diario completo a las 02:00 UTC con cifrado AES-256.\n"
            "- Almacenamiento seguro: Réplica secundaria en región geográfica independiente en almacenamiento de objetos.\n"
            "- Pruebas de restauración: Simulacros trimestrales obligatorios en ambiente aislado para verificar integridad y tiempos de levantamiento.\n"
            "- Conmutación (Failover): En caso de caída de zona primaria, activación del clúster secundario con re-enrutamiento DNS en menos de 15 minutos."
        ),
        metadata={"version": "1.8", "rpo_hours": 1, "rto_hours": 4},
    ),
    ProtocolDocument(
        id="proto-net-tls-04",
        title="Protocolo de Seguridad de Redes, Cifrado TLS y Certificados Digitales",
        category="redes",
        source_file="protocolo_comunicacion_tls_redes.md",
        content=(
            "Reglas de comunicación cifrada y transporte seguro de datos:\n"
            "- Versión mínima: TLS 1.3 mandataria para tráfico público externo. TLS 1.2 permitido solo como fallback legacy para clientes autorizados.\n"
            "- Microservicios y mTLS: Toda comunicación este-oeste dentro del clúster de servicios debe utilizar autenticación mutua (mTLS).\n"
            "- Certificados x509: Renovación automatizada cada 60 días mediante protocolo ACME. Alerta temprana preventiva a los 20 días previos a la expiración.\n"
            "- Puertos: HTTP plano en puerto 80 solo para redirección permanente (301) hacia HTTPS (puerto 443)."
        ),
        metadata={"version": "2.0", "tls_min": "1.3", "mtls": True},
    ),
    ProtocolDocument(
        id="proto-ops-cicd-05",
        title="Protocolo de Despliegue Continuo (CI/CD), Versionado Semántico y Rollback",
        category="operaciones",
        source_file="protocolo_despliegue_ci_cd.md",
        content=(
            "Flujo estándar para integración, entrega y reversión de versiones:\n"
            "- Versionado: Semantic Versioning (MAJOR.MINOR.PATCH) requerido en tags de repositorio e imágenes Docker.\n"
            "- Validación en CI: Ejecución de pruebas unitarias, análisis estático de seguridad y verificación de linters antes del empaquetado.\n"
            "- Estrategia de despliegue: Canary release progresivo (10% -> 50% -> 100%) monitoreando las sondas /health/live y /health/ready.\n"
            "- Criterios de Rollback: Reversión automática a la versión estable anterior si la tasa de fallos 5xx excede el 1% o la latencia p95 supera los 2000 ms durante los primeros 10 minutos."
        ),
        metadata={"version": "3.0", "deployment_strategy": "canary", "auto_rollback": True},
    ),
]


_TEXT_EXTENSIONS = {"md", "markdown", "txt", "json", "csv", "html", "htm"}
_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp"}
_SUPPORTED_EXTENSIONS = _TEXT_EXTENSIONS | _IMAGE_EXTENSIONS | {"pdf"}
_MIME_BY_EXTENSION = {
    "pdf": "application/pdf",
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
    "md": "text/markdown",
    "markdown": "text/markdown",
    "txt": "text/plain",
    "json": "application/json",
    "csv": "text/csv",
    "html": "text/html",
    "htm": "text/html",
}
_MIN_PDF_TEXT_CHARS = 40
_MAX_GEMINI_INLINE_BYTES = 18 * 1024 * 1024


class MultimodalDocumentExtractor:
    """Extractor para documentos de texto, PDF e imágenes.

    Los PDF con capa de texto se procesan localmente con pypdf. Los PDF
    escaneados y las imágenes requieren Gemini para transcribir/describir su
    contenido y así poder indexarlo en el RAG.
    """

    def __init__(self, gemini_client=None, logger: logging.Logger | None = None):
        self._gemini = gemini_client
        self._logger = logger

    async def extract_content(
        self, filename: str, content: bytes, mime_type: str | None = None
    ) -> tuple[str, str, str, dict]:
        """Extrae (título, texto_extraído, doc_type, metadatos) de un archivo."""
        if not content:
            raise DocumentExtractionError(f"El archivo '{filename}' está vacío.")

        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        ext = ext or self._extension_from_mime(mime_type)
        if ext not in _SUPPORTED_EXTENSIONS:
            raise UnsupportedFileTypeError(
                f"Formato no soportado para '{filename}'. Formatos permitidos: "
                f"{', '.join(sorted(_SUPPORTED_EXTENSIONS))}."
            )

        self._validate_signature(ext, content)
        actual_mime = mime_type or _MIME_BY_EXTENSION.get(ext, "application/octet-stream")
        title = filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " ").title()

        # 1. Archivos PDF
        if ext == "pdf":
            text, pages, method = await self._extract_pdf(content)
            if len(text.strip()) < _MIN_PDF_TEXT_CHARS:
                if self._has_gemini():
                    try:
                        text = await self._extract_with_gemini(
                            filename,
                            content,
                            "application/pdf",
                            "Transcribe literalmente todo el texto del PDF. "
                            "Describe también tablas, diagramas, flujos y cualquier "
                            "información visual relevante. Devuelve Markdown estructurado.",
                        )
                        method = "gemini-pdf"
                    except Exception as exc:
                        raise DocumentExtractionError(
                            f"El PDF '{filename}' no tiene capa de texto y Gemini no "
                            f"pudo procesarlo: {exc}"
                        ) from exc
                else:
                    raise DocumentExtractionError(
                        f"El PDF '{filename}' no contiene texto seleccionable. "
                        "Configure GEMINI_API_KEY para usar visión multimodal o "
                        "aplique OCR antes de subirlo."
                    )

            text = text.strip()
            if not text:
                raise DocumentExtractionError(
                    f"No se pudo extraer contenido del PDF '{filename}'."
                )

            first_line = text.split("\n", 1)[0].strip()
            if first_line and len(first_line) < 120 and not first_line.startswith("%"):
                clean_first = first_line.replace("#", "").strip()
                if clean_first:
                    title = clean_first

            return title, text, "pdf", {
                "pages": pages,
                "file_size": len(content),
                "mime_type": actual_mime,
                "extraction_method": method,
            }

        # 2. Archivos de imagen (PNG, JPG, JPEG, WEBP)
        if ext in _IMAGE_EXTENSIONS:
            if not self._has_gemini():
                raise DocumentExtractionError(
                    f"La imagen '{filename}' requiere GEMINI_API_KEY para extraer "
                    "su contenido y poder vectorizarla en el RAG."
                )
            try:
                description = await self._extract_with_gemini(
                    filename,
                    content,
                    actual_mime,
                    "Eres un analizador técnico de arquitectura y protocolos. "
                    "Describe detalladamente esta imagen o diagrama: extrae todo el "
                    "texto visible, componentes, flechas de flujo, certificados, "
                    "puertos y políticas. Devuelve Markdown estructurado e indexable.",
                )
            except Exception as exc:
                raise DocumentExtractionError(
                    f"No se pudo procesar la imagen '{filename}' con Gemini: {exc}"
                ) from exc

            if not description.strip():
                raise DocumentExtractionError(
                    f"Gemini no devolvió contenido para la imagen '{filename}'."
                )
            return title, description.strip(), "image", {
                "file_size": len(content),
                "mime_type": actual_mime,
                "extraction_method": "gemini-image",
            }

        # 3. Documentos Markdown / Texto / JSON / CSV / HTML
        if b"\x00" in content[:4096]:
            raise UnsupportedFileTypeError(
                f"El archivo '{filename}' parece ser binario y no un documento de texto."
            )
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")

        if ext in {"html", "htm"}:
            text = re.sub(r"(?is)<(script|style).*?</\1>", " ", text)
            text = re.sub(r"(?s)<[^>]+>", " ", text)
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n\s*\n+", "\n\n", text)

        doc_type = {
            "md": "markdown",
            "markdown": "markdown",
            "json": "json",
            "csv": "csv",
            "html": "html",
            "htm": "html",
        }.get(ext, "text")

        first_h1 = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
        if first_h1:
            title = first_h1.group(1).strip()

        return title, text.strip(), doc_type, {
            "file_size": len(content),
            "mime_type": actual_mime,
            "extraction_method": "local-text",
        }

    @staticmethod
    def _extension_from_mime(mime_type: str | None) -> str:
        if not mime_type:
            return ""
        for ext, mime in _MIME_BY_EXTENSION.items():
            if mime == mime_type:
                return ext
        if mime_type.startswith("image/"):
            return mime_type.split("/", 1)[1].replace("jpeg", "jpg")
        return ""

    @staticmethod
    def _validate_signature(ext: str, content: bytes) -> None:
        if ext == "pdf":
            if b"%PDF-" not in content[:1024]:
                raise UnsupportedFileTypeError(
                    "El archivo tiene extensión .pdf pero no es un PDF válido."
                )
            return
        if ext == "webp":
            if not (content[:4] == b"RIFF" and content[8:12] == b"WEBP"):
                raise UnsupportedFileTypeError(
                    "El archivo tiene extensión .webp pero no es una imagen WEBP válida."
                )
            return
        signatures = {
            "png": (b"\x89PNG\r\n\x1a\n",),
            "jpg": (b"\xff\xd8\xff",),
            "jpeg": (b"\xff\xd8\xff",),
        }
        expected = signatures.get(ext)
        if expected and not any(content.startswith(sig) for sig in expected):
            raise UnsupportedFileTypeError(
                f"El archivo tiene extensión .{ext} pero su contenido no coincide "
                "con ese formato."
            )

    def _has_gemini(self) -> bool:
        return bool(self._gemini and getattr(self._gemini, "has_api_key", False))

    async def _extract_pdf(self, content: bytes) -> tuple[str, int, str]:
        """Extrae texto del PDF sin bloquear el event loop."""
        return await asyncio.to_thread(self._extract_pdf_sync, content)

    def _extract_pdf_sync(self, content: bytes) -> tuple[str, int, str]:
        """Extrae texto de un PDF usando pypdf y, si falla, los streams del PDF."""
        page_count = max(1, len(re.findall(rb"/Type\s*/Page\b", content)))

        try:
            import pypdf

            reader = pypdf.PdfReader(io.BytesIO(content))
            page_count = max(1, len(reader.pages))
            pages_text = []
            for page in reader.pages:
                extracted = page.extract_text()
                if extracted and extracted.strip():
                    pages_text.append(extracted.strip())
            joined = "\n\n".join(pages_text).strip()
            if self._looks_like_text(joined):
                return joined, page_count, "pypdf"
        except Exception as exc:
            if self._logger:
                structured_log(
                    self._logger,
                    "pdf_pypdf_extraction_failed",
                    level=logging.WARNING,
                    error=str(exc),
                )

        # Fallback a extracción directa de streams PDF (PDFs simples).
        text_parts: list[str] = []
        for stream_match in re.finditer(
            rb"stream\r?\n(.*?)\r?\nendstream", content, re.DOTALL
        ):
            stream_data = stream_match.group(1)
            try:
                # Limitar la salida para evitar bombas de descompresión.
                decompressor = zlib.decompressobj()
                decompressed = decompressor.decompress(stream_data, 5_000_000)
            except Exception:
                decompressed = stream_data

            for m in re.findall(rb"\((.*?)\)\s*Tj", decompressed):
                text_parts.append(m.decode("utf-8", errors="ignore"))
            for tj_arr in re.finditer(rb"\[(.*?)\]\s*TJ", decompressed):
                for part in re.findall(rb"\((.*?)\)", tj_arr.group(1)):
                    text_parts.append(part.decode("utf-8", errors="ignore"))

        joined = "\n".join(text_parts).strip()
        if self._looks_like_text(joined):
            return joined, page_count, "pdf-streams"

        return "", page_count, "none"

    @staticmethod
    def _looks_like_text(text: str) -> bool:
        if not text:
            return False
        sample = text[:4000]
        printable = sum(
            1 for ch in sample if ch.isprintable() or ch in "\n\r\t"
        )
        return printable / max(1, len(sample)) >= 0.7

    async def _extract_with_gemini(
        self, filename: str, content: bytes, mime_type: str, prompt: str
    ) -> str:
        if len(content) > _MAX_GEMINI_INLINE_BYTES:
            raise DocumentExtractionError(
                f"El archivo '{filename}' es demasiado grande para enviarlo a "
                "Gemini (límite 18 MB). Divídalo o aplique OCR local."
            )
        b64_data = base64.b64encode(content).decode("utf-8")
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "inlineData": {
                                "mimeType": mime_type,
                                "data": b64_data,
                            }
                        },
                        {"text": prompt},
                    ]
                }
            ],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 4096},
        }
        return await self._gemini.generate_with_payload(payload)

class GeminiEmbeddingAdapter:
    """Embeddings con Gemini (`gemini-embedding-001`) y fallback local determinista.

    El fallback se usa únicamente cuando no hay API key configurada. Si la API
    está configurada y falla, se lanza ``EmbeddingServiceError`` para no mezclar
    vectores reales con vectores locales en el mismo índice.
    """

    _LOCAL_DIMENSION = 128
    _LOCAL_MODEL = "local-hash-128"
    _MAX_BATCH_SIZE = 100

    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-embedding-001",
        output_dimensionality: int = 768,
        tracer: trace.Tracer | None = None,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        logger: logging.Logger | None = None,
    ):
        self._api_key = api_key
        self._model_name = model_name or "gemini-embedding-001"
        self._output_dimensionality = int(output_dimensionality or 768)
        if not 128 <= self._output_dimensionality <= 3072:
            raise ValueError(
                "output_dimensionality debe estar entre 128 y 3072."
            )
        self._tracer = tracer
        self._timeout_seconds = timeout_seconds
        self._max_retries = max(0, int(max_retries))
        self._logger = logger

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key)

    @property
    def model_name(self) -> str:
        return self._model_name if self._api_key else self._LOCAL_MODEL

    @property
    def dimension(self) -> int:
        return self._output_dimensionality if self._api_key else self._LOCAL_DIMENSION

    async def embed_text(
        self, text: str, task_type: str = "RETRIEVAL_QUERY"
    ) -> list[float]:
        results = await self.embed_batch([text], task_type=task_type)
        if not results:
            raise EmbeddingServiceError("No se pudo generar el embedding de la consulta.")
        return results[0]

    async def embed_batch(
        self, texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT"
    ) -> list[list[float]]:
        if not texts:
            return []
        cleaned = [t if t and t.strip() else " " for t in texts]
        if not self._api_key:
            return [self._pseudo_embedding(t) for t in cleaned]

        embeddings: list[list[float]] = []
        for start in range(0, len(cleaned), self._MAX_BATCH_SIZE):
            batch = cleaned[start : start + self._MAX_BATCH_SIZE]
            embeddings.extend(await self._request_batch(batch, task_type))
        return embeddings

    async def _request_batch(
        self, texts: list[str], task_type: str
    ) -> list[list[float]]:
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self._model_name}:batchEmbedContents?key={self._api_key}"
        )
        supports_dimensions = self._model_name.startswith("gemini-embedding")
        requests_payload = []
        for text in texts:
            request = {
                "model": f"models/{self._model_name}",
                "content": {"parts": [{"text": text[:8000]}]},
                "taskType": task_type,
            }
            if supports_dimensions:
                request["outputDimensionality"] = self._output_dimensionality
            requests_payload.append(request)

        payload = {"requests": requests_payload}
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                status_code, response_text = await self._post_json(url, payload)
            except Exception as exc:
                last_error = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(2**attempt)
                    continue
                raise EmbeddingServiceError(
                    f"No se pudo conectar con el servicio de embeddings: {exc}"
                ) from exc

            if status_code == 200:
                try:
                    return self._parse_embeddings(response_text, len(texts))
                except Exception as exc:
                    raise EmbeddingServiceError(
                        f"Respuesta inválida del servicio de embeddings: {exc}"
                    ) from exc

            error_message = self._extract_error_message(response_text)
            if status_code in {429, 500, 502, 503, 504} and attempt < self._max_retries:
                if self._logger:
                    structured_log(
                        self._logger,
                        "embedding_retry",
                        level=logging.WARNING,
                        status_code=status_code,
                        attempt=attempt + 1,
                        error=error_message,
                    )
                await asyncio.sleep(2**attempt)
                continue

            raise EmbeddingServiceError(
                f"Gemini embeddings devolvió código {status_code}: {error_message}"
            )

        raise EmbeddingServiceError(
            f"No se pudieron generar embeddings: {last_error or 'error desconocido'}"
        )

    async def _post_json(self, url: str, payload: dict) -> tuple[int, str]:
        data_bytes = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        try:
            import httpx

            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.post(url, json=payload, headers=headers)
                return response.status_code, response.text
        except ImportError:
            import urllib.error
            import urllib.request

            def _sync():
                request = urllib.request.Request(
                    url, data=data_bytes, headers=headers, method="POST"
                )
                try:
                    with urllib.request.urlopen(
                        request, timeout=self._timeout_seconds
                    ) as response:
                        return response.getcode(), response.read().decode("utf-8")
                except urllib.error.HTTPError as exc:
                    return exc.code, exc.read().decode("utf-8")

            return await asyncio.to_thread(_sync)

    def _parse_embeddings(
        self, response_text: str, expected: int
    ) -> list[list[float]]:
        data = json.loads(response_text)
        items = data.get("embeddings", [])
        if len(items) != expected:
            raise ValueError(
                f"se esperaban {expected} embeddings y llegaron {len(items)}"
            )
        vectors = []
        for item in items:
            values = item.get("values") or []
            if not values:
                raise ValueError("embedding vacío")
            vectors.append(self._normalize([float(v) for v in values]))
        return vectors

    @staticmethod
    def _extract_error_message(response_text: str) -> str:
        try:
            payload = json.loads(response_text)
            error = payload.get("error", {})
            if isinstance(error, dict) and error.get("message"):
                return str(error["message"])
        except Exception:
            pass
        return response_text[:500]

    @staticmethod
    def _normalize(vec: list[float]) -> list[float]:
        norm = math.sqrt(sum(x * x for x in vec))
        if norm < 1e-9:
            return vec
        return [x / norm for x in vec]

    def _pseudo_embedding(self, text: str, dim: int | None = None) -> list[float]:
        """Embedding local determinista; nunca usa ``hash()`` (aleatorio por proceso)."""
        dimension = dim or self._LOCAL_DIMENSION
        tokens = _tokenize(text)
        vec = [0.0] * dimension
        if not tokens:
            vec[0] = 1.0
            return vec

        for i, token in enumerate(tokens):
            digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
            h = int.from_bytes(digest, "big")
            vec[h % dimension] += (1.0 if (h >> 3) & 1 else -1.0) / (1.0 + 0.1 * i)

            for j in range(len(token) - 2):
                n_digest = hashlib.blake2b(
                    token[j : j + 3].encode("utf-8"), digest_size=8
                ).digest()
                n_hash = int.from_bytes(n_digest, "big")
                vec[n_hash % dimension] += 0.5 * (1.0 if (n_hash & 1) else -1.0)

        return self._normalize(vec)


class CorpusProtocolRetriever:
    """Recuperador RAG sobre un corpus de protocolos (archivos locales y documentos en memoria)."""

    def __init__(
        self,
        protocols_dir: str | Path | None = None,
        tracer: trace.Tracer | None = None,
    ):
        self._protocols_dir = Path(protocols_dir) if protocols_dir else None
        self._tracer = tracer
        self._documents: dict[str, ProtocolDocument] = {}
        self._load_corpus()

    def _load_corpus(self) -> None:
        # 1. Cargar protocolos predeterminados
        for doc in _DEFAULT_PROTOCOLS:
            self._documents[doc.id] = doc

        # 2. Cargar archivos del directorio configurado (si existe)
        if self._protocols_dir and self._protocols_dir.exists():
            for file_path in self._protocols_dir.glob("**/*"):
                if file_path.is_file() and file_path.suffix.lower() in {".md", ".txt", ".json"}:
                    self._index_file(file_path)

    def _index_file(self, file_path: Path) -> None:
        try:
            content = file_path.read_text(encoding="utf-8")
        except Exception:
            return

        doc_id = f"file-{file_path.stem}"
        title = file_path.stem.replace("_", " ").replace("-", " ").title()
        category = "general"

        # Detección simple de categoría por carpeta o nombre
        parent_name = file_path.parent.name.lower()
        if parent_name in {"seguridad", "redes", "operaciones", "datos", "sistemas"}:
            category = parent_name
        elif "seguridad" in file_path.name.lower():
            category = "seguridad"
        elif "red" in file_path.name.lower() or "tls" in file_path.name.lower():
            category = "redes"
        elif "respaldo" in file_path.name.lower() or "cicd" in file_path.name.lower() or "ops" in file_path.name.lower():
            category = "operaciones"

        # Extraer primer título H1 en markdown si existe
        first_h1 = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if first_h1:
            title = first_h1.group(1).strip()

        self._documents[doc_id] = ProtocolDocument(
            id=doc_id,
            title=title,
            content=content.strip(),
            source_file=file_path.name,
            category=category,
            metadata={"path": str(file_path)},
        )

    async def list_documents(self) -> list[ProtocolDocument]:
        return list(self._documents.values())

    async def get_document(self, document_id: str) -> ProtocolDocument | None:
        return self._documents.get(document_id)

    async def search(
        self, query: str, limit: int = 5, category: str | None = None
    ) -> list[ProtocolSearchResult]:
        span_ctx = (
            self._tracer.start_as_current_span("test.rag_search")
            if self._tracer
            else nullcontext()
        )
        with span_ctx as span:
            if span and hasattr(span, "set_attribute"):
                span.set_attribute("rag.query", query)
                span.set_attribute("rag.top_k", limit)
                if category:
                    span.set_attribute("rag.filter.category", category)

            q_tokens = _tokenize(query)
            clean_query = " ".join(q_tokens)
            docs = list(self._documents.values())

            if category:
                docs = [d for d in docs if d.category.lower() == category.lower()]

            if not docs:
                return []

            if not q_tokens:
                # Si no hay tokens en la query, devolver los primeros limit
                return [ProtocolSearchResult(document=d, score=1.0) for d in docs[:limit]]

            # Cálculo de BM25
            n_docs = len(docs)
            doc_tokens = {d.id: _tokenize(d.title + " " + d.content) for d in docs}
            avg_dl = sum(len(toks) for toks in doc_tokens.values()) / max(1, n_docs)

            # Document frequency para cada término de la consulta
            df: dict[str, int] = {}
            for t in set(q_tokens):
                df[t] = sum(1 for d in docs if t in doc_tokens[d.id])

            k1 = 1.5
            b = 0.75
            scored: list[tuple[float, ProtocolDocument]] = []

            for doc in docs:
                tokens = doc_tokens[doc.id]
                dl = len(tokens)
                score = 0.0

                for t in q_tokens:
                    doc_freq = df.get(t, 0)
                    if doc_freq == 0:
                        continue
                    idf = math.log(1.0 + (n_docs - doc_freq + 0.5) / (doc_freq + 0.5))
                    tf = tokens.count(t)
                    if tf == 0:
                        continue

                    # Ponderación BM25
                    term_score = idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avg_dl)))

                    # Bonificación si el término aparece en el título
                    if t in _tokenize(doc.title):
                        term_score *= 2.0

                    score += term_score

                # Bonificación por coincidencia de frase exacta en el texto
                doc_text_clean = " ".join(tokens)
                if clean_query and clean_query in doc_text_clean:
                    score += 2.5

                if score > 0.0:
                    scored.append((score, doc))

            if not scored:
                # Fallback: si ningún término coincidió exactamente, devolver los más recientes con score mínimo
                return [ProtocolSearchResult(document=d, score=0.1) for d in docs[:limit]]

            # Ordenar por puntuación descendente
            scored.sort(key=lambda x: x[0], reverse=True)
            max_score = scored[0][0]

            results = []
            for raw_score, doc in scored[:limit]:
                # Normalizar a una escala entre 0.0 y 1.0
                normalized = round(min(1.0, raw_score / max(1e-6, max_score)), 4)
                results.append(ProtocolSearchResult(document=doc, score=normalized))

            if span and hasattr(span, "set_attribute"):
                span.set_attribute("rag.results_count", len(results))

            return results


class GeminiLLMAdapter:
    """Adaptador de infraestructura para conectar con modelos LLM de Google Gemini vía REST API."""

    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-2.5-flash",
        tracer: trace.Tracer | None = None,
        logger: logging.Logger | None = None,
        timeout_seconds: float = 30.0,
    ):
        self._api_key = api_key
        self._model_name = model_name or "gemini-2.5-flash"
        self._tracer = tracer
        self._logger = logger
        self._timeout_seconds = timeout_seconds

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key)

    async def generate_with_payload(self, payload: dict) -> str:
        if not self._api_key:
            raise GeminiServiceError("GEMINI_API_KEY no está configurada.")
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self._model_name}:generateContent?key={self._api_key}"
        )
        return await self._call_gemini_api(url, payload)

    async def generate_response(
        self, prompt: str, context: list[str], temperature: float = 0.2
    ) -> str:
        if not self._api_key:
            raise GeminiServiceError(
                "GEMINI_API_KEY no está configurada. Configure la variable de entorno para habilitar la generación de respuestas con Gemini."
            )

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self._model_name}:generateContent?key={self._api_key}"
        )
        payload = {
            "contents": [
                {
                    "parts": [{"text": prompt}]
                }
            ],
            "generationConfig": {
                "temperature": max(0.0, min(1.0, temperature)),
                "maxOutputTokens": 2048,
            },
        }

        span_ctx = (
            self._tracer.start_as_current_span("test.gemini_generate_content")
            if self._tracer
            else nullcontext()
        )

        with span_ctx as span:
            if span and hasattr(span, "set_attribute"):
                span.set_attribute("gen_ai.system", "gemini")
                span.set_attribute("gen_ai.request.model", self._model_name)
                span.set_attribute("gen_ai.request.temperature", temperature)

            return await self._call_gemini_api(url, payload)

    async def _call_gemini_api(self, url: str, payload: dict) -> str:
        data_bytes = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}

        # Intento con httpx asíncrono si está disponible
        try:
            import httpx

            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.post(url, json=payload, headers=headers)
                status_code = response.status_code
                response_text = response.text
        except ImportError:
            # Fallback a urllib estándar ejecutado en un hilo de trabajo
            import urllib.error
            import urllib.request

            def _sync_request():
                req = urllib.request.Request(
                    url, data=data_bytes, headers=headers, method="POST"
                )
                try:
                    with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                        return resp.getcode(), resp.read().decode("utf-8")
                except urllib.error.HTTPError as err:
                    return err.code, err.read().decode("utf-8")
                except urllib.error.URLError as err:
                    raise GeminiServiceError(
                        f"Error de red al conectar con Gemini API: {err.reason}"
                    )

            status_code, response_text = await asyncio.to_thread(_sync_request)

        if status_code != 200:
            error_msg = response_text
            try:
                err_json = json.loads(response_text)
                if "error" in err_json and "message" in err_json["error"]:
                    error_msg = err_json["error"]["message"]
            except Exception:
                pass
            raise GeminiServiceError(
                f"Gemini API devolvió código {status_code}: {error_msg}",
                status_code=status_code,
            )

        try:
            resp_data = json.loads(response_text)
            candidates = resp_data.get("candidates", [])
            if not candidates:
                feedback = resp_data.get("promptFeedback", {})
                block_reason = feedback.get("blockReason")
                if block_reason:
                    raise GeminiServiceError(
                        f"Contenido bloqueado por filtros de seguridad de Gemini: {block_reason}"
                    )
                return "No se generó respuesta para la consulta."

            parts = candidates[0].get("content", {}).get("parts", [])
            generated_text = "".join(part.get("text", "") for part in parts)
            return generated_text.strip()
        except json.JSONDecodeError as exc:
            raise GeminiServiceError(f"Respuesta JSON inválida de Gemini: {exc}")

