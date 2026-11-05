# Servicio de prueba FastAPI

Servicio HTTP del app para probar observabilidad, persistencia, cache y
publicacion de eventos. Se expone hacia los clientes unicamente a traves de
Traefik; dentro de Docker escucha en el puerto `8083`.

## Arquitectura

La implementacion separa el transporte HTTP de las reglas de aplicacion y de
los detalles de infraestructura:

- `api/`: rutas FastAPI y validacion de los cuerpos de entrada.
- `application/`: casos de uso y contratos (puertos) requeridos por la
  aplicacion; no dependen de FastAPI, PostgreSQL, Redis ni RabbitMQ.
- `domain/`: modelos de negocio independientes del framework.
- `infrastructure/`: adaptadores concretos para PostgreSQL, Redis y RabbitMQ.
- `app.py`: composition root; configura telemetria, recursos, dependencias y
  ciclo de vida del servicio.
- `app_metrics.py` y `telemetry.py`: metricas Prometheus y telemetria
  OpenTelemetry.

El flujo de dependencias apunta hacia el centro: la API invoca casos de uso,
los casos de uso dependen de contratos y la infraestructura implementa esos
contratos. Durante el inicio se crean el pool y las conexiones; durante el
cierre se liberan incluso si el arranque falla a mitad.

## Endpoints

Las rutas de negocio conservan sus contratos:

- `GET /api/test?delay_ms=100&fail=0`: respuesta `pong`; `delay_ms` acepta
  valores de 0 a 2000 ms y `fail=1` genera un error simulado para probar
  trazas, logs y metricas.
- `POST /api/test/items` con `{"name":"app"}`: crea un elemento en PostgreSQL,
  invalida el cache y responde `201`.
- `GET /api/test/items`: consulta los ultimos 20 elementos usando cache-aside
  en Redis, con TTL de 30 segundos.
- `POST /api/test/events` con `{"name":"app"}`: registra el evento en la tabla
  `test_events` de PostgreSQL, lo publica en la cola RabbitMQ configurada y
  responde `202` con `event_id`. Si la publicacion falla, el registro se elimina.
- `GET /api/test/events`: lista los 20 eventos mas recientes.
- `GET /api/test/events/{event_id}`: obtiene un evento (`404` si no existe;
  `422` si el id no es un UUID).
- `PUT /api/test/events/{event_id}` con `{"name":"nuevo"}`: actualiza el nombre
  y `updated_at`. No vuelve a publicar el mensaje.
- `DELETE /api/test/events/{event_id}`: elimina el registro y responde `204`.
  Los mensajes ya publicados en RabbitMQ no se retiran.
- `POST /api/test/protocols/rag`: consulta RAG sobre el corpus de protocolos con integración al LLM de Google Gemini. Permite buscar fragmentos relevantes en los archivos de protocolos y generar una respuesta contextualizada y fundamentada.
  - Body de ejemplo:
    ```json
    {
      "query": "¿Cuáles son los pasos obligatorios ante una fuga de datos según el protocolo?",
      "top_k": 3,
      "category": "seguridad",
      "generate_answer": true,
      "temperature": 0.2
    }
    ```
  - Si `generate_answer: false`, realiza únicamente la recuperación semántica (RAG retrieval) sin invocar al LLM.
- `POST /api/test/protocols/upload`: recibe un archivo multipart (`file`) y una
  `category` opcional, extrae su contenido, lo divide en fragmentos, genera
  embeddings y lo indexa en el RAG. Formatos soportados: `.md`, `.markdown`,
  `.txt`, `.json`, `.csv`, `.html`, `.pdf`, `.png`, `.jpg`, `.jpeg`, `.webp`.
  - Los PDF con capa de texto se procesan localmente con `pypdf`. Los PDF
    escaneados y las imágenes requieren `GEMINI_API_KEY` para transcribir o
    describir su contenido con visión multimodal.
  - Re-subir un archivo reemplaza sus fragmentos anteriores.
  - Ejemplo: `curl.exe -F "file=@manual.pdf" -F "category=seguridad" http://localhost:8000/api/test/protocols/upload`
  - Respuestas de error: `413` archivo demasiado grande, `415` formato no
    soportado, `422` sin contenido indexable, `502` fallo de Gemini.
- `GET /api/test/protocols`: lista todos los protocolos indexados en el corpus RAG (archivos en `data/protocols` y protocolos predeterminados).
- `GET /api/test/protocols/{protocol_id}`: consulta los detalles completos y contenido de un protocolo específico del corpus.
- `GET /health/live`: comprueba que el proceso responde.
- `GET /health/ready`: verifica PostgreSQL, Redis y RabbitMQ; responde `503`
  si el servicio aun no termino de iniciar o alguna dependencia no esta lista.
- `GET /metrics`: expone las metricas Prometheus del servicio.

La publicacion conserva el formato JSON del mensaje (`event_id`, `name`) y propaga `traceparent` en
los headers del mensaje para mantener la correlacion con el consumidor
Fastify.

## Configuracion

El servicio acepta estas variables de entorno: `SERVICE_NAME`,
`SERVICE_PORT`, `DATABASE_URL`, `REDIS_URL`, `RABBITMQ_URL`, `RABBITMQ_QUEUE`,
`GEMINI_API_KEY`, `GEMINI_MODEL` (por defecto `gemini-2.5-flash`),
`GEMINI_EMBEDDING_MODEL` (por defecto `gemini-embedding-001`),
`GEMINI_EMBEDDING_DIM` (por defecto `768`), `MAX_UPLOAD_BYTES` (por defecto
20 MB), `PROTOCOLS_DIR` (por defecto `data/protocols`),
`OTEL_EXPORTER_OTLP_ENDPOINT` y `DEPLOYMENT_ENVIRONMENT`. En Compose, los
valores de conexion apuntan a los nombres de servicio de la red Docker.

El índice vectorial se persiste en la tabla `protocol_chunks` de PostgreSQL
(embeddings en JSONB, sin requerir la extensión pgvector). Al arrancar se
recupera el índice existente y se agregan o actualizan los archivos del corpus
que hayan cambiado. Si no hay `GEMINI_API_KEY`, se usa un embedding local
determinista de 128 dimensiones para desarrollo; al configurar la clave, los
documentos se re-vectorizan automáticamente con Gemini.

## Pruebas

Las pruebas unitarias de casos de uso no requieren levantar las dependencias
externas:

```powershell
$env:PYTHONPATH = "services\test-fastapi"
python -m unittest discover -s services\test-fastapi\tests -v
```

Para validar el contenedor y el flujo integrado, desde la raiz del repositorio:

```powershell
docker compose up -d --build test-fastapi-service
docker compose ps test-fastapi-service
curl.exe http://localhost:8000/api/test
curl.exe http://localhost:8000/api/test/items
```
