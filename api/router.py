import json
import time
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import JSONResponse
from opentelemetry import trace

from api.schemas import (
    EventIn,
    EventUpdate,
    ItemIn,
    ProtocolDocumentOut,
    ProtocolRAGIn,
    ProtocolRAGOut,
    ProtocolSourceOut,
)
from app_metrics import record_request
from domain.errors import EventNotFound, GeminiServiceError, ProtocolNotFound
from domain.models import Event
from telemetry import structured_log

router = APIRouter()


def serialize_event(event: Event) -> dict:
    return {
        "event_id": event.event_id,
        "name": event.name,
        "created_at": event.created_at.isoformat() if event.created_at else None,
        "updated_at": event.updated_at.isoformat() if event.updated_at else None,
    }


def event_not_found(event_id: str) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={"ok": False, "error": f"Evento {event_id} no encontrado."},
    )


def current_trace_id() -> str | None:
    context = trace.get_current_span().get_span_context()
    return format(context.trace_id, "032x") if context.is_valid else None


def parse_roles(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(r) for r in data]
        return [str(data)]
    except Exception:
        return [r.strip() for r in raw.split(",") if r.strip()]


def get_auth_context(request: Request) -> dict:
    user = request.headers.get("x-auth-user")
    email = request.headers.get("x-auth-email")
    roles = parse_roles(request.headers.get("x-auth-roles"))
    return {
        "user": user,
        "email": email,
        "roles": roles,
    }


@router.get("/api/test/public")
async def test_public(request: Request):
    started = time.perf_counter()
    status = 200
    auth = get_auth_context(request)
    try:
        structured_log(
            request.app.state.logger,
            "test_public_access",
            authenticated=bool(auth["user"]),
        )
        return {
            "ok": True,
            "service": request.app.title,
            "message": "Este endpoint es publico. No requiere autenticacion JWT.",
            "auth": auth,
        }
    finally:
        record_request(request.app.title, "/api/test/public", status, started)


@router.get("/api/test")
async def test(
    request: Request,
    delay_ms: int = Query(100, ge=0, le=2000),
    fail: int = Query(0),
):
    started = time.perf_counter()
    status = 200
    auth = get_auth_context(request)
    try:
        with trace.get_tracer(__name__).start_as_current_span(
            "test.business_logic"
        ) as span:
            span.set_attribute("app.delay_ms", delay_ms)
            if auth["user"]:
                span.set_attribute("enduser.id", auth["user"])
            if auth["email"]:
                span.set_attribute("enduser.email", auth["email"])
            if auth["roles"]:
                span.set_attribute("enduser.role", ",".join(auth["roles"]))

            await request.app.state.container.simulate_test.execute(delay_ms)

            if fail == 1:
                status = 500
                structured_log(
                    request.app.state.logger,
                    "test_simulated_error",
                    user=auth["user"],
                )
                return JSONResponse(
                    status_code=status,
                    content={
                        "ok": False,
                        "service": request.app.title,
                        "trace_id": current_trace_id(),
                        "error": "Error simulado para validar observabilidad.",
                        "auth": auth,
                    },
                )

            structured_log(
                request.app.state.logger,
                "test_ok",
                delay_ms=delay_ms,
                user=auth["user"],
                email=auth["email"],
                roles=auth["roles"],
            )
            return {
                "ok": True,
                "service": request.app.title,
                "trace_id": current_trace_id(),
                "message": "pong",
                "auth": auth,
            }
    finally:
        record_request(request.app.title, "/api/test", status, started)


@router.post("/api/test/items", status_code=201)
async def create_item(item: ItemIn, request: Request):
    started = time.perf_counter()
    status = 201
    auth = get_auth_context(request)
    try:
        created = await request.app.state.container.create_item.execute(item.name)
        structured_log(
            request.app.state.logger,
            "item_created",
            item_id=created.id,
            user=auth["user"],
            roles=auth["roles"],
        )
        res = {
            "id": created.id,
            "name": created.name,
            "created_at": created.created_at.isoformat(),
        }
        if auth["user"]:
            res["created_by"] = auth["user"]
        return res
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/items", status, started)


@router.post("/api/test/events", status_code=202)
async def publish_event(event: EventIn, request: Request):
    started = time.perf_counter()
    status = 202
    auth = get_auth_context(request)
    try:
        event_id = await request.app.state.container.publish_event.execute(event.name)
        structured_log(
            request.app.state.logger,
            "event_published",
            event_id=event_id,
            user=auth["user"],
            roles=auth["roles"],
        )
        res = {"accepted": True, "event_id": event_id}
        if auth["user"]:
            res["published_by"] = auth["user"]
        return res
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/events", status, started)


@router.get("/api/test/events")
async def list_events(request: Request):
    started = time.perf_counter()
    status = 200
    try:
        events = await request.app.state.container.list_events.execute()
        return {"ok": True, "events": [serialize_event(event) for event in events]}
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/events", status, started)


@router.get("/api/test/events/{event_id}")
async def get_event(event_id: UUID, request: Request):
    started = time.perf_counter()
    status = 200
    try:
        event = await request.app.state.container.get_event.execute(str(event_id))
        return {"ok": True, "event": serialize_event(event)}
    except EventNotFound:
        status = 404
        return event_not_found(str(event_id))
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/events/{event_id}", status, started)


@router.put("/api/test/events/{event_id}")
async def update_event(event_id: UUID, body: EventUpdate, request: Request):
    started = time.perf_counter()
    status = 200
    try:
        event = await request.app.state.container.update_event.execute(
            str(event_id), body.name
        )
        structured_log(request.app.state.logger, "event_updated", event_id=str(event_id))
        return {"ok": True, "event": serialize_event(event)}
    except EventNotFound:
        status = 404
        return event_not_found(str(event_id))
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/events/{event_id}", status, started)


@router.delete("/api/test/events/{event_id}", status_code=204)
async def delete_event(event_id: UUID, request: Request):
    started = time.perf_counter()
    status = 204
    try:
        await request.app.state.container.delete_event.execute(str(event_id))
        structured_log(request.app.state.logger, "event_deleted", event_id=str(event_id))
        return Response(status_code=204)
    except EventNotFound:
        status = 404
        return event_not_found(str(event_id))
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/events/{event_id}", status, started)


@router.get("/api/test/items")
async def list_items(request: Request):
    started = time.perf_counter()
    status = 200
    try:
        result = await request.app.state.container.list_items.execute()
        if result.source == "cache":
            structured_log(request.app.state.logger, "items_cache_hit")
        else:
            structured_log(
                request.app.state.logger,
                "items_cache_miss",
                count=len(result.items),
            )
        return {
            "ok": True,
            "source": result.source,
            "items": [
                {
                    "id": item.id,
                    "name": item.name,
                    "created_at": item.created_at.isoformat(),
                }
                for item in result.items
            ],
        }
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/items", status, started)


@router.post(
    "/api/test/protocols/rag",
    response_model=ProtocolRAGOut,
    summary="Buscar en corpus de protocolos y generar respuesta con RAG + Gemini LLM",
)
async def query_protocols_rag(body: ProtocolRAGIn, request: Request):
    started = time.perf_counter()
    status = 200
    auth = get_auth_context(request)
    try:
        rag_answer = await request.app.state.container.query_protocols_rag.execute(
            query=body.query,
            top_k=body.top_k,
            category=body.category,
            generate_answer=body.generate_answer,
            temperature=body.temperature,
        )
        structured_log(
            request.app.state.logger,
            "protocols_rag_executed",
            query=body.query,
            category=body.category,
            sources_count=len(rag_answer.sources),
            generate_answer=body.generate_answer,
            model=rag_answer.model,
            user=auth["user"],
        )
        return {
            "ok": True,
            "query": rag_answer.query,
            "answer": rag_answer.answer,
            "model": rag_answer.model,
            "total_sources": len(rag_answer.sources),
            "sources": [
                {
                    "protocol_id": s.document.id,
                    "title": s.document.title,
                    "content": s.document.content,
                    "source_file": s.document.source_file,
                    "category": s.document.category,
                    "score": s.score,
                    "metadata": s.document.metadata,
                }
                for s in rag_answer.sources
            ],
        }
    except GeminiServiceError as exc:
        status = 502
        structured_log(
            request.app.state.logger,
            "protocols_rag_gemini_error",
            query=body.query,
            error=str(exc),
            user=auth["user"],
        )
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": f"Error del servicio Gemini: {exc.message}",
                "query": body.query,
            },
        )
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/protocols/rag", status, started)


@router.get(
    "/api/test/protocols",
    summary="Listar protocolos disponibles en el corpus RAG",
)
async def list_protocols(request: Request):
    started = time.perf_counter()
    status = 200
    try:
        documents = await request.app.state.container.list_protocols.execute()
        return {
            "ok": True,
            "total": len(documents),
            "protocols": [
                {
                    "id": doc.id,
                    "title": doc.title,
                    "source_file": doc.source_file,
                    "category": doc.category,
                    "metadata": doc.metadata,
                }
                for doc in documents
            ],
        }
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/protocols", status, started)


@router.get(
    "/api/test/protocols/{protocol_id}",
    summary="Consultar un protocolo específico del corpus por su ID",
)
async def get_protocol(protocol_id: str, request: Request):
    started = time.perf_counter()
    status = 200
    try:
        doc = await request.app.state.container.get_protocol.execute(protocol_id)
        return {
            "ok": True,
            "protocol": {
                "id": doc.id,
                "title": doc.title,
                "content": doc.content,
                "source_file": doc.source_file,
                "category": doc.category,
                "metadata": doc.metadata,
            },
        }
    except ProtocolNotFound:
        status = 404
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"Protocolo '{protocol_id}' no encontrado en el corpus."},
        )
    except Exception:
        status = 500
        raise
    finally:
        record_request(
            request.app.title, f"/api/test/protocols/{protocol_id}", status, started
        )

