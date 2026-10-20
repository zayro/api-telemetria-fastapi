import time
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import JSONResponse
from opentelemetry import trace

from api.schemas import EventIn, EventUpdate, ItemIn
from app_metrics import record_request
from domain.errors import EventNotFound
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


@router.get("/api/test")
async def test(
    request: Request,
    delay_ms: int = Query(100, ge=0, le=2000),
    fail: int = Query(0),
):
    started = time.perf_counter()
    status = 200
    try:
        with trace.get_tracer(__name__).start_as_current_span(
            "test.business_logic"
        ) as span:
            span.set_attribute("demo.delay_ms", delay_ms)
            await request.app.state.container.simulate_test.execute(delay_ms)

            if fail == 1:
                status = 500
                structured_log(request.app.state.logger, "test_simulated_error")
                return JSONResponse(
                    status_code=status,
                    content={
                        "ok": False,
                        "service": request.app.title,
                        "trace_id": current_trace_id(),
                        "error": "Error simulado para validar observabilidad.",
                    },
                )

            structured_log(request.app.state.logger, "test_ok", delay_ms=delay_ms)
            return {
                "ok": True,
                "service": request.app.title,
                "trace_id": current_trace_id(),
                "message": "pong",
            }
    finally:
        record_request(request.app.title, "/api/test", status, started)


@router.post("/api/test/items", status_code=201)
async def create_item(item: ItemIn, request: Request):
    started = time.perf_counter()
    status = 201
    try:
        created = await request.app.state.container.create_item.execute(item.name)
        structured_log(request.app.state.logger, "item_created", item_id=created.id)
        return {
            "id": created.id,
            "name": created.name,
            "created_at": created.created_at.isoformat(),
        }
    except Exception:
        status = 500
        raise
    finally:
        record_request(request.app.title, "/api/test/items", status, started)


@router.post("/api/test/events", status_code=202)
async def publish_event(event: EventIn, request: Request):
    started = time.perf_counter()
    status = 202
    try:
        event_id = await request.app.state.container.publish_event.execute(event.name)
        structured_log(request.app.state.logger, "event_published", event_id=event_id)
        return {"accepted": True, "event_id": event_id}
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
