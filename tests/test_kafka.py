import json
import unittest
from unittest.mock import AsyncMock

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.propagate import set_global_textmap

from domain.models import Event
from infrastructure.adapters import KafkaEventPublisher


class KafkaPublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_waits_for_confirmation_and_propagates_producer_context(self):
        provider = TracerProvider()
        tracer = provider.get_tracer("test")
        set_global_textmap(TraceContextTextMapPropagator())
        producer = AsyncMock()
        event = Event(event_id="11111111-1111-4111-8111-111111111111", name="example")
        publisher = KafkaEventPublisher(producer, "test.events", tracer)
        with tracer.start_as_current_span("http") as parent:
            await publisher.publish(event)
            parent_trace_id = parent.get_span_context().trace_id
        producer.send_and_wait.assert_awaited_once()
        args, kwargs = producer.send_and_wait.call_args
        self.assertEqual(args, ("test.events",))
        self.assertEqual(json.loads(kwargs["value"]), {"event_id": event.event_id, "name": event.name})
        self.assertEqual(kwargs["key"], event.event_id.encode())
        traceparent = dict(kwargs["headers"])["traceparent"].decode()
        self.assertEqual(traceparent.split("-")[1], format(parent_trace_id, "032x"))
        self.assertFalse(trace.get_current_span().is_recording())
        provider.shutdown()

    async def test_broker_error_is_propagated_to_the_use_case(self):
        producer = AsyncMock()
        producer.send_and_wait.side_effect = RuntimeError("broker unavailable")
        provider = TracerProvider()
        publisher = KafkaEventPublisher(producer, "test.events", provider.get_tracer("test"))
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await publisher.publish(Event(event_id="id", name="example"))
        provider.shutdown()
