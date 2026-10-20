import atexit
import json
import logging
import os

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup_telemetry(service_name: str):
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/")
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "development")

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.namespace": "demo-etapa1",
            "deployment.environment.name": environment,
        }
    )

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
    )
    trace.set_tracer_provider(tracer_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{endpoint}/v1/logs"))
    )
    set_logger_provider(logger_provider)

    app_logger = logging.getLogger(service_name)
    app_logger.setLevel(logging.INFO)
    app_logger.propagate = False

    if not app_logger.handlers:
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter("%(message)s"))
        app_logger.addHandler(console)
        app_logger.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

    atexit.register(tracer_provider.shutdown)
    atexit.register(logger_provider.shutdown)

    return trace.get_tracer(service_name), app_logger


def structured_log(logger, event: str, level: int = logging.INFO, **fields):
    span_context = trace.get_current_span().get_span_context()
    payload = {"event": event, **fields}

    if span_context.is_valid:
        payload["trace_id"] = format(span_context.trace_id, "032x")
        payload["span_id"] = format(span_context.span_id, "016x")

    logger.log(level, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
