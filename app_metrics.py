from time import perf_counter

from prometheus_client import Counter, Histogram

REQUESTS = Counter(
    "app_http_requests_total",
    "Total de solicitudes HTTP del app",
    ["service", "route", "status"],
)
DURATION = Histogram(
    "app_http_request_duration_seconds",
    "Duracion de solicitudes HTTP del app",
    ["service", "route"],
)
PUBLISHED = Counter(
    "app_messaging_published_total",
    "Publicaciones Kafka confirmadas o fallidas",
    ["topic", "outcome"],
)
PUBLISH_DURATION = Histogram(
    "app_messaging_publish_duration_seconds",
    "Tiempo de publicacion Kafka hasta confirmacion del broker",
    ["topic"],
)


def record_request(service_name: str, route: str, status: int, started: float) -> None:
    REQUESTS.labels(service_name, route, str(status)).inc()
    DURATION.labels(service_name, route).observe(perf_counter() - started)
