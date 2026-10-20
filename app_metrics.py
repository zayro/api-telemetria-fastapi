from time import perf_counter

from prometheus_client import Counter, Histogram

REQUESTS = Counter(
    "demo_http_requests_total",
    "Total de solicitudes HTTP del demo",
    ["service", "route", "status"],
)
DURATION = Histogram(
    "demo_http_request_duration_seconds",
    "Duracion de solicitudes HTTP del demo",
    ["service", "route"],
)


def record_request(service_name: str, route: str, status: int, started: float) -> None:
    REQUESTS.labels(service_name, route, str(status)).inc()
    DURATION.labels(service_name, route).observe(perf_counter() - started)
