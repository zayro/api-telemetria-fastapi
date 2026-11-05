class EventNotFound(Exception):
    def __init__(self, event_id: str):
        super().__init__(f"Event {event_id} not found")
        self.event_id = event_id


class ProtocolNotFound(Exception):
    def __init__(self, protocol_id: str):
        super().__init__(f"Protocol {protocol_id} not found")
        self.protocol_id = protocol_id


class GeminiServiceError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code

