class EventNotFound(Exception):
    def __init__(self, event_id: str):
        super().__init__(f"Event {event_id} not found")
        self.event_id = event_id
