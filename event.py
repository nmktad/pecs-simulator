import itertools
from enum import Enum


class EventType(Enum):
    SEND = "SEND"
    RECV = "RECV"
    DEPT = "DEPT"


class Event:
    event_id_iter = itertools.count()

    def __lt__(self, other):
        return self.time < other.time

    def __init__(self, message, event_type: EventType, time) -> None:
        self.id = next(Event.event_id_iter)
        self.time = time
        self.message = message
        self.event_type = event_type

    def get_time(self):
        return self.time

    def get_id(self):
        return self.id

    def get_message(self):
        return self.message

    def get_event_type(self):
        return self.event_type

    def set_message(self, message) -> None:
        if message is None:
            raise ValueError("message cannot be None")
        self.message = message

    def set_event_type(self, event_type: EventType) -> None:
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be EventType")
        self.event_type = event_type

    def get_node(self):
        if self.event_type == EventType.SEND:
            return self.message.source
        else:
            return self.message.destination

    def print_event(self) -> None:
        print(
            f"[Event {self.id}] "
            f"Time: {self.time:.3f} | "
            f"Node: {self.get_node()} | "
            f"{self.event_type.value} | "
            f"MsgID: {self.message.id} | "
            f"{self.message.source} -> {self.message.destination}"
        )
