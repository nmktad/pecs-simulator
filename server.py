import itertools
import random
from event import Event, EventType


class Server:
    server_id_iter = itertools.count()

    @classmethod
    def reset_counter(cls):
        cls.server_id_iter = itertools.count()

    def __init__(self, scheduler, mu: float = 8.0) -> None:
        self.id = next(Server.server_id_iter)
        self.mu = mu
        self.busy = False
        self.current_message = None
        self.scheduler = scheduler
        self.total_service_time = 0.0
        self.served_count = 0

    def is_busy(self) -> bool:
        return self.busy

    def set_busy(self, state: bool, message=None) -> None:
        self.busy = state
        self.current_message = message

    def get_service(self, message, current_time) -> Event:
        service_time = random.expovariate(self.mu)
        event = Event(message, EventType.DEPT, current_time + service_time)
        self.scheduler.add_event(event)
        return event

    def update_departure_metrics(self, message) -> None:
        self.total_service_time += message.get_service_time()
        self.served_count += 1

    def get_avg_service_time(self):
        if self.served_count == 0:
            return None
        return self.total_service_time / self.served_count
