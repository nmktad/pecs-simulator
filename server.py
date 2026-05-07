import itertools
import random
from event import Event, EventType


class Server:
    server_id_iter = itertools.count()

    def __init__(self, scheduler, mu=8) -> None:
        self.id = next(Server.server_id_iter)
        self.mu = mu
        self.busy = False
        self.current_message = None
        self.scheduler = scheduler

    def is_busy(self) -> bool:
        return self.busy

    def set_busy(self, state: bool, message=None) -> None:
        self.busy = state
        self.current_message = message

    def create_event(self, message, current_time) -> Event:
        service_time = random.expovariate(self.mu)
        return Event(message, EventType.DEPT, current_time + service_time)
