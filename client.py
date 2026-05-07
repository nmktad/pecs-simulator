import itertools
import random
from event import Event, EventType
from message import Message


class Client:
    client_id_iter = itertools.count()

    def __init__(self, scheduler, lam=4) -> None:
        self.id = next(Client.client_id_iter)
        self.lam = lam
        self.scheduler = scheduler

    def create_message(self, current_time):
        msg = Message(self.id, 0)
        inter_arrival = random.expovariate(self.lam)
        event = Event(msg, EventType.SEND, current_time + inter_arrival)
        self.scheduler.add_event(event)
        return msg
