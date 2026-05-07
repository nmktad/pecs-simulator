import itertools


class Message:
    msg_id_iter = itertools.count()

    def __init__(self, source, destination=0) -> None:
        self.id = next(Message.msg_id_iter)
        self.source = source
        self.destination = destination
        self.arrival_time = 0.0
        self.service_start_time = 0.0
        self.departure_time = 0.0

    def get_id(self) -> int:
        return self.id

    def get_source(self) -> int:
        return self.source

    def get_destination(self) -> int:
        return self.destination

    def set_source(self, source) -> None:
        self.source = source

    def set_destination(self, destination) -> None:
        self.destination = destination

    def get_wait_time(self) -> float:
        return self.service_start_time - self.arrival_time

    def get_sojourn_time(self) -> float:
        return self.departure_time - self.arrival_time

    def print_message(self) -> None:
        print(f"[Message {self.id}] {self.source} -> {self.destination}")
