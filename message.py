import itertools


class Message:
    msg_id_iter = itertools.count()

    def __init__(self, source, destination=0) -> None:
        self.id = next(Message.msg_id_iter)
        self.source = source
        self.destination = destination

    def get_id(self) -> int:
        return self.id

    def set_source(self, source) -> None:
        self.source = source

    def set_destination(self, destination) -> None:
        self.destination = destination

    def get_source(self) -> int:
        return self.source

    def get_destination(self) -> int:
        return self.destination

    def print_message(self) -> None:
        print(f"[Message {self.id}] {self.source} -> {self.destination}")
