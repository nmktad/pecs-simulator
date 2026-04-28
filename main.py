from scheduler import Scheduler
from message import Message
from event import Event, EventType
import random


class Engine:
    def __init__(self, num_of_client=3, num_of_server=1) -> None:
        self.num_of_clients = num_of_client
        self.num_of_servers = num_of_server
        self.arr = []
        self.scheduler = Scheduler()

    def trace(self, e) -> None:
        msg = e.get_message()

        node = msg.source if e.get_event_type() == EventType.SEND else msg.destination

        print(
            f"{e.get_time():<8.3f} {node:<5} {e.get_event_type().value:<6} "
            f"{msg.source:<7} {msg.destination:<5} {msg.id:<5}"
        )

    def test_events(self, num_of_messages=5) -> None:
        current_time = 1.0

        # Create initial SEND events
        for _ in range(num_of_messages):
            source = random.randint(1, self.num_of_clients)
            dest = 0

            msg = Message(source, dest)
            event = Event(msg, EventType.SEND, current_time)
            self.scheduler.add_event(event)

            current_time += random.uniform(0.5, 1.5)

            # RECV
            recv_event = Event(msg, EventType.RECV, current_time)
            self.scheduler.add_event(recv_event)

            current_time += random.uniform(2.0, 4.0)

            # DEPT
            dept_event = Event(msg, EventType.DEPT, current_time)
            self.scheduler.add_event(dept_event)

            current_time += random.uniform(0.5, 1.5)

        print(
            f"{'time':<8} {'node':<5} {'event':<6} {'source':<7} {'dest':<5} {'msgID':<5}"
        )

        for e in self.arr:
            self.trace(e)

    def test_message(self, num_of_messages=5) -> None:
        current_time = 1.0

        for _ in range(num_of_messages):
            source = random.randint(1, self.num_of_clients)
            dest = 0

            msg = Message(source, dest)

            # SEND event
            self.scheduler.add_event(Event(msg, EventType.SEND, current_time))

            current_time += random.uniform(0.5, 1.5)

            # RECV event
            self.scheduler.add_event(Event(msg, EventType.RECV, current_time))

            current_time += random.uniform(2.5, 4.0)

            # DEPT event
            self.scheduler.add_event(Event(msg, EventType.DEPT, current_time))

            current_time += random.uniform(0.5, 2.0)

    def run(self):
        self.test_events()


if __name__ == "__main__":
    engine = Engine()
    engine.run()
