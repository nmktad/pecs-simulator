from client import Client
from gateway import Gateway
from scheduler import Scheduler
from message import Message
from event import Event, EventType
import random

SERVER = 0


class Engine:
    def __init__(self, num_of_server=1) -> None:
        self.clients = []
        self.current_time = 0.0

        self.num_of_servers = num_of_server
        self.scheduler = Scheduler()
        self.gateway = Gateway(self.scheduler)

    def create_clients(self, n_clients: int) -> None:
        self.clients.extend(
            [Client(scheduler=self.scheduler) for _ in range(n_clients)]
        )

    def get_current_time(self):
        return self.current_time

    def trace(self, e) -> None:
        msg = e.get_message()

        node = msg.source if e.get_event_type() == EventType.SEND else msg.destination

        print(
            f"{e.get_time():<8.3f} {node:<5} {e.get_event_type().value:<6} "
            f"{msg.source:<7} {msg.destination:<5} {msg.id:<5}"
        )

    def test_gateway(self, num_of_clients=10, num_of_servers=3) -> None:
        self.create_clients(num_of_clients)

        current_time = self.get_current_time() or 1.0

        for client in self.clients:
            client.create_message(current_time)

        self.gateway = Gateway(self.scheduler, num_of_servers=num_of_servers)

        print(
            f"{'time':<8} {'node':<5} {'event':<6} {'source':<7} {'dest':<5} {'msgID':<5}"
        )

        while (event := self.scheduler.get_event()) is not None:
            self.current_time = event.get_time()
            self.trace(event)

            message = event.get_message()
            if event.get_event_type() == EventType.SEND:
                self.gateway.add_to_queue(message)
                self.gateway.process_queue(self.current_time)
            elif event.get_event_type() == EventType.DEPT:
                server_id = message.get_destination()
                self.gateway.release_server(server_id, self.current_time)

    def test_events(self, num_of_messages=5) -> None:
        current_time = 1.0

        # Create initial SEND events
        for _ in range(num_of_messages):
            source = random.randint(1, len(self.clients))
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

        for e in self.scheduler._queue:
            self.trace(e)

    def test_message(self, num_of_messages=5) -> None:
        current_time = 1.0

        for _ in range(num_of_messages):
            source = random.randint(1, len(self.clients))
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

    def test_clients(self, num_of_clients=10):
        self.create_clients(num_of_clients)

        current_time = self.get_current_time() or 1.0

        for client in self.clients:
            client.create_message(current_time)

        print(
            f"{'time':<8} {'node':<5} {'event':<6} {'source':<7} {'dest':<5} {'msgID':<5}"
        )

        for _ in range(3):
            event = self.scheduler.get_event()
            if event != None:
                self.current_time = event.get_time()
                self.trace(event)

                msg = event.get_message()
                client = self.clients[msg.get_source()].create_message(
                    self.get_current_time()
                )

        while (event := self.scheduler.get_event()) is not None:
            self.current_time += event.get_time()
            self.trace(event)

            # msg = event.get_message()
            # client = self.clients[msg.get_source()].create_message(
            #     self.get_current_time()
            # )

    def run(self, num_of_clients=1, sim_time=100.0):
        self.create_clients(num_of_clients)

        # seed initial SEND events
        for client in self.clients:
            client.create_message(self.current_time)

        print(
            f"{'time':<8} {'node':<5} {'event':<6} {'source':<7} {'dest':<5} {'msgID':<5}"
        )

        while (event := self.scheduler.get_event()) is not None:
            self.current_time = event.get_time()

            if self.current_time > sim_time:
                break

            self.trace(event)
            message = event.get_message()
            event_type = event.get_event_type()

            if event_type == EventType.SEND:
                # schedule RECV 1 second later
                recv_event = Event(message, EventType.RECV, self.current_time + 1.0)
                self.scheduler.add_event(recv_event)
                # client schedules its next message
                self.clients[message.get_source()].create_message(self.current_time)

            elif event_type == EventType.RECV:
                self.gateway.receive_message(message, self.current_time)

            elif event_type == EventType.DEPT:
                # record sojourn time before releasing
                message.departure_time = self.current_time
                self.gateway.sojourn_times.append(
                    message.departure_time - message.arrival_time
                )
                self.gateway.total_served += 1
                self.gateway.release_server(
                    message.get_destination(), self.current_time
                )

        self.gateway.print_stats()


if __name__ == "__main__":
    engine = Engine()
    engine.test_gateway(3)
