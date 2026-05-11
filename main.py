import itertools
from client import Client
from gateway import Gateway
from my_queue import Queue
from scheduler import Scheduler
from message import Message
from event import Event, EventType
from server import Server


class Engine:
    MODELS = {
        "M/M/1": (1, float("inf")),
        "M/M/1/4": (1, 3),
        "M/M/1/8": (1, 7),
        "M/M/3/8": (3, 5),
    }

    def reset(self) -> None:
        Client.client_id_iter = itertools.count(1)
        Message.msg_id_iter = itertools.count()
        Server.server_id_iter = itertools.count()
        Event.event_id_iter = itertools.count()
        self.clients = []
        self.current_time = 0.0
        self.scheduler = Scheduler()
        self.gateway = Gateway(
            self.scheduler,
            num_of_servers=self.num_of_servers,
            queue_size=self.queue_size,
            mu=self.mu,
        )

    def __init__(self, num_of_servers=1, queue_size=float("inf"), mu=8) -> None:
        self.num_of_servers = num_of_servers
        self.queue_size = queue_size
        self.mu = mu
        self.clients = []
        self.current_time = 0.0
        self.scheduler = Scheduler()
        self.gateway = Gateway(
            self.scheduler,
            num_of_servers=num_of_servers,
            queue_size=queue_size,
            mu=mu,
        )

    # def __init__(self, num_of_servers=1, queue_size=float("inf"), mu=8) -> None:
    #     self.clients = []
    #     self.current_time = 0.0
    #     self.scheduler = Scheduler()
    #     self.gateway = Gateway(
    #         self.scheduler,
    #         num_of_servers=num_of_servers,
    #         queue_size=queue_size,
    #         mu=mu,
    #     )

    def create_clients(self, n_clients: int, lam=4) -> None:
        self.clients.extend(
            [Client(scheduler=self.scheduler, lam=lam) for _ in range(n_clients)]
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

    def run(self, num_of_clients=1, lam=4, sim_time=100.0) -> None:
        self.create_clients(num_of_clients, lam=lam)

        # seed initial SEND events
        for client in self.clients:
            client.create_message(self.current_time)

        print(
            f"{'time':<8} {'node':<5} {'event':<6} "
            f"{'source':<7} {'dest':<5} {'msgID':<5}"
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
                message.departure_time = self.current_time
                self.gateway.update_departure_metrics(message)
                self.gateway.release_server(message, self.current_time)

        self.gateway.print_stats()

    def test_message(self) -> None:
        m1 = Message(1, 0)
        m2 = Message(2, 0)

        m1.arrival_time = 1.0
        m1.service_start_time = 2.5
        m1.departure_time = 4.0
        m2.arrival_time = 2.0
        m2.service_start_time = 3.0
        m2.departure_time = 5.0

        for m in [m1, m2]:
            m.print_message()
            print(f"  wait time:    {m.get_wait_time():.3f}")
            print(f"  sojourn time: {m.get_sojourn_time():.3f}")

    def test_event(self) -> None:
        e1 = Event(Message(1, 0), EventType.SEND, 1.0)
        e2 = Event(Message(2, 0), EventType.RECV, 2.0)
        e3 = Event(Message(3, 0), EventType.DEPT, 3.0)
        for e in [e1, e2, e3]:
            e.print_event()

    def test_scheduler(self) -> None:
        scheduler = Scheduler()
        m1 = Message(1, 0)
        m2 = Message(2, 0)
        m3 = Message(3, 0)
        # add out of order to verify heap ordering
        e1 = Event(m1, EventType.SEND, 3.0)
        e2 = Event(m2, EventType.RECV, 1.0)
        e3 = Event(m3, EventType.DEPT, 2.0)
        scheduler.add_event(e1)
        scheduler.add_event(e2)
        scheduler.add_event(e3)
        print("Events should come out in time order: 1.0, 2.0, 3.0")
        while (event := scheduler.get_event()) is not None:
            event.print_event()

    def test_queue(self) -> None:
        # test normal enqueue/dequeue
        q = Queue(3)
        m1 = Message(1, 0)
        m2 = Message(2, 0)
        m3 = Message(3, 0)
        m4 = Message(4, 0)  # this one should be dropped
        print(f"Enqueue m1: {q.enqueue(m1)}")  # True
        print(f"Enqueue m2: {q.enqueue(m2)}")  # True
        print(f"Enqueue m3: {q.enqueue(m3)}")  # True
        print(f"Enqueue m4 (should drop): {q.enqueue(m4)}")  # False
        print(f"Queue size: {len(q)}")  # 3
        print("Dequeuing in order:")
        while not q.is_empty():
            m = q.dequeue()
            m.print_message()

    def test_client(self) -> None:
        scheduler = Scheduler()
        client = Client(scheduler, lam=4)
        # generate 5 messages
        current_time = 0.0
        for _ in range(5):
            client.create_message(current_time)
        print(f"Client {client.id} generated 5 messages:")
        print(
            f"{'time':<8} {'node':<5} {'event':<6} {'source':<7} {'dest':<5} {'msgID':<5}"
        )
        while (event := scheduler.get_event()) is not None:
            msg = event.get_message()
            print(
                f"{event.get_time():<8.3f} {msg.source:<5} "
                f"{event.get_event_type().value:<6} "
                f"{msg.source:<7} {msg.destination:<5} {msg.id:<5}"
            )
            current_time = event.get_time()
            client.create_message(current_time)
            if msg.id > 5:
                break

    def test_server(self) -> None:
        scheduler = Scheduler()
        server = Server(scheduler, mu=8)
        print(f"Server {server.id} busy: {server.is_busy()}")
        m1 = Message(1, 0)
        m2 = Message(2, 0)
        # assign message to server
        server.set_busy(True, m1)
        print(f"Server {server.id} busy: {server.is_busy()}")
        print(f"Server {server.id} current message: {server.current_message.id}")
        # create dept event
        event = server.get_service(m1, 1.0)
        event.print_event()
        # release server
        server.set_busy(False, None)
        print(f"Server {server.id} busy after release: {server.is_busy()}")
        # assign second message
        server.set_busy(True, m2)
        event = server.get_service(m2, 2.0)
        event.print_event()

    def test_gateway(self, num_of_servers=1, queue_size=4) -> None:
        scheduler = Scheduler()
        gateway = Gateway(
            scheduler, num_of_servers=num_of_servers, queue_size=queue_size
        )
        # simulate messages arriving
        messages = [Message(i, 0) for i in range(6)]
        current_time = 1.0

        print(
            f"Sending {len(messages)} messages to gateway with "
            f"{num_of_servers} server(s) and queue size {queue_size}"
        )

        for msg in messages:
            message = gateway.receive_message(msg, current_time)
            current_time += 0.5

        print(f"\nAfter arrivals:")
        print(f"Queue size:     {len(gateway.queue)}")
        print(f"Total arrived:  {gateway.total_arrived}")
        print(f"Total dropped:  {gateway.total_dropped}")
        # process some departures
        print("\nProcessing departures:")

        while (event := scheduler.get_event()) is not None:
            msg = event.get_message()
            msg.departure_time = event.get_time()
            gateway.update_departure_metrics(msg)
            gateway.release_server(msg, event.get_time())

        gateway.print_stats()


if __name__ == "__main__":
    # M/M/1
    engine = Engine(num_of_servers=2, queue_size=4)
    # engine.test_gateway()

    # # M/M/1/4
    # engine = Engine(num_of_servers=1, queue_size=12)
    #
    # # M/M/1/8
    # Engine(num_of_servers=1, queue_size=8)
    #
    # # M/M/3/8
    # Engine(num_of_servers=3, queue_size=8)
    engine.run(num_of_clients=2, lam=6, sim_time=30.0)
