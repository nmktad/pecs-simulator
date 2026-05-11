from message import Message
from my_queue import Queue
from server import Server


class Gateway:
    def __init__(self, scheduler, num_of_servers=1, queue_size=float("inf"), mu=8):
        self.scheduler = scheduler
        self.servers = [Server(scheduler, mu=mu) for _ in range(num_of_servers)]
        self.queue = Queue(queue_size)

        # metrics
        self.total_arrived = 0
        self.total_dropped = 0
        self.total_served = 0
        self.total_queued = 0

        self.total_wait_time = 0.0
        self.total_sojourn_time = 0.0

    def get_free_server(self):
        for server in self.servers:
            if not server.is_busy():
                return server
        return None

    def receive_message(self, message, current_time) -> Message:
        self.total_arrived += 1
        message.arrival_time = current_time

        server = self.get_free_server()
        if server is not None:
            self._assign_to_server(server, message, current_time)
        else:
            dropped = not self.queue.enqueue(message)
            if dropped:
                self.total_dropped += 1
            else:
                self.total_queued += 1

        return message

    def _assign_to_server(self, server, message, current_time):
        server.set_busy(True, message)
        message.service_start_time = current_time
        self.total_wait_time += current_time - message.arrival_time
        server.get_service(message, current_time)

    def release_server(self, message, current_time):
        server = next(s for s in self.servers if s.current_message == message)
        server.set_busy(False, None)

        if not self.queue.is_empty():
            next_message = self.queue.dequeue()
            self._assign_to_server(server, next_message, current_time)

    def update_departure_metrics(self, message):
        server = next(s for s in self.servers if s.current_message == message)
        server.update_departure_metrics(message)

        self.total_sojourn_time += message.get_sojourn_time()
        self.total_served += 1

    def print_stats(self):
        print(f"\n--- Gateway Stats ---")
        print(f"Total arrived:  {self.total_arrived}")
        print(f"Total queued:   {self.total_queued}")
        print(f"Total dropped:  {self.total_dropped}")
        print(f"Total served:   {self.total_served}")

        if self.total_served > 0:
            total_service_time = sum(
                server.total_service_time for server in self.servers
            )
            print(f"Avg wait time:  {self.total_wait_time / self.total_served:.3f}")
            print(f"Avg sojourn:    {self.total_sojourn_time / self.total_served:.3f}")
            print(f"Avg service:    {total_service_time / self.total_served:.3f}")

            print("Avg service/server:")
            for server in self.servers:
                avg = server.get_avg_service_time()
                if avg is not None:
                    print(f"  Server {server.id}: {avg:.3f}")
                else:
                    print(f"  Server {server.id}: N/A")
