import heapq
from event import Event


class Scheduler:
    def __init__(self) -> None:
        self._queue = []

    def add_event(self, event):
        heapq.heappush(self._queue, event)

    def get_event(self) -> Event | None:
        if not self._queue:
            return None
        return heapq.heappop(self._queue)

    def get_current_time(self) -> float:
        if not self._queue:
            return 0.0
        return self._queue[0].get_time()

    def is_empty(self) -> bool:
        return len(self._queue) == 0
