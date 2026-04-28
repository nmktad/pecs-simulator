import itertools
import heapq

from event import Event


class Scheduler:

    def __init__(self) -> None:
        self._queue = []

    def add_event(self, event):
        heapq.heappush(self._queue, event)

    def get_current_time(self):
        return 0 if not self._queue else self._queue[0].get_time()

    def get_event(self) -> Event | None:
        if not self._queue:
            return None

        event = heapq.heappop(self._queue)

        return event
