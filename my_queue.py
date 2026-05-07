from collections import deque
from message import Message


class Queue:
    def __init__(self, queue_size):
        self.queue = deque()
        self.queue_limit = queue_size

    def enqueue(self, message: Message) -> bool:
        if len(self.queue) >= self.queue_limit:
            return False
        self.queue.append(message)
        return True

    def dequeue(self) -> Message | None:
        if self.is_empty():
            return None
        return self.queue.popleft()

    def peek(self) -> Message | None:
        if self.is_empty():
            return None
        return self.queue[0]

    def is_empty(self) -> bool:
        return len(self.queue) == 0

    def __len__(self) -> int:
        return len(self.queue)
