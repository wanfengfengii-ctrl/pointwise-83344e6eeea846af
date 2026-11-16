from commitcore import Core
from consumer import Consumer


class Worker:
    """Compatibility facade: assembles the pure core with the lifecycle port."""
    def __init__(self, lake, source, trace, observer=None, timeout=5):
        if type(timeout) is not int or timeout <= 0:
            raise ValueError("timeout")
        self._consumer = Consumer(lake, source,
                                  Core(timeout=timeout, confirmed=lake.confirmed()),
                                  trace, observer)

    def accept(self, batch):
        return self._consumer.accept(batch)

    def tick(self, now):
        return self._consumer.tick(now)

    def close(self):
        return self._consumer.close()

    def snapshot(self):
        return self._consumer.snapshot()
