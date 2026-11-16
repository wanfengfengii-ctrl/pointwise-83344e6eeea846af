class RecordedSource:
    """A deterministic asynchronous commit adapter, not a Kafka client."""
    def __init__(self, trace, plans=None):
        self.trace = trace
        self.plans = dict(plans or {})
        self.callbacks = {}
        self.close_error = None

    def request(self, token, offset, callback):
        self.trace.append(["source.request", token, offset])
        self.callbacks[token] = callback
        plan = self.plans.get(token, {})
        # A callback failure has priority over an adapter's after-callback error.
        if "sync" in plan:
            callback(token, plan["sync"])
        if "raise" in plan:
            raise RuntimeError(plan["raise"])
        self.trace.append(["source.return", token])

    def complete(self, token, error=None):
        self.trace.append(["source.complete", token, error])
        self.callbacks[token](token, error)

    def close(self):
        self.trace.append(["source.close"])
        error, self.close_error = self.close_error, None
        if error is not None:
            raise RuntimeError(error)
