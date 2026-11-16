class Worker:
    """Correct legacy implementation; commit and lifecycle state are intertwined."""
    def __init__(self, lake, source, trace, observer=None, timeout=5):
        if type(timeout) is not int or timeout <= 0:
            raise ValueError("timeout")
        self.lake, self.source, self.trace = lake, source, trace
        self.observer = observer or (lambda event, state: None)
        self.timeout = timeout
        self._now = 0
        self._last = -1
        self._confirmed = lake.confirmed()
        self._next = 1
        self._attempts = {}
        self._waiting = None
        self._streak = 0
        self._mode = "running"
        self._finished = False

    def snapshot(self):
        return {"mode": self._mode, "now": self._now, "last": self._last,
                "confirmed": self._confirmed, "waiting": self._waiting,
                "streak": self._streak}

    def _notify(self, event):
        self.trace.append(["notify", event, self.snapshot()])
        self.observer(event, self.snapshot())

    def _finish(self, reason, mode):
        if self._finished:
            return
        # Claim all finalization before calling user code: close may reenter here.
        self._finished = True
        self._mode = mode
        self.trace.append(["stop", reason])
        first = None
        for action in (lambda: self._notify("stopped"), self.source.close, self.lake.finish):
            try:
                action()
            except Exception as error:
                if first is None:
                    first = error
        if first is not None:
            raise first

    def _fail_preserving(self, reason):
        try:
            self._finish(reason, "stopped")
        except Exception:
            pass

    def accept(self, batch):
        if self._mode != "running":
            raise RuntimeError("not accepting")
        if self._waiting is not None:
            raise RuntimeError("commit pending")
        if (not isinstance(batch, dict) or set(batch) != {"offset", "rows"} or
                type(batch["offset"]) is not int or batch["offset"] <= self._last or
                not isinstance(batch["rows"], list)):
            raise ValueError("batch")
        for row in batch["rows"]:
            if (not isinstance(row, list) or len(row) != 2 or
                    not isinstance(row[0], str) or not row[0] or type(row[1]) is not int):
                raise ValueError("row")
        offset, rows = batch["offset"], batch["rows"]
        try:
            self.lake.write(offset, rows)
        except Exception:
            self._fail_preserving("lake")
            raise
        self._last = offset
        token, self._next = self._next, self._next + 1
        self._attempts[token] = {"offset": offset, "deadline": self._now + self.timeout,
                                 "status": "waiting"}
        self._waiting = token
        self.trace.append(["attempt", token, offset, self._now + self.timeout])
        try:
            self.source.request(token, offset, self._done)
        except Exception:
            attempt = self._attempts[token]
            if attempt["status"] == "waiting":
                attempt["status"] = "request_error"
                if self._waiting == token:
                    self._waiting = None
            self._fail_preserving("request")
            raise
        self.trace.append(["accept.return", token])
        return token

    def _done(self, token, error=None):
        self.trace.append(["callback", token, error])
        attempt = self._attempts[token]
        status = attempt["status"]
        if status not in ("waiting", "timeout"):
            self.trace.append(["ignored", token])
            return
        current = status == "waiting" and self._mode == "running"
        attempt["status"] = "success" if error is None else "error"
        if self._waiting == token:
            self._waiting = None
        if error is not None:
            if current:
                self._finish("remote", "stopped")
            return
        try:
            offset = attempt["offset"]
            if offset > self._confirmed:
                self.lake.confirm(offset)
                self._confirmed = offset
                self.trace.append(["ack", offset])
            if current:
                self._streak = 0
                self.trace.append(["health", 0])
            # Receipt, attempt and health are all installed before a reentrant observer.
            self._notify("ack")
        except Exception:
            self._fail_preserving("callback")
            raise

    def tick(self, now):
        if type(now) is not int or now < self._now:
            raise ValueError("clock")
        self._now = now
        self.trace.append(["tick", now])
        if self._mode != "running" or self._waiting is None:
            return None
        token = self._waiting
        attempt = self._attempts[token]
        if now < attempt["deadline"]:
            return None
        attempt["status"] = "timeout"
        self._waiting = None
        self._streak += 1
        self.trace.append(["timeout", token, self._streak])
        if self._streak == 3:
            self._finish("timeouts", "stopped")
        return token

    def close(self):
        self._finish("close", "closed")
