"""Consumption lifecycle component.

Owns receiving eligibility, the consecutive-timeout counter, stop
notification and finalization, plus all I/O (lake, commit adapter, observer).
All commit adjudication is delegated through the injected Core port
(``handle``/``snapshot``); this module never imports the Core implementation.
"""


class Consumer:
    """Receiving lifecycle around an injected pure commit core."""

    def __init__(self, lake, source, core, trace, observer=None):
        self.lake, self.source, self.core, self.trace = lake, source, core, trace
        self.observer = observer or (lambda event, state: None)
        self._last = -1
        self._streak = 0
        self._mode = "running"
        self._finished = False

    def snapshot(self):
        commit = self.core.snapshot()
        return {"mode": self._mode, "now": commit["now"], "last": self._last,
                "confirmed": commit["confirmed"], "waiting": commit["waiting"],
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
        if self.core.snapshot()["waiting"] is not None:
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
        effects = self.core.handle({"type": "submit", "offset": offset})
        attempt = next((e for e in effects if e.get("effect") == "attempt"), None)
        if attempt is None:
            raise RuntimeError("commit pending")
        token = attempt["token"]
        self.trace.append(["attempt", token, offset, attempt["deadline"]])
        try:
            self.source.request(token, offset, self._complete)
        except Exception:
            self.core.handle({"type": "request_failed", "token": token})
            self._fail_preserving("request")
            raise
        self.trace.append(["accept.return", token])
        return token

    def _complete(self, token, error=None):
        self.trace.append(["callback", token, error])
        # Adjudication errors (e.g. an unregistered token) propagate raw and
        # must not be mistaken for a failed I/O effect.
        effects = self.core.handle({"type": "complete", "token": token,
                                    "error": error,
                                    "active": self._mode == "running"})
        try:
            for effect in effects:
                self._apply(effect)
        except Exception:
            self._fail_preserving("callback")
            raise

    def _apply(self, effect):
        kind = effect.get("effect")
        if kind == "ignored":
            self.trace.append(["ignored", effect["token"]])
        elif kind == "confirm":
            self._confirm(effect)
        elif kind == "health":
            self._streak = 0
            self.trace.append(["health", 0])
        elif kind == "notify":
            self._notify(effect["event"])
        elif kind == "remote_failure":
            self._finish("remote", "stopped")

    def _confirm(self, effect):
        offset = effect["offset"]
        try:
            self.lake.confirm(offset)
        except Exception:
            self.core.handle({"type": "effect_result", "effect": "confirm",
                              "token": effect["token"], "offset": offset,
                              "ok": False})
            raise
        self.core.handle({"type": "effect_result", "effect": "confirm",
                          "token": effect["token"], "offset": offset, "ok": True})
        self.trace.append(["ack", offset])

    def tick(self, now):
        effects = self.core.handle({"type": "tick", "now": now,
                                    "active": self._mode == "running"})
        self.trace.append(["tick", now])
        token = None
        for effect in effects:
            if effect.get("effect") != "timeout":
                continue
            token = effect["token"]
            self._streak += 1
            self.trace.append(["timeout", token, self._streak])
            if self._streak == 3:
                self._finish("timeouts", "stopped")
        return token

    def close(self):
        self._finish("close", "closed")
