"""Pure commit decision core.

The core owns commit-attempt identity, deadlines, completion/timeout
attribution and the confirmed frontier.  It performs no I/O and exchanges
only JSON-safe built-in data through ``handle``/``snapshot``.
"""


class Core:
    """Data-only commit state machine driven by event dictionaries."""

    def __init__(self, timeout=5, confirmed=-1):
        if type(timeout) is not int or timeout <= 0:
            raise ValueError("timeout")
        self._timeout = timeout
        self._now = 0
        self._confirmed = confirmed
        self._next = 1
        self._attempts = {}
        self._waiting = None

    def snapshot(self):
        """Return an independent JSON-safe commit state view."""
        return {"now": self._now, "confirmed": self._confirmed,
                "waiting": self._waiting}

    def handle(self, event):
        """Consume one data event and return the resulting data effects."""
        if not isinstance(event, dict):
            raise ValueError("event")
        kind = event.get("type")
        if kind == "submit":
            return self._submit(event)
        if kind == "request_failed":
            return self._request_failed(event)
        if kind == "complete":
            return self._complete(event)
        if kind == "effect_result":
            return self._effect_result(event)
        if kind == "tick":
            return self._tick(event)
        raise ValueError("event")

    def _attempt(self, token):
        return self._attempts[str(token)]

    def _submit(self, event):
        if self._waiting is not None:
            return [{"effect": "rejected", "reason": "commit pending"}]
        offset = event["offset"]
        if type(offset) is not int:
            raise ValueError("event")
        token = self._next
        self._next += 1
        deadline = self._now + self._timeout
        self._attempts[str(token)] = {
            "offset": offset, "deadline": deadline, "status": "waiting"}
        self._waiting = token
        return [{"effect": "attempt", "token": token, "offset": offset,
                 "deadline": deadline}]

    def _request_failed(self, event):
        token = event["token"]
        attempt = self._attempt(token)
        if attempt["status"] == "waiting":
            attempt["status"] = "request_error"
            if self._waiting == token:
                self._waiting = None
        return []

    def _complete(self, event):
        token = event["token"]
        error = event.get("error")
        active = event.get("active", True)
        attempt = self._attempt(token)
        status = attempt["status"]
        if status not in ("waiting", "timeout"):
            return [{"effect": "ignored", "token": token}]

        # Completion attribution happens before any effect is executed.  A
        # late success/failure is terminal but is not "current".
        was_waiting = status == "waiting"
        attempt["status"] = "success" if error is None else "error"
        if self._waiting == token:
            self._waiting = None

        if error is not None:
            if was_waiting and active:
                return [{"effect": "remote_failure", "token": token}]
            return []

        effects = []
        if attempt["offset"] > self._confirmed:
            effects.append({"effect": "confirm", "token": token,
                            "offset": attempt["offset"]})
        if was_waiting and active:
            effects.append({"effect": "health", "token": token, "streak": 0})
        effects.append({"effect": "notify", "token": token, "event": "ack"})
        return effects

    def _effect_result(self, event):
        # The frontier moves only after the I/O owner reports that the durable
        # receipt was actually written.  A failed write leaves it untouched.
        if event.get("effect") == "confirm" and event.get("ok") is True:
            offset = event["offset"]
            if offset > self._confirmed:
                self._confirmed = offset
        return []

    def _tick(self, event):
        now = event["now"]
        if type(now) is not int or now < self._now:
            raise ValueError("clock")
        self._now = now
        if not event.get("active", True) or self._waiting is None:
            return []
        token = self._waiting
        attempt = self._attempt(token)
        if now < attempt["deadline"]:
            return []
        attempt["status"] = "timeout"
        self._waiting = None
        return [{"effect": "timeout", "token": token}]
