"""Structural verification for the responsibility split.

Drives the pure Core without any I/O, drives Consumer through a scripted core
that exposes only the published port (and records every port access), and
checks dependency direction and JSON-safe state isolation.  Business
compatibility remains covered by checks.run_cases and its frozen traces.
"""

import ast
import json
from pathlib import Path

from commitcore import Core
from consumer import Consumer
from lake import Lake
from source import RecordedSource
from worker import Worker

HERE = Path(__file__).resolve().parent
BUILTINS = (bool, int, float, str, type(None))


def _assert_builtin_json(value, path="state"):
    if type(value) in BUILTINS:
        return
    if type(value) is list:
        for i, item in enumerate(value):
            _assert_builtin_json(item, "%s[%d]" % (path, i))
        return
    if type(value) is dict:
        for key, item in value.items():
            assert type(key) is str, (path, "non-string key", key)
            _assert_builtin_json(item, "%s[%r]" % (path, key))
        return
    raise AssertionError("%s holds a live/non-JSON object: %r" % (path, type(value)))


def _assert_json_roundtrip(value, path="state"):
    _assert_builtin_json(value, path)
    assert json.loads(json.dumps(value)) == value, path


def _imports(path):
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add((node.module or "").split(".")[0])
    return names


def check_dependencies():
    # Core: no imports at all, so no database/adapter/lifecycle coupling.
    assert _imports(HERE / "commitcore.py") == set()
    # Consumer: no imports, so it cannot import the Core implementation.
    assert _imports(HERE / "consumer.py") == set()
    # Worker is the only assembler and imports exactly the two components.
    assert _imports(HERE / "worker.py") == {"commitcore", "consumer"}
    for name in ("commitcore.py", "consumer.py"):
        assert "worker" not in _imports(HERE / name), name
    assert "commitcore" not in (HERE / "consumer.py").read_text()
    # The legacy execution chain is gone from the facade.
    for attr in ("_done", "_finish", "_fail_preserving", "_notify",
                 "_attempts", "_waiting", "_streak", "_mode", "_confirmed"):
        assert attr not in Worker.__dict__, attr
    # The adapter-facing component holds no commit adjudication state.
    for attr in ("_attempts", "_next", "_timeout", "_confirmed", "_waiting"):
        assert attr not in Consumer.__dict__, attr
    assert "lake" not in Core.__dict__ and "trace" not in Core.__dict__
    # I/O adapters carry no commit adjudication, timeout counting or stop rules.
    for name in ("lake.py", "source.py"):
        assert _imports(HERE / name) <= {"sqlite3"}, name
        text = (HERE / name).read_text()
        for word in ("streak", "timeout", "commitcore", "consumer", "worker",
                     "deadline"):
            assert word not in text, (name, word)


def check_core_without_io():
    core = Core(timeout=5, confirmed=-1)
    assert core.snapshot() == {"now": 0, "confirmed": -1, "waiting": None}
    _assert_json_roundtrip(core.snapshot())
    assert not hasattr(core, "trace")

    # Synchronous success: the core only reports effects; the frontier moves
    # only when the I/O owner confirms that the receipt write succeeded.
    event = {"type": "submit", "offset": 4}
    original = json.loads(json.dumps(event))
    effects = core.handle(event)
    assert event == original, "core must not mutate the input event"
    assert effects == [{"effect": "attempt", "token": 1, "offset": 4,
                        "deadline": 5}], effects
    _assert_json_roundtrip(effects)
    assert core.handle({"type": "submit", "offset": 5}) == [
        {"effect": "rejected", "reason": "commit pending"}]
    result = core.handle({"type": "complete", "token": 1, "error": None,
                          "active": True})
    assert result == [
        {"effect": "confirm", "token": 1, "offset": 4},
        {"effect": "health", "token": 1, "streak": 0},
        {"effect": "notify", "token": 1, "event": "ack"}], result
    _assert_json_roundtrip(result)
    assert core.snapshot()["confirmed"] == -1  # not advanced before write ack
    assert core.handle({"type": "effect_result", "effect": "confirm",
                        "token": 1, "offset": 4, "ok": True}) == []
    assert core.snapshot()["confirmed"] == 4
    assert core.snapshot()["waiting"] is None

    # A failed receipt write must leave the frontier untouched.
    core.handle({"type": "submit", "offset": 7})
    core.handle({"type": "complete", "token": 2, "error": None, "active": True})
    core.handle({"type": "effect_result", "effect": "confirm", "token": 2,
                 "offset": 7, "ok": False})
    assert core.snapshot()["confirmed"] == 4

    # Duplicate and post-request-error completions are ignored.
    assert core.handle({"type": "complete", "token": 2, "error": None,
                        "active": True}) == [{"effect": "ignored", "token": 2}]
    core.handle({"type": "submit", "offset": 9})
    core.handle({"type": "request_failed", "token": 3})
    assert core.snapshot()["waiting"] is None
    assert core.handle({"type": "complete", "token": 3, "error": None,
                        "active": True}) == [{"effect": "ignored", "token": 3}]

    # Timeout only at the deadline.
    core.handle({"type": "submit", "offset": 10})  # token 4, deadline 5
    assert core.handle({"type": "tick", "now": 4, "active": True}) == []
    assert core.handle({"type": "tick", "now": 5, "active": True}) == [
        {"effect": "timeout", "token": 4}]

    # Late success advances the frontier and still notifies ack, but produces
    # no health reset for the newer attempt and no revival.
    core.handle({"type": "submit", "offset": 11})  # token 5
    late = core.handle({"type": "complete", "token": 4, "error": None,
                        "active": False})
    assert late == [{"effect": "confirm", "token": 4, "offset": 10},
                    {"effect": "notify", "token": 4, "event": "ack"}], late
    assert core.snapshot()["waiting"] == 5
    assert core.snapshot()["confirmed"] == 4

    # Late remote failure is terminal but harmless.
    assert core.handle({"type": "complete", "token": 5, "error": "boom",
                        "active": False}) == []
    core.handle({"type": "submit", "offset": 12})  # token 6
    core.handle({"type": "tick", "now": 10, "active": True})
    assert core.handle({"type": "complete", "token": 6, "error": "late",
                        "active": False}) == []

    # Inactive ticks still advance now but never time out.
    core.handle({"type": "submit", "offset": 13})  # token 7, deadline 15
    assert core.handle({"type": "tick", "now": 20, "active": False}) == []
    assert core.snapshot()["now"] == 20 and core.snapshot()["waiting"] == 7
    try:
        core.handle({"type": "tick", "now": 19, "active": True})
    except ValueError as error:
        assert str(error) == "clock"
    else:
        raise AssertionError("expected clock error")
    assert core.snapshot()["now"] == 20

    # Snapshot isolation: callers cannot mutate internal state.
    snapshot = core.snapshot()
    snapshot["confirmed"] = 999
    snapshot["waiting"] = None
    assert core.snapshot()["confirmed"] != 999
    assert core.snapshot()["waiting"] == 7
    _assert_json_roundtrip(core.__dict__)
    _assert_json_roundtrip(core.snapshot())

    try:
        Core(timeout=0)
    except ValueError as error:
        assert str(error) == "timeout"
    else:
        raise AssertionError("expected timeout error")


class _ScriptedCore:
    """A test double implementing only the published handle/snapshot port."""

    def __init__(self):
        self.events = []
        self.now = 0
        self.confirmed = -1
        self.waiting = None
        self.tokens = iter([101, 202, 303, 404])
        self.attempts = {}

    def handle(self, event):
        self.events.append(json.loads(json.dumps(event)))
        kind = event["type"]
        if kind == "submit":
            token = next(self.tokens)
            self.attempts[token] = {"offset": event["offset"],
                                    "status": "waiting"}
            self.waiting = token
            return [{"effect": "attempt", "token": token,
                     "offset": event["offset"], "deadline": self.now + 5}]
        if kind == "request_failed":
            attempt = self.attempts[event["token"]]
            if attempt["status"] == "waiting":
                attempt["status"] = "request_error"
                self.waiting = None
            return []
        if kind == "complete":
            token = event["token"]
            attempt = self.attempts[token]
            if attempt["status"] not in ("waiting", "timeout"):
                return [{"effect": "ignored", "token": token}]
            was_waiting = attempt["status"] == "waiting"
            attempt["status"] = "success" if event["error"] is None else "error"
            if self.waiting == token:
                self.waiting = None
            if event["error"] is not None:
                if was_waiting and event["active"]:
                    return [{"effect": "remote_failure", "token": token}]
                return []
            effects = []
            if attempt["offset"] > self.confirmed:
                effects.append({"effect": "confirm", "token": token,
                                "offset": attempt["offset"]})
            if was_waiting and event["active"]:
                effects.append({"effect": "health", "token": token,
                                "streak": 0})
            effects.append({"effect": "notify", "token": token,
                            "event": "ack"})
            return effects
        if kind == "tick":
            if type(event["now"]) is not int or event["now"] < self.now:
                raise ValueError("clock")
            self.now = event["now"]
            if not event["active"] or self.waiting is None:
                return []
            token = self.waiting
            self.attempts[token]["status"] = "timeout"
            self.waiting = None
            return [{"effect": "timeout", "token": token}]
        if kind == "effect_result":
            if event.get("effect") == "confirm" and event.get("ok") is True:
                offset = event["offset"]
                if offset > self.confirmed:
                    self.confirmed = offset
            return []
        raise AssertionError("unexpected event: %r" % (event,))

    def snapshot(self):
        return {"now": self.now, "confirmed": self.confirmed,
                "waiting": self.waiting}


class _PortSpy:
    """Wraps the double and rejects any access outside the published port."""

    def __init__(self, inner):
        object.__setattr__(self, "inner", inner)
        object.__setattr__(self, "accessed", [])

    def __getattribute__(self, name):
        if name in ("inner", "accessed"):
            return object.__getattribute__(self, name)
        if name in ("handle", "snapshot"):
            object.__getattribute__(self, "accessed").append(name)
            return getattr(object.__getattribute__(self, "inner"), name)
        if name.startswith("__") and name.endswith("__"):
            return object.__getattribute__(self, name)
        raise AssertionError("Consumer bypassed the port via %r" % (name,))


def _consumer(observer=None, lake=None, trace=None, plans=None):
    trace = [] if trace is None else trace
    lake = Lake(":memory:", trace) if lake is None else lake
    source = RecordedSource(trace, plans)
    core = _ScriptedCore()
    consumer = Consumer(lake, source, _PortSpy(core), trace, observer)
    return consumer, lake, source, core, trace


def check_consumer_through_port():
    observed = []
    consumer, lake, source, core, trace = _consumer(
        lambda event, snap: observed.append([event, snap]),
        plans={101: {"sync": None}})
    assert consumer.accept({"offset": 4, "rows": [["s", 7]]}) == 101
    # Tokens 101/202 come from the injected port, never from local state.
    assert core.events[0] == {"type": "submit", "offset": 4}
    assert core.events[1] == {"type": "complete", "token": 101,
                              "error": None, "active": True}
    assert core.events[2] == {"type": "effect_result", "effect": "confirm",
                              "token": 101, "offset": 4, "ok": True}
    assert [event for event, _ in observed] == ["ack"]
    assert consumer.snapshot()["confirmed"] == 4 and lake.confirmed() == 4
    assert lake.rows() == [[4, 0, "s", 7]]
    assert ["attempt", 101, 4, 5] in trace and ["ack", 4] in trace

    # A reentrant accept during ack is a fresh port attempt, not local state.
    holder = {}
    observed = []

    def reenter(event, snap):
        observed.append(event)
        if event == "ack" and snap["last"] == 1:
            holder["token"] = holder["w"].accept({"offset": 2, "rows": []})

    consumer, lake, source, core, trace = _consumer(reenter, plans={
        101: {"sync": None}, 202: {"sync": None}})
    holder["w"] = consumer
    assert consumer.accept({"offset": 1, "rows": []}) == 101
    assert holder["token"] == 202
    assert observed == ["ack", "ack"]
    assert consumer.snapshot()["last"] == 2
    assert consumer.snapshot()["confirmed"] == 2

    # A late success for a timed-out attempt advances the frontier and
    # notifies, but never resets the lifecycle's streak.
    consumer, lake, source, core, trace = _consumer()
    consumer.accept({"offset": 1, "rows": []})  # token 101
    assert consumer.tick(0) == 101  # the scripted port owns the deadline
    assert consumer.snapshot()["streak"] == 1
    consumer.accept({"offset": 2, "rows": []})  # token 202
    source.complete(101)
    assert consumer.snapshot()["streak"] == 1
    assert consumer.snapshot()["confirmed"] == 1
    assert consumer.snapshot()["mode"] == "running"
    source.complete(202)
    assert consumer.snapshot()["streak"] == 0
    assert consumer.snapshot()["confirmed"] == 2

    # Three scripted timeouts stop once, with cleanup exactly once.
    observed = []
    consumer, lake, source, core, trace = _consumer(
        lambda event, snap: observed.append(event))
    for offset in (1, 2, 3):
        consumer.accept({"offset": offset, "rows": []})
        consumer.tick(offset)
        consumer.tick(offset)  # no waiting attempt: no extra timeout
    assert consumer.snapshot()["mode"] == "stopped"
    assert consumer.snapshot()["streak"] == 3
    assert observed == ["stopped"]
    assert trace.count(["source.close"]) == trace.count(["lake.finish"]) == 1

    # A reentrant close during ack happens inside the port callback, not after
    # request returns; the core sees the stop as the lifecycle's decision.
    observed = []

    def close_on_ack(event, snap):
        observed.append(event)
        if event == "ack":
            holder["w"].close()
            holder["w"].close()

    consumer, lake, source, core, trace = _consumer(close_on_ack, plans={
        101: {"sync": None}})
    holder["w"] = consumer
    assert consumer.accept({"offset": 1, "rows": []}) == 101
    assert observed == ["ack", "stopped"]
    assert consumer.snapshot()["mode"] == "closed"
    assert consumer.snapshot()["confirmed"] == 1
    assert trace.count(["source.close"]) == trace.count(["lake.finish"]) == 1
    # The callback itself was delivered while request was still on the stack.
    assert trace.index(["source.close"]) < trace.index(["source.return", 101])

    # Invalid clock fails in the core before any trace entry is recorded.
    consumer, lake, source, core, trace = _consumer()
    try:
        consumer.tick(-1)
    except ValueError as error:
        assert str(error) == "clock"
    assert trace == [] and consumer.snapshot()["now"] == 0
    before = len(core.events)
    try:
        consumer.accept({"offset": 1, "rows": [["", 1]]})
    except ValueError as error:
        assert str(error) == "row"
    assert len(core.events) == before and lake.rows() == []

    # A failed receipt write is reported back as a failed effect.
    trace = []
    lake = Lake(":memory:", trace)
    lake.fail["confirm"] = "receipt"
    consumer, lake, source, core, trace = _consumer(
        lake=lake, trace=trace, plans={101: {"sync": None}})
    try:
        consumer.accept({"offset": 11, "rows": [["v", 1]]})
    except RuntimeError as error:
        assert str(error) == "receipt"
    else:
        raise AssertionError("expected receipt error")
    assert core.confirmed == -1
    types = [event["type"] for event in core.events]
    assert types[:3] == ["submit", "complete", "effect_result"], types
    # The callback error travels back out of request(), so accept reports the
    # request as failed; the core has already marked the attempt terminal.
    assert types[3:] == ["request_failed"], types
    assert core.events[2] == {"type": "effect_result", "effect": "confirm",
                              "token": 101, "offset": 11, "ok": False}
    assert consumer.snapshot()["mode"] == "stopped"
    assert lake.rows() == [[11, 0, "v", 1]]


def check_worker_is_facade():
    trace = []
    lake = Lake(":memory:", trace)
    source = RecordedSource(trace)
    worker = Worker(lake, source, trace)
    assert isinstance(worker._consumer, Consumer)
    assert isinstance(worker._consumer.core, Core)
    assert not hasattr(worker, "_attempts") and not hasattr(worker, "_waiting")
    snapshot = worker.snapshot()
    snapshot["mode"] = "tampered"
    assert worker.snapshot()["mode"] == "running"
    _assert_json_roundtrip(worker.snapshot())


def verify_structure():
    check_dependencies()
    check_core_without_io()
    check_consumer_through_port()
    check_worker_is_facade()
    print("BOUNDARIES_OK core/consumer split")


if __name__ == "__main__":
    verify_structure()
