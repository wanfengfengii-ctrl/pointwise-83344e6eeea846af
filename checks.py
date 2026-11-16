import hashlib
import json
from samples import make, batch, timeout_demo


def raises(message, action):
    try:
        action()
    except Exception as error:
        assert type(error) is RuntimeError, (type(error), str(error))
        assert str(error) == message, str(error)
    else:
        raise AssertionError("expected " + message)


def state(mode="running", now=0, last=-1, confirmed=-1, waiting=None, streak=0):
    return dict(mode=mode, now=now, last=last, confirmed=confirmed,
                waiting=waiting, streak=streak)


def run_cases():
    traces = {}
    w, lake, source, trace = make({1: {"sync": None}})
    assert w.accept(batch(4)) == 1
    assert w.snapshot() == state(last=4, confirmed=4)
    assert lake.rows() == [[4, 0, "4", 4]]
    source.complete(1)
    w.close()
    w.close()
    traces["sync_duplicate_close"] = trace

    result = timeout_demo()
    assert result["state"] == state("stopped", 15, 30, 30, None, 3)
    assert result["rows"] == [[10, 0, "10", 10], [20, 0, "20", 20], [30, 0, "30", 30]]
    traces["late_after_stop"] = result["trace"]

    w, lake, source, trace = make()
    assert w.accept(batch(10)) == 1
    raises("commit pending", lambda: w.accept(batch(20)))
    w.tick(4)
    w.tick(5)
    w.accept(batch(20))
    source.complete(2)
    source.complete(1)
    assert w.snapshot() == state(now=5, last=20, confirmed=20)
    assert lake.confirmed() == 20
    w.close()
    traces["out_of_order_frontier"] = trace

    w, lake, source, trace = make()
    w.accept(batch(1))
    w.tick(5)
    source.complete(1, "late error")
    w.accept(batch(2))
    source.complete(2, "fatal")
    raises("not accepting", lambda: w.accept(batch(3)))
    assert w.snapshot() == state("stopped", 5, 2, -1, None, 1)
    assert len(lake.rows()) == 2
    traces["remote_errors"] = trace

    holder = {}
    observed = []
    def observe(event, snap):
        observed.append([event, snap])
        if event == "ack" and snap["last"] == 1:
            assert holder["w"].accept(batch(2)) == 2
        elif event == "ack" and snap["last"] == 2:
            holder["w"].close()
        elif event == "stopped":
            holder["w"].close()
            raises("not accepting", lambda: holder["w"].accept(batch(3)))
    w, lake, source, trace = make({1: {"sync": None}, 2: {"sync": None}}, observe)
    holder["w"] = w
    assert w.accept(batch(1)) == 1
    assert w.snapshot() == state("closed", 0, 2, 2)
    assert len(lake.rows()) == 2
    assert [r[0] for r in observed] == ["ack", "ack", "stopped"]
    traces["nested_accept_close"] = trace

    w, lake, source, trace = make({1: {"sync": None, "raise": "after"}})
    raises("after", lambda: w.accept(batch(7)))
    assert w.snapshot() == state("stopped", 0, 7, 7)
    traces["success_then_request_error"] = trace

    holder = {}
    def bad_observer(event, snap):
        if event == "ack":
            holder["w"].close()
            raise RuntimeError("observer")
        raise RuntimeError("notify-stop")
    w, lake, source, trace = make({1: {"sync": None, "raise": "after"}}, bad_observer)
    holder["w"] = w
    source.close_error = "source-cleanup"
    lake.fail["finish"] = "lake-cleanup"
    # close called from the ack observer fails first with notify-stop.
    raises("notify-stop", lambda: w.accept(batch(8)))
    assert w.snapshot() == state("closed", 0, 8, 8)
    assert trace.count(["source.close"]) == trace.count(["lake.finish"]) == 1
    traces["nested_cleanup_priority"] = trace

    def explode(event, snap):
        raise RuntimeError("observer")
    w, lake, source, trace = make({1: {"sync": None, "raise": "after"}}, explode)
    source.close_error = "source-cleanup"
    lake.fail["finish"] = "lake-cleanup"
    raises("observer", lambda: w.accept(batch(9)))
    assert w.snapshot() == state("stopped", 0, 9, 9)
    assert trace.count(["source.close"]) == trace.count(["lake.finish"]) == 1
    traces["primary_callback_error"] = trace

    w, lake, source, trace = make({1: {"sync": None}})
    lake.fail["confirm"] = "receipt"
    source.close_error = "source-cleanup"
    raises("receipt", lambda: w.accept(batch(11)))
    assert w.snapshot() == state("stopped", 0, 11)
    assert lake.confirmed() == -1 and lake.rows() == [[11, 0, "11", 11]]
    source.complete(1)
    assert lake.confirmed() == -1
    traces["receipt_rollback_consumed"] = trace

    w, lake, source, trace = make()
    lake.fail["write"] = "landing"
    raises("landing", lambda: w.accept(batch(12)))
    assert w.snapshot() == state("stopped")
    assert lake.rows() == [] and source.callbacks == {}
    traces["landing_failure"] = trace

    w, lake, source, trace = make()
    for n in (1, 2, 3):
        w.accept(batch(n))
        if n == 3:
            source.close_error = "source-cleanup"
            lake.fail["finish"] = "lake-cleanup"
            raises("source-cleanup", lambda: w.tick(15))
        else:
            w.tick(n * 5)
    assert w.snapshot() == state("stopped", 15, 3, -1, None, 3)
    source.complete(3)
    w.close()
    assert lake.confirmed() == 3
    assert trace.count(["source.close"]) == trace.count(["lake.finish"]) == 1
    traces["third_timeout_cleanup"] = trace

    w, lake, source, trace = make({1: {"raise": "request"}})
    raises("request", lambda: w.accept(batch(13)))
    source.complete(1)
    assert w.snapshot() == state("stopped", 0, 13)
    assert lake.rows() == [[13, 0, "13", 13]]
    traces["request_failure_consumed"] = trace
    return traces


# Frozen full-trace digests: these assertions are part of the public baseline.
FROZEN = {
    "sync_duplicate_close": "1bf35bb511121989e72dce3492fd8d7016877e8cd70ab1d8b504bd720b89cfa3",
    "late_after_stop": "63c0487e896e4ba9193af18978f970fa5122121dc5e093988d75e92beb073c4e",
    "out_of_order_frontier": "43c7f5ad035c2e03f37d58218e4adc2a717c6799e1c230d52c54bb215698f7ef",
    "remote_errors": "e0f770d54aa61538cf8fd0ed65489bd173a5a9c6d3e20f500e45951c45c3699a",
    "nested_accept_close": "e687708df5d954ca9c8bd67426121aa2b335e974f2cfd455d2280cbf33e06d0e",
    "success_then_request_error": "2c7e6d9199ee41950158ffc8e1e9735cde6012a6c456bf90bb0bd8a3cf4814ae",
    "nested_cleanup_priority": "2a08eb477f7b6e85f7e8b1519771f95b36f373b944cc45df1fd198cb42b4ab70",
    "primary_callback_error": "d20b97209dbf78f16122f10ad055c9a9301d4332d467445c310c0998713979c3",
    "receipt_rollback_consumed": "394894d910121b8c9e78cc851fcc6b2042166f26c070b9be3d43db2edb651696",
    "landing_failure": "d2026c548475ff78408df0891311bdc74507f2e85279d529a6e66c9a66531759",
    "third_timeout_cleanup": "ce17d579f4c50a3ca232557bfc7fbbb93e016ae54653308c8ad26e702b072003",
    "request_failure_consumed": "20061fefdee53028b02b8481ec80806b0ab76df1f87d3826309778b447093227"
}


def main():
    traces = run_cases()
    got = {name: hashlib.sha256(json.dumps(trace, ensure_ascii=False,
            separators=(",", ":")).encode()).hexdigest() for name, trace in traces.items()}
    assert got == FROZEN, got
    print("BASELINE_OK", len(traces), "full event recordings")


if __name__ == "__main__":
    main()
