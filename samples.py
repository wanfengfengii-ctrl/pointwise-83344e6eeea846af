from lake import Lake
from source import RecordedSource
from worker import Worker


def batch(offset, value=None):
    return {"offset": offset, "rows": [[str(offset), offset if value is None else value]]}


def make(plans=None, observer=None):
    trace = []
    lake = Lake(":memory:", trace)
    source = RecordedSource(trace, plans)
    worker = Worker(lake, source, trace, observer)
    return worker, lake, source, trace


def timeout_demo():
    worker, lake, source, trace = make()
    worker.accept(batch(10))
    worker.tick(5)
    worker.accept(batch(20))
    worker.tick(10)
    source.complete(1)
    worker.accept(batch(30))
    worker.tick(15)
    source.complete(2)
    source.complete(3)
    return {"state": worker.snapshot(), "rows": lake.rows(), "trace": trace}


if __name__ == "__main__":
    import json
    print(json.dumps(timeout_demo(), ensure_ascii=False, indent=2))
