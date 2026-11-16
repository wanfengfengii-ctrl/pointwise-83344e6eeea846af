import sqlite3


class Lake:
    """Append-only landing rows plus the durable source receipt."""
    def __init__(self, path, trace):
        self.trace = trace
        self.db = sqlite3.connect(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS rows_seen(
                offset INTEGER, pos INTEGER, id TEXT, value INTEGER,
                PRIMARY KEY(offset,pos));
            CREATE TABLE IF NOT EXISTS receipt(id INTEGER PRIMARY KEY, offset INTEGER);
            INSERT OR IGNORE INTO receipt VALUES(1,-1);
        """)
        self.fail = {}

    def _trip(self, name):
        error = self.fail.pop(name, None)
        if error is not None:
            raise RuntimeError(error)

    def write(self, offset, rows):
        self.trace.append(["lake.write", offset, rows])
        self._trip("write")
        with self.db:
            self.db.executemany("INSERT INTO rows_seen VALUES(?,?,?,?)",
                                [(offset, i, r[0], r[1]) for i, r in enumerate(rows)])

    def confirm(self, offset):
        self.trace.append(["lake.confirm", offset])
        with self.db:
            self.db.execute("UPDATE receipt SET offset=max(offset,?) WHERE id=1", (offset,))
            self._trip("confirm")

    def rows(self):
        return [list(r) for r in self.db.execute(
            "SELECT offset,pos,id,value FROM rows_seen ORDER BY offset,pos")]

    def confirmed(self):
        return self.db.execute("SELECT offset FROM receipt WHERE id=1").fetchone()[0]

    def finish(self):
        self.trace.append(["lake.finish"])
        self._trip("finish")
