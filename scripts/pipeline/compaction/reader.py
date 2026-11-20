"""Hold a shared snapshot lock while opening/reading canonical interval files."""
from contextlib import contextmanager
import fcntl
from pathlib import Path
import sqlite3


@contextmanager
def snapshot(root):
    root=Path(root)
    with (root/'catalog.lock').open('r') as lock:
        fcntl.flock(lock,fcntl.LOCK_SH)
        db=sqlite3.connect('file:'+str(root/'catalog.sqlite')+'?mode=ro',uri=True)
        db.row_factory=sqlite3.Row
        try:
            yield [{**dict(r),'absolute_path':str(root/r['path'])}
                   for r in db.execute('select * from files order by stream,start')]
        finally:db.close()
