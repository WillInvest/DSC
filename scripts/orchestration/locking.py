"""Use OS advisory locks for local coordinator and tracker ownership."""
from contextlib import contextmanager
import fcntl


@contextmanager
def file_lock(path, shared=False, blocking=True):
    with path.open('a') as stream:
        operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        fcntl.flock(stream, operation | (0 if blocking else fcntl.LOCK_NB))
        yield
