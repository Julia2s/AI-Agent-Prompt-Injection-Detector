import contextlib
import io
import sys


@contextlib.contextmanager
def quiet_run(run):
    active = run.__enter__()
    try:
        yield active
    finally:
        with contextlib.redirect_stdout(io.StringIO()):
            run.__exit__(*sys.exc_info())
