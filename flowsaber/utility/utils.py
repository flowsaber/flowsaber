# TODO try to make things more simple for ease of pickle problems

import os
import sys
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from typing import Union


@contextmanager
def change_cwd(path: Union[str, Path]) -> Path:
    """
    A context manager which changes the working directory to the given
    path, and then changes it back to its previous _output on exit.
    Usage:
    > # Do something in original directory
    > with working_directory('/my/new/path'):
    >     # Do something in new directory
    > # Back to old directory
    """
    path = Path(path).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    try:
        prev_cwd = os.getcwd()
        os.chdir(path)
    except Exception as e:
        raise e

    try:
        yield Path(path)
    finally:
        os.chdir(prev_cwd)


@contextmanager
def capture_local(event='return'):
    import sys
    local_vars = {}

    def capture_tracer(frame, _event, arg=None):
        if _event == event:
            local_vars.update(frame.f_locals)
        else:
            return capture_tracer

    sys.settrace(capture_tracer)

    yield local_vars

    sys.settrace(None)


class CaptureTerminal(object):
    def __init__(self, stdout=None, stderr=None):
        if isinstance(stdout, str):
            stdout = open(stdout, 'a')
        if isinstance(stderr, str):
            stderr = open(stderr, 'a')
        self.stdout = stdout
        self.stderr = stderr
        self.stringio_stdout = StringIO()
        self.stringio_stderr = StringIO()

    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = self.stdout or self.stringio_stdout
        sys.stderr = self.stderr or self.stringio_stderr
        return self

    def __exit__(self, *args):
        self.stringio_stdout = StringIO()
        self.stringio_stderr = StringIO()
        sys.stdout = self._stdout
        sys.stderr = self._stderr
