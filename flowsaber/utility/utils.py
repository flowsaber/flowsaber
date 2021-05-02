# TODO try to make things more simple for ease of pickle problems

import importlib
import os
import sys
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from typing import Any, Union, Generator


def import_object(name: str) -> Any:
    """Import an object given a fully-qualified name.

    Args:
        - name (string): The fully-qualified name of the object to import.

    Returns:
        - obj: The object that was imported.

    Example:

    ```python
    >>> obj = import_object("random.randint")
    >>> import random
    >>> obj == random.randint
    True
    ```
    """
    try:
        mod_name, attr_name = name.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, attr_name)
    except ValueError:
        return importlib.import_module(name)


@contextmanager
def change_cwd(path: Union[str, Path]) -> Generator[Path, None, None]:
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
    prev_cwd = os.getcwd()
    os.chdir(path)

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


def interrupt_main():
    import os
    if os.name == 'nt':
        from _thread import interrupt_main
        interrupt_main()
    else:
        import signal
        import threading
        signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)


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
