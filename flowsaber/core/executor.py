import asyncio
import inspect
import sys
from functools import partial
from io import StringIO
from multiprocessing import cpu_count

from flowsaber.utility.logtool import get_logger

logger = get_logger(__name__)


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


class Executor(object):
    def __init__(self, config):
        self.config = config

    async def run(self, fn, *args, **kwargs):
        raise NotImplementedError

    def init(self):
        pass

    def shutdown(self):
        pass


class Local(Executor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def run(self, fn, *args, **kwargs):
        if inspect.iscoroutine(fn):
            return await fn(*args, **kwargs)
        else:
            return fn(*args, **kwargs)


class ProcessExecutor(Executor):
    # TODO ProcessExecutor will not work, need to find a way to use cloudpickle as pickler
    def __init__(self, pool_cls=None, **kwargs):
        super().__init__(**kwargs)
        if pool_cls is None:
            from loky import get_reusable_executor
            pool_cls = get_reusable_executor
        self.pool = pool_cls()

    async def run(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        _run = partial(fn, *args, **kwargs)
        res = await loop.run_in_executor(self.pool, _run)
        return res


class RayExecutor(Executor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.inited = False

    def init(self):
        if not self.inited:
            import ray
            with CaptureTerminal('/tmp/.context.ray_stdout.log', '/tmp/.context.ray_stderr.log'):
                ray.init(num_cpus=max(cpu_count() - 2, 1))
            self.inited = True

    def shutdown(self):
        if self.inited:
            import ray
            ray.shutdown()
            self.inited = False

    async def run(self, fn, *args, **kwargs):
        import ray
        # assert self.inited, "Not inited, please use executor.init()"
        # TODO why ray is not inited in github actions
        self.init()

        # TODO why use closure doesn't work ?
        @ray.remote
        def _run(*args, **kwargs):
            return fn(*args, **kwargs)

        res = await _run.remote(*args, **kwargs)
        return res


def get_executor(executor: str = 'local', *args, **kwargs):
    executors = {
        'local': Local,
        'process': ProcessExecutor,
        'ray': RayExecutor
    }
    if executor not in executors:
        raise ValueError(f"{executor} not supported, please choose one of {executors.keys()}")
    exe = executors[executor](*args, **kwargs)
    exe.init()
    return exe
