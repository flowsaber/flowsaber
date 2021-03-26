import asyncio
import inspect
from functools import partial
from multiprocessing import cpu_count

from loky import get_reusable_executor

from pyflow.context import config
from pyflow.utility.logtool import get_logger

logger = get_logger(__name__)


class Executor(object):
    async def run(self, fn, *args):
        raise NotImplementedError


class Local(Executor):
    async def run(self, fn, *args, **kwargs):
        if inspect.iscoroutine(fn):
            return await fn(*args, **kwargs)
        else:
            return fn(*args, **kwargs)


class ProcessExecutor(Executor):
    # TODO ProcessExecutor will not work, need to find a way to use cloudpicke as pickler
    def __init__(self, pool_cls=get_reusable_executor):
        self.pool = pool_cls()

    async def run(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        _run = partial(fn, *args, **kwargs)
        logger.debug("run in executor")
        res = await loop.run_in_executor(self.pool, _run)
        logger.debug(f"result is: {res}")
        return res


class RayExecutor(Executor):
    def __init__(self, config):
        self.config = config
        self.inited = False

    def init(self):
        if not self.inited:
            import ray
            ray.init(num_cpus=cpu_count())
            self.inited = True

    async def run(self, fn, *args, **kwargs):
        import ray
        self.init()

        # TODO why use closure doesn't work ?
        @ray.remote
        def _run(*args, **kwargs):
            return fn(*args, **kwargs)

        res = await _run.remote(*args, **kwargs)
        return res


local = Local()
process = ProcessExecutor()
ray = RayExecutor(config=config)


def get_executor(executor: str = 'local'):
    return {
        'local': local,
        'process': process,
        'ray': ray
    }[executor]
