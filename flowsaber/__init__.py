__version__ = '0.1.3.3'

from .context import *
from .core import *
from .operators import *
from .utility.logtool import *
from .utility.utils import *


async def run(build_flow: Flow) -> asyncio.Future:
    from rich import print
    print("Config is: ", config.__dict__)

    with Scheduler() as scheduler:
        loop = asyncio.get_running_loop()
        loop._scheduler = scheduler

        res = await build_flow.execute(scheduler=scheduler)
    await asyncio.sleep(1)
    for executor in context.get('__executors__', {}).values():
        executor.shutdown()

    return res
