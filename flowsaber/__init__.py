__version__ = '0.1.3.3'

from flowsaber.utility.logtool import *

from flowsaber.core import *
from flowsaber.core.operators import *
from flowsaber.core.utility.context import context
from flowsaber.utility.utils import *


async def run(build_flow: Flow) -> asyncio.Future:
    with context():
        async with TaskScheduler().start() as scheduler:
            executor_type = config.executor['executor_type']
            async with get_executor(executor_type, **config.executor).start() as executor:
                loop = asyncio.get_running_loop()
                loop._scheduler = scheduler
                context.__dict__['__executor__'] = executor

                res = await build_flow.execute(scheduler=scheduler)
            await asyncio.sleep(1)

    return res
