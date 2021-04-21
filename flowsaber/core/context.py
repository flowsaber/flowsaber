"""

During the running of flow, flowsaber.context will automatically be updated, Ideally, at the end of a task run,
context will be looked like this:

```
flow_config: {}
task_config: {}

flow_id
flow_name
flow_full_name
flow_labels

task_id
task_name
task_full_name
task_labels

flowrun_id
flowrun_name

taskrun_id
```

"""

import asyncio
import inspect
import logging
from collections import defaultdict
from functools import partial

from flowsaber.core.utility.cache import Cache, get_cache
from flowsaber.core.utility.executor import Executor, get_executor
from flowsaber.utility.context import Context
from flowsaber.utility.logging import get_logger


class FlowSaberContext(Context):
    """The global coroutine-safe context meant to be used for inferring the running status of a flow at any time.
    Compared to raw context, this global context has intelligent(automatically change according to the
    time/position of the callee) properties:
        cache: used by running flow/task
        run_lock: used by running task
        logger: used in anywhere and anytime
        executor: used by running task

    """

    # utility instance based on the current context
    async def __aenter__(self):
        """Initializing executors.
        Returns
        -------

        """
        executors = self.__info.setdefault('__executors', {})
        for executor_config in self.executors:
            executor_type = executor_config['executor_type']
            executors[executor_type] = get_executor(**executor_config)
        for executor in executors:
            await executor.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        executors = self.__info.setdefault('__executors', {})
        # TODO, how to pass __aexit__ parameters?
        for executor in executors:
            await executor.__aexit__(exc_type, exc_val, exc_tb)
        executors.clear()

    @property
    def run_lock(self) -> asyncio.Lock:
        """Fetch a asyncio.Lock based on `run_workdir` in the current context.
        Returns
        -------

        """
        locks = self.__info.setdefault('__run_locks', default=defaultdict(asyncio.Lock))
        assert 'run_workdir' in self, "Can not find 'run_workdir' in context. You are not within a task."
        run_workdir = self.run_workdir
        return locks[run_workdir]

    @property
    def cache(self, ) -> Cache:
        """Fetch a Cache based on `cache_type` in the current context.
        Returns
        -------

        """
        cache = self.__info.setdefault('__cache', default=partial(get_cache, self.cache_type))
        return cache

    @property
    def executor(self) -> Executor:
        """Fetch an initialized executor based on `executor_type` in the current context.
        Returns
        -------

        """
        assert 'executor_type' in self, "Can not find 'executor_type in context. You are not within a task."
        executors = self.__info.setdefault('__executors', {})
        executor_type = self.executor_type
        if executor_type not in executors:
            raise RuntimeWarning(f"The executor: {executor_type} not found. fall back to local")
        return executors[executor_type]

    @property
    def logger(self) -> logging.Logger:
        """Get a child logger of `flowsaber` logger with name of:
        `callee.__name__.agent_id.flow_id.flowrun_id.task_id.taskrun_id`

        Returns
        -------

        """
        # find callee
        callee_frame = inspect.currentframe().f_back
        callee_module_name = callee_frame.f_globals['__name__']
        # find running info
        run_infos = [self.get(attr, 'NULL') for attr in
                     ['agent_id', 'flow_id', 'flowrun_id', 'task_id', 'taskrun_id']]
        run_name = '.'.join(run_infos)

        logger_name = f"{callee_module_name}.{run_name}".rstrip('.')

        return get_logger(logger_name)


context = FlowSaberContext()

context.update({
    'default_flow_config': {
        'test__': {
            'test__': [1, 2, 3]
        }
    },
    'default_task_config': {
        'test__': {
            'test__': [1, 2, 3]
        }
    },
    'logging': {
        'format': "[%(levelname)6s:%(filename)10s:%(lineno)3s-%(funcName)15s()] %(message)s",
        'datefmt': "%Y-%m-%d %H:%M:%S%z",
        'level': 0,
        'buffer_size': 10,
        'context_attrs': None,
    },
    'executors': [
        {
            'executor_type': 'local'
        },
        {
            'executor_type': 'dask',
            'address': None,
            'cluster_class': None,
            'cluster_kwargs': None,
            'adapt_kwargs': None,
            'client_kwargs': None,
            'debug': False
        }
    ]
})
