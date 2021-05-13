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

id
flowrun_name

taskrun_id
```

"""

import asyncio
import inspect
import logging
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from functools import partial
from typing import List, Optional, TYPE_CHECKING

from flowsaber.core.default_context import DEFAULT_CONTEXT
from flowsaber.core.utility.cache import Cache, get_cache
from flowsaber.core.utility.executor import Executor, get_executor
from flowsaber.utility.context import Context
from flowsaber.utility.logging import create_logger

if TYPE_CHECKING:
    from flowsaber.core.flow import Flow


class FlowSaberContext(Context):
    """The global coroutine-safe context meant to be used for inferring the running status of a flow at any time.
    Compared to raw context, this global context has intelligent(automatically change according to the
    time/position of the callee) properties:
        cache: used by running flow/task
        run_lock: used by running task
        logger: used in anywhere and anytime
        executor: used by running task

    """
    EXECUTOR_TABLE = '__executors'
    LOCK_TABLE = '__locks'
    CACHE_TABLE = "__caches"
    LOGGER_TABLE = '__loggers'
    FLOW_STACK_LIST = '__flow_stack'

    @property
    def random_id(self) -> str:
        return str(uuid.uuid4())

    @property
    def flow_stack(self) -> List['Flow']:
        return self._info.setdefault(self.FLOW_STACK_LIST, [])

    @property
    def top_flow(self) -> Optional['Flow']:

        return self.flow_stack[0] if self.flow_stack else None

    @property
    def up_flow(self) -> Optional['Flow']:
        return self.flow_stack[-1] if self.flow_stack else None

    # utility instance based on the current context
    async def __aenter__(self):
        """initialize executors
        Returns
        -------

        """
        # start executor
        executors = self._info.setdefault(self.EXECUTOR_TABLE, {})
        for executor_config in self.executors:
            executor_type = executor_config['executor_type']
            executors[executor_type] = get_executor(**executor_config)
        for executor in executors.values():
            self.logger.info(f"Starting executor: {executor}")
            await executor.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        executors = self._info[self.EXECUTOR_TABLE]  # must exists, or raise error
        # dask distributed has Warning about future was never awaited
        # https://github.com/dask/distributed/pull/3921
        for executor in executors.values():
            self.logger.info(f"Stopping executor: {executor}")
            await executor.__aexit__(exc_type, exc_val, exc_tb)
        executors.clear()
        del self._info[self.EXECUTOR_TABLE]

    @property
    def executor(self) -> Executor:
        """Fetch an initialized executor based on `executor_type` in the current context.
        Returns
        -------

        """
        assert 'executor_type' in self, "Can not find 'executor_type in context. You are not within a task."
        assert self.EXECUTOR_TABLE in self._info, 'Executors are not initialized, use async with statement ' \
                                                  'to initialize context'
        executors = self._info[self.EXECUTOR_TABLE]
        executor_type = self.executor_type
        if executor_type not in executors:
            self.logger.warning(f"The executor: {executor_type} not found. fall back to local")
            executor_type = "local"  # set to local
        return executors[executor_type]

    @asynccontextmanager
    async def lock(self, keys: List[str]):
        """Fetch a asyncio.Lock based on `run_workdir` in the current context.
        Returns
        -------

        """
        lock_table = self._info.setdefault(self.LOCK_TABLE, defaultdict(asyncio.Lock))
        locks = tuple(lock_table[key] for key in keys)
        try:
            for lock in locks:
                await lock.acquire()
            yield locks
        finally:
            for lock in locks:
                lock.release()

    @property
    def cache(self, ) -> Cache:
        """Fetch a Cache based on `cache_type` in the current context.
        Returns
        -------

        """
        cache = self._info.setdefault(
            self.CACHE_TABLE,
            default=defaultdict(partial(get_cache, self.cache_type))
        )
        return cache[self.cache_type]

    @property
    def logger(self) -> logging.Logger:
        """Get a child logger of `flowsaber` logger with name of:
        `callee.__name__.agent_id.flow_id.id.task_id.taskrun_id`

        Returns
        -------

        """
        # find callee
        callee_frame = inspect.currentframe().f_back
        callee_module_name = callee_frame.f_globals['__name__']
        # TODO can add more running infos
        info_name = "NULL"

        logger_name = f"{callee_module_name}.{info_name}".rstrip('.')
        loggers = self._info.setdefault(self.LOGGER_TABLE, {})
        if logger_name not in loggers:
            loggers[logger_name] = flowsaber_logger.getChild(logger_name)

        return loggers[logger_name]


def inject_context_attrs(factory):
    """Inject context attrs into the log record base on context.context_attrs.
    Parameters
    ----------
    factory

    Returns
    -------

    """

    # TODO now users must explicitly specify attrs need to be attached to log_record
    # TODO Formatter._style.format has no ways to handle missing attts, thus causing errors
    def inner(*args, **kwargs):
        import flowsaber
        record = factory(*args, **kwargs)
        try:
            # TODO, sometimes, logging does not exist in flowsaber.context?
            context_attrs = flowsaber.context.logging.context_attrs or []
            for attr in context_attrs:
                setattr(record, attr, context.get(attr, ''))
        except Exception:
            pass
        return record

    return inner


context = FlowSaberContext()
context.update(DEFAULT_CONTEXT)

log_record_factory = logging.getLogRecordFactory()
log_record_factory = inject_context_attrs(log_record_factory)

# if use `logger` as name, will coflict with logger in dask
flowsaber_logger, buffer_handler, log_handler = create_logger("flowsaber", log_record_factory, context.logging)
