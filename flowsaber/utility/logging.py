"""
Modified from prefect
"""
import atexit
import inspect
import logging
import sys
import threading
from queue import Queue
from typing import Optional

import flowsaber
from flowsaber.core.context import FlowSaberContext
from flowsaber.core.utils import extend_method


@extend_method(FlowSaberContext)
class _(object):
    """Prevent circular import
    """

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


def inject_context(factory):
    """Inject context attrs into the log record.
    Parameters
    ----------
    factory

    Returns
    -------

    """

    def inner(*args, **kwargs):
        record = factory(*args, **kwargs)
        context_attrs = flowsaber.context.logging.context_attrs or flowsaber.context.keys()
        for attr in context_attrs:
            setattr(record, attr, flowsaber.context.get(attr))

        return record

    return inner


log_record_factory = logging.getLogRecordFactory()
log_record_factory = inject_context(log_record_factory)


class ThreadLogManager(logging.Handler):
    """A logger handler can be registered with log handlers, logs in batch will be passed to registered handlers and
    processed in another thread.
    """

    def __init__(self, buffer_size=1, max_buffer_size=2000, **kwargs):
        super().__init__(**kwargs)
        self.log_queue = Queue()
        self.buffer_size = buffer_size
        self.max_buffer_size = max_buffer_size
        self.buffer = []
        self.record_handlers = []
        self.thread: Optional[threading.Thread] = None
        self.stopped = threading.Event()

    def add_handlers(self, handler):
        self.record_handlers.append(handler)

    def emit(self, record: logging.LogRecord) -> None:
        self.log_queue.put_nowait(record)
        if self.log_queue.qsize() > self.max_buffer_size:
            self.log_queue.get_nowait()

    def start(self):
        def on_shutdown():
            for _ in range(3):
                try:
                    self.stop()
                    return
                except SystemExit:
                    pass

        if self.thread is None:
            self.thread = threading.Thread(target=self.loop, daemon=True)
            self.thread.start()
            atexit.register(on_shutdown)

    def stop(self):
        if self.thread is not None:
            self.stopped.set()
            self.thread.join()
            self.call_handlers()
            self.thread = None
            self.stopped.clear()

    def loop(self):
        while not self.stopped.wait(3):
            while len(self.buffer) < self.buffer_size:
                self.buffer.append(self.log_queue.get())
            self.call_handlers()
            self.buffer.clear()

    def call_handlers(self):
        for handler in self.record_handlers:
            handler(self.buffer)


def create_logger(name: str) -> logging.Logger:
    """
    comes from https://github.com/GangCaoLab/CoolBox/blob/master/coolbox/utilities/logtools.py
    """
    logging.setLogRecordFactory(log_record_factory)
    logger = logging.getLogger(name)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        flowsaber.context.logging.format, flowsaber.context.logging.datefmt
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(log_manager)
    logger.setLevel(flowsaber.context.logging.level)

    return logger


log_manager = ThreadLogManager(buffer_size=flowsaber.context.logging.buffer_size)
logger = create_logger("flowsaber")


def get_logger(name: str) -> logging.Logger:
    return logger.getChild(name)
