"""
Modified from prefect
"""
import atexit
import logging
import sys
import threading
from queue import Queue
from typing import Optional

import flowsaber

from contextvars import ContextVar


def inject_context(factory):
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
    def __init__(self, buffer_size=1, **kwargs):
        super().__init__(**kwargs)
        self.log_queue = Queue()
        self.buffer_size = buffer_size
        self.buffer = []
        self.record_handlers = []
        self.thread: Optional[threading.Thread] = None
        self.stopped = threading.Event()

    def add_handlers(self, handler):
        self.record_handlers.append(handler)

    def emit(self, record: logging.LogRecord) -> None:
        self.log_queue.put_nowait(record)

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


log_manager = ThreadLogManager(buffer_size=flowsaber.context.buffer_size)
logger = create_logger("flowsaber")


def get_logger(name: str) -> logging.Logger:
    return logger.getChild(name)
