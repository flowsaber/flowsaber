"""
Modified from prefect
"""
import logging
import sys
from datetime import datetime
from logging import LogRecord
from logging.handlers import MemoryHandler, QueueListener
from queue import SimpleQueue
from typing import List, Tuple

from flowsaber.server.database import RunLogInput


class BatchQueueHandler(MemoryHandler):
    def __init__(self, queue=None, **kwargs):
        super().__init__(**kwargs)
        self.queue = queue or SimpleQueue()

    def flush(self):
        """
        For a MemoryHandler, flushing means just sending the buffered
        records to the target, if there is one. Override if you want
        different behaviour.

        The record buffer is also cleared by this operation.
        """
        self.acquire()
        try:
            batch_records = [
                self.prepare(record) for record in self.buffer
            ]
            if batch_records:
                self.queue.put_nowait(batch_records)
            self.buffer = []
        finally:
            self.release()

    def prepare(self, record: LogRecord) -> LogRecord:
        msg = self.format(record)
        record.message = record.msg = msg
        return record


class LogManager(QueueListener):
    def prepare(self, records: List[LogRecord]) -> List[RunLogInput]:
        run_logs = []
        for record in records:
            record_dict = {
                'level': record.levelname,
                'time': datetime.fromtimestamp(record.created),
                'task_id': getattr(record, 'task_id', None),
                'flow_id': getattr(record, 'flow_id', None),
                'taskrun_id': getattr(record, 'taskrun_id', None),
                'flowrun_id': getattr(record, 'flowrun_id', None),
                'agent_id': getattr(record, 'agent_id', None),
                'message': record.message
            }
            run_log = RunLogInput(**record_dict)
            run_logs.append(run_log)

        return run_logs

    def start(self) -> None:
        if self._thread is None:
            super().start()

    def stop(self):
        if self._thread is not None:
            super().stop()

    def add_handler(self, handler: logging.Handler):
        if isinstance(self.handlers, tuple):
            self.handlers = list(self.handlers)
        self.handlers.append(handler)


def create_logger(name: str, log_record_factory, logging_options) -> Tuple[logging.Logger, LogManager]:
    """
    comes from https://github.com/GangCaoLab/CoolBox/blob/master/coolbox/utilities/logtools.py
    """
    formatter = logging.Formatter(
        fmt=logging_options.fmt,
        datefmt=logging_options.datefmt,
        style=logging_options.style
    )

    logging.setLogRecordFactory(log_record_factory)
    logger = logging.getLogger(name)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    batch_handler = BatchQueueHandler(capacity=logging_options.buffer_size, flushLevel=logging.DEBUG)
    batch_handler.setFormatter(formatter)
    log_manager = LogManager(queue=batch_handler.queue)

    logger.addHandler(stream_handler)
    logger.addHandler(batch_handler)
    logger.setLevel(logging_options.level)

    return logger, log_manager


class RedirectToLog:
    """
    Custom redirect of stdout messages to logs

    Args:
        - logger (logging.Logger, optional): an optional logger to redirect stdout. If
            not provided a logger names `stdout` will be created.
    """

    def __init__(self, logger: logging.Logger, level: int) -> None:
        self.logger: logging.Logger = logger
        self.level: int = level

    def write(self, msg: str) -> None:
        """
        Write message from stdout to a prefect logger.
        Note: blank newlines will not be logged.

        Args:
            msg (str): the message
        """
        if not isinstance(msg, str):
            # stdout is expecting str
            raise TypeError(f"string argument expected, got {type(msg)}")

        if msg.strip():
            self.logger.log(self.level, msg)

    def flush(self) -> None:
        """
        Implemented flush operation for logger handler
        """
        for handler in self.logger.handlers:
            handler.flush()
