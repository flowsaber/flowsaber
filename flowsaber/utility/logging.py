"""
Modified from prefect
"""
import logging
import sys
from datetime import datetime
from logging.handlers import MemoryHandler, QueueHandler
from queue import SimpleQueue
from typing import Tuple

from flowsaber.server.database import RunLogInput


class LogQueueHandler(QueueHandler):

    def prepare(self, record) -> RunLogInput:
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
        return run_log


def create_logger(name: str, log_record_factory, logging_options) -> Tuple[logging.Logger, LogQueueHandler]:
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
    buffer_handler = MemoryHandler(capacity=logging_options.buffer_size, flushLevel=logging.DEBUG)
    queue_handler = LogQueueHandler(queue=SimpleQueue())
    buffer_handler.setTarget(queue_handler)
    stream_handler.setFormatter(formatter)
    queue_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(buffer_handler)
    logger.setLevel(logging_options.level)

    return logger, queue_handler


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
