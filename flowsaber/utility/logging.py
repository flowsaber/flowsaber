"""
Modified from prefect
"""
import logging
import sys
from logging import LogRecord
from logging.handlers import MemoryHandler
from typing import Tuple


class BufferHandler(MemoryHandler):

    def emit(self, record: LogRecord):
        msg = self.format(record)
        record.message = msg
        super().emit(record)


class LogHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handler = None

    def handle(self, record):
        handler = self.handler
        if handler is not None:
            handler(record)


def create_logger(name: str, log_record_factory, logging_options) \
        -> Tuple[logging.Logger, MemoryHandler, logging.Handler]:
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

    # stream logger use user defined log level
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging_options.level)
    stream_handler.setFormatter(formatter)

    # server log handler always capture all logs
    custom_log_handler = LogHandler()
    buffer_handler = BufferHandler(
        target=custom_log_handler,
        capacity=logging_options.buffer_size,
        flushLevel=logging.DEBUG
    )
    buffer_handler.setLevel(logging.DEBUG)
    buffer_handler.setFormatter(formatter)  # this has no effect,

    logger.addHandler(stream_handler)
    logger.addHandler(buffer_handler)
    logger.setLevel(logging.DEBUG)

    return logger, buffer_handler, custom_log_handler


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
