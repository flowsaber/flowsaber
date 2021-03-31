import logging
import sys

LOG_LEVEL = logging.INFO


def get_logger(name, file_=sys.stderr, level=LOG_LEVEL):
    """
    comes from https://github.com/GangCaoLab/CoolBox/blob/master/coolbox/utilities/logtools.py
    """
    FORMAT = "[%(levelname)6s:%(filename)10s:%(lineno)3s - %(funcName)15s()] %(message)s"
    formatter = logging.Formatter(fmt=FORMAT)
    if isinstance(file_, str):
        handler = logging.FileHandler(file_)
    else:
        handler = logging.StreamHandler(file_)
    handler.setFormatter(formatter)
    log = logging.getLogger(name)
    log.addHandler(handler)
    log.setLevel(level)

    class NoParsingFilter(logging.Filter):
        def filter(self, record):
            return True

    log.addFilter(NoParsingFilter())

    return log
