import logging
import sys

LOG_LEVEL = logging.DEBUG


def get_logger(name, file_=sys.stderr, level=LOG_LEVEL):
    """
    comes from https://github.com/GangCaoLab/CoolBox/blob/master/coolbox/utilities/logtools.py
    """
    FORMAT = "[%(levelname)s:%(filename)s:%(lineno)s - %(funcName)5s()] %(message)s"
    formatter = logging.Formatter(fmt=FORMAT)
    if isinstance(file_, str):
        handler = logging.FileHandler(file_)
    else:
        handler = logging.StreamHandler(file_)
    handler.setFormatter(formatter)
    log = logging.getLogger(name)
    log.addHandler(handler)
    log.setLevel(level)
    return log
