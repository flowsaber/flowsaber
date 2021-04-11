"""
Some codes are borrowed from https://github.com/PrefectHQ/prefect/blob/master/src/prefect/engine/runner.py
"""

import functools
from typing import Callable

from ..utils.state import *


class RunException(Exception):
    def __init__(self, state: State):
        self.state = state


def catch_to_failure(method: Callable[..., State]) -> Callable[..., State]:
    @functools.wraps(method)
    def catch_exception_to_failure(self: "Runner", *args, **kwargs) -> State:
        try:
            new_state = method(self, *args, **kwargs)
        except Exception as exc:
            e_str = f"Unexpected error: {exc} when calling method: {method}"
            new_state = Failure(result=exc, message=e_str)
        return new_state

    return catch_exception_to_failure


def call_state_change_handlers(method: Callable[..., State]) -> Callable[..., State]:
    @functools.wraps(method)
    def check_and_run(self: "Runner", state: State, *args, **kwargs) -> State:
        new_state = method(self, state, *args, **kwargs)

        if new_state is not state:
            new_state = self.handle_state_change(state, new_state)

        return new_state

    return check_and_run


class Runner(object):
    def __init__(self, logger=None):
        self.state_change_handlers = []
        self.logger = logger

    def initialize_run(self, state) -> State:
        return state

    def handle_state_change(self, prev_state, cur_state):
        handler = None
        try:
            for handler in self.state_change_handlers:
                cur_state = handler(self, prev_state, cur_state) or cur_state
        except Exception as exc:
            e_str = f"Unexpected error: {exc} when calling state_handler: {handler}"
            if self.logger:
                self.logger.exception(e_str)
            cur_state = Failure(result=exc, message=e_str)
        return cur_state

    def run(self, state: State) -> State:
        raise NotImplementedError

    @call_state_change_handlers
    def set_state(self, old_state: State, state_type: type):
        assert issubclass(state_type, State)
        return state_type.copy(old_state)
