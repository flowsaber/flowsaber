"""
Some codes are borrowed from https://github.com/PrefectHQ/prefect/blob/master/src/prefect/engine/runner.py
"""

import functools
from typing import Callable, Any

import flowsaber
from flowsaber.core.utility.state import State, Failure
from flowsaber.server.database.models import RunInput


class RunException(Exception):
    def __init__(self, state: State):
        self.state = state


def catch_to_failure(method: Callable[..., State]) -> Callable[..., State]:
    """A decorator that wraps method into a method that automatically capture exceptions into Failure state.

    Parameters
    ----------
    method

    Returns
    -------

    """

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
    """A decorator checks the difference between input and output state of the wrapped method, if two states are
    not identical, trigger runner's handle_state_change method for calling all state change handlers.

    Parameters
    ----------
    method

    Returns
    -------

    """

    @functools.wraps(method)
    def check_and_run(self: "Runner", state: State, *args, **kwargs) -> State:
        new_state = method(self, state, *args, **kwargs)

        if new_state is not state:
            new_state = self.handle_state_change(state, new_state)

        return new_state

    return check_and_run


def run_within_context(method: Callable[..., Any]) -> Any:
    """A decorator runs the wrapped method within a new context composed of runner.context and kwargs' context.

    Parameters
    ----------
    method

    Returns
    -------

    """

    @functools.wraps(method)
    def enter_context(self: "Runner", *args, **kwargs) -> Any:
        with flowsaber.context(self.context):
            flowsaber.context.update(kwargs.get('context', {}))
            return method(self, *args, **kwargs)

    return enter_context


class Runner(object):
    """Base runner class, intended to be the state manager of runnable object like flow and task.

    Users need to add state change handlers in order to be informed when meeting state changes of some method. Methods of runner
     should both accept and return state, and need to be decorated with `call_state_change_handlers` decorator.
    """

    def __init__(self, id: str = None, name: str = None, labels: list = None, **kwargs):
        self.state_change_handlers = []
        self.id = id or flowsaber.context.random_id
        self.name = name or flowsaber.context.random_id
        self.labels = labels or []

    def serialize(self, old_state: State, new_state: State) -> RunInput:
        raise NotImplementedError

    @property
    def context(self) -> dict:
        raise NotImplementedError

    def initialize_run(self, state, **kwargs) -> State:
        return state

    def handle_state_change(self, prev_state, cur_state):
        """Call all registered state change handlers with parameter of old_state and new_state.

        Parameters
        ----------
        prev_state
        cur_state

        Returns
        -------

        """
        handler = None
        try:
            for handler in self.state_change_handlers:
                cur_state = handler(self, prev_state, cur_state) or cur_state
        except Exception as exc:
            e_str = f"Unexpected error: {exc} when calling state_handler: {handler}"
            flowsaber.context.logger.exception(e_str)
            cur_state = Failure(result=exc, message=e_str)
        return cur_state

    def run(self, state: State) -> State:
        raise NotImplementedError

    @call_state_change_handlers
    def set_state(self, old_state: State, state_type: type):
        assert issubclass(state_type, State)
        return state_type.copy(old_state)
