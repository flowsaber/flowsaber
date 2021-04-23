"""
Some codes are borrowed from https://github.com/PrefectHQ/prefect/blob/master/src/prefect/engine/runner.py
"""
import functools
import traceback
from typing import Callable, Union, Optional

import flowsaber
from flowsaber.core.base import Component
from flowsaber.core.utility.state import State, Failure, Scheduled
from flowsaber.server.database.models import RunInput


class RunException(Exception):
    def __init__(self, state: State):
        self.state = state


def redirect_std_to_logger(method: Callable[..., State]) -> Callable[..., State]:
    @functools.wraps(method)
    def redirect(self: 'Runner', *args, **kwargs) -> State:
        import logging
        from contextlib import redirect_stdout, redirect_stderr, nullcontext
        from flowsaber.utility.logging import RedirectToLog

        rd_stdout_context = nullcontext()
        rd_stderr_context = nullcontext()
        cur_logger = flowsaber.context.logger
        updated_config = flowsaber.context.get(self.component.config_name, {})
        if updated_config.get('log_stdout', False):
            rd_stdout_context = redirect_stdout(RedirectToLog(cur_logger, level=logging.INFO))
        if updated_config.get('log_stderr', False):
            rd_stderr_context = redirect_stderr(RedirectToLog(cur_logger, level=logging.ERROR))

        with rd_stdout_context, rd_stderr_context:
            return method(self, *args, **kwargs)

    return redirect


def call_state_change_handlers(method: Callable[..., State]) -> Callable[..., State]:
    """A decorator checks the difference between _input and _output state of the wrapped method, if two states are
    not identical, trigger runner's handle_state_change method for calling all state change handlers.

    Parameters
    ----------
    method

    Returns
    -------

    """

    @functools.wraps(method)
    def check_and_run(self: "Runner", state: State = None, *args, **kwargs) -> State:
        new_state = method(self, state, *args, **kwargs)

        if new_state is not state:
            new_state = self.handle_state_change(state, new_state)

        return new_state

    return check_and_run


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
            tb = traceback.format_exc()
            e_str = f"Unexpected error: {exc} when calling method: {method} with traceback: {tb}"
            new_state = Failure(result=exc, message=e_str)
        return new_state

    return catch_exception_to_failure


def run_timeout_signal(func: Callable, timeout: int, *args, **kwargs):
    """Run function in main thread in unix system with timeout using SIGALARM signal.

    References
    ----------
    https://github.com/pnpnpn/timeout-decorator/blob/master/timeout_decorator/timeout_decorator.py

    Parameters
    ----------
    func
    timeout
    args
    kwargs

    Returns
    -------

    """
    import signal

    def error_handler(signum, frame):
        raise TimeoutError(f"Execution timout out of {timeout}")

    old = signal.signal(signal.SIGALRM, error_handler)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:

        # Raise the alarm if `timeout` seconds pass
        flowsaber.context.logger.debug(f"Sending alarm with {timeout}s timeout...")
        signal.alarm(timeout)
        flowsaber.context.logger.debug(f"Executing function in main thread...")
        res = func(*args, **kwargs)

    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)

    return res


def run_timeout_thread(func: Callable, timeout: int, **kwargs):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            fut = executor.submit(func, **kwargs)
            return fut.result(timeout=timeout)
    except TimeoutError as e:
        fut.cancel()
        raise e


class Runner(object):
    """Base runner class, intended to be the state manager of runnable object like flow and task.

    Users need to add state change handlers in order to be informed when meeting state changes of some method. Methods of runner
     should both accept and return state, and need to be decorated with `call_state_change_handlers` decorator.
    """

    def __init__(self, id: str = None, name: str = None, labels: list = None, **kwargs):
        self.state_change_handlers = [self.logging_state_change_chandler]
        self.id = id or flowsaber.context.random_id
        self.name = name or flowsaber.context.random_id
        self.labels = labels or []
        self.component: Optional[Component] = None

    @staticmethod
    def logging_state_change_chandler(runner: "Runner", old_state, new_state):
        flowsaber.context.logger.debug(f"State change from [{old_state}] to [{new_state}]")

    def serialize(self, old_state: State, new_state: State) -> RunInput:
        raise NotImplementedError

    @property
    def context(self) -> dict:
        assert self.component is not None, 'component is not settled'
        return self.component.context

    @property
    def config(self) -> dict:
        assert self.component is not None, 'component is not settled'
        return self.component.config_dict

    def initialize_run(self, state: Union[State, None], **kwargs) -> State:
        if state is None:
            state = Scheduled()
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
            cur_state = Failure(result=exc, message=e_str)
        return cur_state

    def run(self, state: State = None, **kwargs) -> State:
        try:
            self.enter_run(state, **kwargs)

            final_state = self.start_run(state=state, **kwargs)

            return final_state
        finally:
            self.leave_run()

    def enter_run(self, *args, **kwargs):
        flowsaber.flowsaber_log_manager.start()

    def start_run(self, state: State = None, **kwargs) -> State:
        raise NotImplementedError

    def leave_run(self, *args, **kwargs):
        flowsaber.flowsaber_log_manager.stop()

    @call_state_change_handlers
    def set_state(self, old_state: State, state_type: type):
        assert issubclass(state_type, State)
        return state_type.copy(old_state)
