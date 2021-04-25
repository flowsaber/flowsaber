"""
Some codes are borrowed from https://github.com/PrefectHQ/prefect/blob/master/src/prefect/engine/runner.py
"""
import asyncio
import functools
import inspect
import threading
import traceback
from typing import Callable, Set, List, Union, Optional, Coroutine

import flowsaber
from flowsaber.client.client import Client
from flowsaber.core.base import Component, enter_context
from flowsaber.core.utility.state import State, Failure, Scheduled, Cancelled
from flowsaber.server.database.models import RunInput, RunLogsInput, RunLogInput

AsyncFunc = Callable[..., Coroutine]


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
        # TODO how to handle this only once?
        # must use BaseException, because KeyBoardInterrupt is not Exception
        except BaseException as exc:
            tb = traceback.format_exc()
            # print(f'----- {exc} {tb}')
            if isinstance(exc, KeyboardInterrupt):
                e_str = f"{type(self).__name__} is cancelled with error: {exc}."
                new_state = Cancelled(result=exc, message=e_str, trace_back=tb)
            else:
                e_str = f"{type(self).__name__} meet Unexpected error: {exc} when calling method: {method} " \
                        f"with traceback: {tb}."
                new_state = Failure(result=exc, message=e_str, trace_back=tb)

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
    """Run the task within timeout within a thread pool. Note that the flowsaber.context would be corrupted
    in the new thread.

    Parameters
    ----------
    func
    timeout
    kwargs

    Returns
    -------

    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            fut = executor.submit(func, **kwargs)
            return fut.result(timeout=timeout)
    except TimeoutError as e:
        fut.cancel()
        raise e


async def check_cancellation(runner: "Runner"):
    import asyncio
    check_interval = runner.config.get('check_cancellation_interval', 5)
    runner_id = runner.id
    client: Client = runner.client
    if 'flow' in type(runner).__name__.lower():
        method = 'get_flowrun'
    else:
        method = 'get_taskrun'
    fields = "state"
    while True:
        await asyncio.sleep(check_interval)
        run = await client.query(method, input=runner_id, field=fields)
        # state = State.from_dict(run['state'])
        # if isinstance(state, Cancelling):
        #     interrupt_main()


async def send_logs(runner: "Runner"):
    from datetime import datetime
    client = runner.client
    log_queue = runner.log_queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def enqueue_log(record):
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
        loop.call_soon_threadsafe(log_queue.put_nowait, run_log)

    try:
        flowsaber.log_handler.handler = enqueue_log
        while True:
            log = await log_queue.get()
            fields = "success\n" \
                     "error"
            res = await client.mutation('write_runlogs', RunLogsInput(logs=[log]), fields)
    finally:
        flowsaber.log_handler.handler = None


class RunnerExecuteError(RuntimeError):
    def __init__(self, *args, futures=None):
        super().__init__(*args)
        self.futures = futures or []


class RunnerExecutor(threading.Thread):
    class DoneException(Exception):
        pass

    def __init__(self, context=dict, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        self.tasks: List[asyncio.Future] = []
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.queue: Optional[asyncio.Queue] = None
        self.started: Optional[threading.Event] = None

    def start(self) -> None:
        self.started = threading.Event()
        super().start()
        self.started.wait()

    def stop(self):
        return self.join()

    def join(self, timeout: Optional[float] = ...) -> None:
        self.signal_stop()
        super().join()
        self.started = None

    def signal_stop(self):
        # the first task is a stop future
        self.loop.call_soon_threadsafe(self.tasks[0].set_exception, self.DoneException())
        self.loop.call_soon_threadsafe(self.queue.put_nowait, 'STOP')

    async def main_loop(self):
        """Endlessly fetch async task anc schedule for running in asyncio envent loop untill some task raise an
        exception.

        Returns
        -------

        """

        def raise_on_error(loop, context):
            loop.default_exception_handler(context)
            self.signal_stop()

        self.loop = asyncio.get_running_loop()
        self.loop.set_exception_handler(raise_on_error)
        self.queue = task_queue = asyncio.Queue()
        self.started.set()
        # add a stop future as stop signal of executor
        self.tasks.clear()
        stop_future = asyncio.Future()
        self.tasks.append(stop_future)

        while True:
            coro = await task_queue.get()
            if coro == 'STOP':
                break
            task = asyncio.create_task(coro)
            self.tasks.append(task)

        done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)
        for job in pending:
            job.cancel()

        try:
            error_futures = [task for task in done
                             if not task.cancelled()
                             and task.exception()
                             and task is not stop_future]
        except Exception as e:
            raise e
        futures = list(done) + list(pending)
        futures.remove(stop_future)
        if len(error_futures):
            raise RunnerExecuteError(futures=futures) from error_futures[0].exception()

        return futures

    @enter_context
    def run(self):

        asyncio.run(self.main_loop())

    def create_task(self, coro: Union[Coroutine, str]):
        assert self.started and self.started.is_set()
        self.loop.call_soon_threadsafe(self.queue.put_nowait, coro)


class Runner(object):
    """Base runner class, intended to be the state manager of runnable object like flow and task.

    Users need to add state change handlers in order to be informed when meeting state changes of some method. Methods of runner
     should both accept and return state, and need to be decorated with `call_state_change_handlers` decorator.
    """

    def __init__(self, server_address: str = None, id: str = None, name: str = None, labels: list = None, **kwargs):
        self.id = id or flowsaber.context.random_id
        self.name = name or flowsaber.context.random_id
        self.labels = labels or []
        self.component: Optional[Component] = None
        # used for executing background async tasks
        self.executor: Optional[RunnerExecutor] = None
        self.async_tasks: Set[AsyncFunc] = set()
        self.state_change_handlers: List[Callable] = [self.logging_state_change_chandler]
        # client
        self.server_address: str = server_address
        self.client: Optional[Client] = Client(self.server_address) if self.server_address else None
        self.log_queue: Optional[asyncio.Queue] = None
        # add tasks
        if self.client:
            self.add_async_task(send_logs)

    def add_async_task(self, async_task: AsyncFunc):
        # TODO each async task should add a exit async function
        assert inspect.iscoroutinefunction(async_task), "The async task must be a coroutine " \
                                                        "function accepts runner as the only parameter."
        self.async_tasks.add(async_task)

    def remove_async_tasks(self, async_task: AsyncFunc):
        self.async_tasks.remove(async_task)

    @staticmethod
    def logging_state_change_chandler(runner: "Runner", old_state, new_state):
        flowsaber.context.logger.info(f"State change from [{old_state}] to [{new_state}]")

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
            # for debug
            if 'flow' in type(self).__name__.lower() and isinstance(final_state, Failure):
                raise final_state.result
            return final_state
        finally:
            self.leave_run()

    def enter_run(self, *args, **kwargs):
        self.initialize_context(*args, **kwargs)
        # TODO to may redundant update, should move in the future
        with flowsaber.context(self.context):
            with flowsaber.context(kwargs.get('context', {})) as context:
                context_dict = context.to_dict()
        self.executor = RunnerExecutor(context=context_dict)
        self.executor.start()
        for async_func in self.async_tasks:
            self.executor.create_task(async_func(self))

    def initialize_context(self, *args, **kwargs):
        pass

    def start_run(self, state: State = None, **kwargs) -> State:
        raise NotImplementedError

    def leave_run(self, *args, **kwargs):
        # stop the logger loop
        flowsaber.log_handler.handler = None
        self.executor.stop()
        self.executor = None

    @call_state_change_handlers
    def set_state(self, old_state: State, state_type: type):
        assert issubclass(state_type, State)
        return state_type.copy(old_state)
