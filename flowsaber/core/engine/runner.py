"""
Some codes are borrowed from https://github.com/PrefectHQ/prefect/blob/master/src/prefect/engine/runner.py
"""
import asyncio
import functools
import inspect
import threading
import traceback
from typing import Callable, List, Union, Optional, Coroutine, Set, Tuple

import flowsaber
from flowsaber.client.client import Client
from flowsaber.core.base import Component, enter_context
from flowsaber.core.utility.state import State, Failure, Scheduled, Cancelling, Cancelled
from flowsaber.server.database.models import RunInput, RunLogsInput, RunLogInput, FlowRunInput, TaskRunInput
from flowsaber.utility.utils import interrupt_main

AsyncFunc = Callable[..., Coroutine]


class RunException(RuntimeError):
    def __init__(self, *args, state: State = None):
        super().__init__(*args)
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
            if isinstance(exc, KeyboardInterrupt):
                e_str = f"{type(self).__name__} is cancelled with error: {exc}."
                new_state = Cancelled(result=exc, message=e_str, trace_back=tb)
            else:
                e_str = f"{type(self).__name__} meet Unexpected error: {exc} when calling method: {method} " \
                        f"with traceback: {tb}."
                new_state = Failure(result=exc, message=e_str, trace_back=tb)

        return new_state

    return catch_exception_to_failure


def run_timeout_signal(timeout: int, func: Callable, *args, **kwargs):
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
        flowsaber.context.logger.debug("Executing function in main thread...")
        res = func(*args, **kwargs)

    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)

    return res


def run_timeout_thread(timeout: int, *args, **kwargs):
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
    from concurrent.futures import ThreadPoolExecutor

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            fut = executor.submit(*args, **kwargs)
            return fut.result(timeout=timeout)
    finally:
        if not fut.done():
            fut.cancel()


class RunnerExecuteError(Exception):
    def __init__(self, *args, futures=None):
        super().__init__(*args)
        self.futures = futures or []


class RunnerExecutor(threading.Thread):
    class DoneException(Exception):
        pass

    def __init__(self, context=dict, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        self.tasks: List[Callable[..., Tuple[Coroutine, Callable]]] = []
        self.task_done_callbacks: List[AsyncFunc] = []
        # running attributes
        self.futures: List[asyncio.Future] = []
        self.jobs: List[Tuple[Coroutine, Callable]] = []
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.started: Optional[threading.Event] = None
        self.exception: Optional[BaseException] = None

    def start(self) -> None:
        self.started = threading.Event()
        super().start()
        self.started.wait()

    def siganl_kill(self):
        if self.started and self.started.is_set() and len(self.futures):
            # the first task is a stop future
            self.loop.call_soon_threadsafe(self.futures[0].set_exception, self.DoneException())

    def signal_stop(self):
        if self.started and self.started.is_set() and len(self.futures):
            # the first task is a stop future
            self.loop.call_soon_threadsafe(self.futures[0].set_result, None)

    def join(self, timeout: Optional[float] = ...) -> None:
        self.signal_stop()
        for _, stop in self.jobs:
            stop()
        super().join()
        self.started = None
        if self.exception:
            exception = self.exception
            self.exception = None
            raise exception

    async def main_loop(self):
        """Endlessly fetch async task anc schedule for running in asyncio envent loop untill some task raise an
        exception.

        Returns
        -------

        """

        def raise_on_error(loop, context):
            loop.default_exception_handler(context)
            self.siganl_kill()

        self.loop = asyncio.get_running_loop()
        self.loop.set_exception_handler(raise_on_error)
        self.futures.clear()
        self.jobs.clear()
        # add a stop future as stop signal of executor
        stop_future = asyncio.Future()
        self.futures.append(stop_future)

        while len(self.tasks):
            task_func = self.tasks.pop(-1)
            job = task_func()
            if isinstance(job, (list, tuple)):
                coro, stop, *_ = job
            else:
                coro, stop = job, lambda: 1
            assert inspect.iscoroutine(coro) and callable(stop) and not inspect.iscoroutinefunction(stop)

            fut = asyncio.create_task(coro)
            self.futures.append(fut)
            self.jobs.append((coro, stop))

        # now thread.start end blocking
        self.started.set()
        # use the stop_future to force cancel all tasks in case of Exception
        done, pending = await asyncio.wait(self.futures, return_when=asyncio.FIRST_EXCEPTION)
        # try to cancel all jobs
        for job in pending:
            job.cancel()

        # do callbacks
        for callback in self.task_done_callbacks:
            res = callback()
            if inspect.iscoroutine(res):
                await res

        # raise exception jobs
        error_futures = [task for task in done
                         if not task.cancelled()
                         and task.exception()
                         and task is not stop_future]
        futures = list(done) + list(pending)
        futures.remove(stop_future)
        try:
            if len(error_futures):
                first_exception = error_futures[0].exception()
                raise RunnerExecuteError(str(first_exception), futures=futures) from first_exception

            return futures
        finally:
            self.started.clear()
            self.futures.clear()
            self.jobs.clear()
            self.loop = None

    @enter_context
    def run(self):
        try:
            asyncio.run(self.main_loop())
        except BaseException as exc:
            self.exception = exc

    def add_task(self, task: Callable[..., Coroutine]):
        assert not self.started or not self.started.is_set(), "Can only add task before running"
        assert callable(task) and not inspect.iscoroutinefunction(task)
        self.tasks.append(task)


class Runner(object):
    """Base runner class, intended to be the state manager of runnable object like flow and task.

    Users need to add state change handlers in order to be informed when meeting state changes of some method.
    Methods of runner should both accept and return state, and need to be decorated with
    `call_state_change_handlers` decorator.
    """

    def __init__(self, server_address: str = None, id: str = None, name: str = None, labels: list = None, **kwargs):
        self.id = id or flowsaber.context.random_id
        self.name = name or flowsaber.context.random_id
        self.labels = labels or []
        self.component: Optional[Component] = None
        # used for executing background async tasks
        self.tasks: List[Callable[..., Coroutine]] = []
        self.executor: Optional[RunnerExecutor] = None
        self.state_change_handlers: Set[Callable] = set()
        # client
        self.server_address: str = server_address
        self.client: Optional[Client] = Client(self.server_address) if self.server_address else None
        # add tasks and state handlers
        self.state_change_handlers.add(self.logging_run_state)
        if self.client:
            self.add_task(self.update_run_state)
            # TODO may meet situations when update_run_state is not done but send_logs is done
            self.add_task(self.send_logs)

    def add_task(self, task: Callable[..., Tuple[Coroutine, Callable]]):
        # TODO each async task should add a exit async function
        assert callable(task) and not inspect.iscoroutinefunction(task), \
            "The task must be a function return a coroutine"
        self.tasks.append(task)

    def remove_async_tasks(self, task: Callable):
        self.tasks.remove(task)

    @property
    def context(self) -> dict:
        assert self.component is not None, 'component is not settled'
        return self.component.context

    @property
    def config(self) -> dict:
        assert self.component is not None, 'component is not settled'
        return self.component.config_dict

    @call_state_change_handlers
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
                if handler is None:
                    continue
                cur_state = handler(self, prev_state, cur_state) or cur_state
        except Exception as exc:
            e_str = f"Unexpected error: {exc} when calling state_handler: {handler}"
            cur_state = Failure(result=exc, message=e_str)
        return cur_state

    def run(self, state: State = None, **kwargs) -> State:
        try:
            kwargs.setdefault('context', {})
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
        if self.client:
            self.executor = RunnerExecutor(context=context_dict)
            for task in self.tasks:
                self.executor.add_task(task)
            self.executor.task_done_callbacks.append(self.client.close)
            self.executor.start()

    def initialize_context(self, *args, **kwargs):
        pass

    def start_run(self, state: State = None, **kwargs) -> State:
        raise NotImplementedError

    def leave_run(self, *args, **kwargs):
        if self.client:
            self.executor.join()
            self.executor = None

    @call_state_change_handlers
    def set_state(self, old_state: State, state_type: type):
        assert issubclass(state_type, State)
        return state_type.copy(old_state)

    def serialize(self, state: State, state_only=True) -> RunInput:
        raise NotImplementedError

    @staticmethod
    def logging_run_state(runner: "Runner", old_state, new_state):
        flowsaber.context.logger.info(f"State change from [{old_state}] to [{new_state}]")

    def send_logs(self) -> Tuple[Coroutine, Callable]:
        """This should be ran in runner.executor's async main loop"""

        def enqueue_log(record):
            from datetime import datetime
            record_dict = {
                'level': record.levelname,
                'time': datetime.fromtimestamp(record.created).timestamp(),
                'task_id': getattr(record, 'task_id', None),
                'flow_id': getattr(record, 'flow_id', None),
                'taskrun_id': getattr(record, 'taskrun_id', None),
                'flowrun_id': getattr(record, 'flowrun_id', None),
                'agent_id': getattr(record, 'agent_id', None),
                'message': record.message
            }
            run_log = RunLogInput(**record_dict)
            loop.call_soon_threadsafe(log_queue.put_nowait, run_log)

        client = self.client
        log_queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        # TODO not thread safe
        # add handler
        log_handler = flowsaber.log_handler
        log_handler.handler = enqueue_log

        async def main_loop():
            try:
                while True:
                    log = await log_queue.get()
                    if log is None:
                        break
                    fields = "success\n" \
                             "error"
                    await client.mutation('write_runlogs', RunLogsInput(logs=[log]), fields)
            finally:
                # TODO not thread safe
                # remove handler
                log_handler.handler = None

        def stop():
            if loop.is_running():
                loop.call_soon_threadsafe(log_queue.put_nowait, None)

        return main_loop(), stop

    def update_run_state(self) -> Tuple[Coroutine, Callable]:
        """Does not use create_task as these jobs has a strict order

        Parameters
        ----------
        runner

        Returns
        -------

        """

        def state_change_handler(runner: "Runner", old_state: State, new_state: State):
            async def update_run():
                await client.mutation(method, run_input, field='id')

            is_flow = 'flow' in type(runner).__name__.lower()
            method = 'update_flowrun' if is_flow else "update_taskrun"
            state_only = not (old_state is None and isinstance(new_state, Scheduled))
            run_input = runner.serialize(new_state, state_only=state_only)
            client = runner.client
            loop.call_soon_threadsafe(task_queue.put_nowait, update_run())

        loop = asyncio.get_running_loop()
        task_queue = asyncio.Queue()
        # TODO not thread safe
        # add handler
        self.state_change_handlers.add(state_change_handler)

        async def main_loop():
            try:
                while True:
                    coro = await task_queue.get()
                    if coro is None:
                        break
                    await coro
            finally:
                # TODO not thread safe
                # remove handler
                self.state_change_handlers.remove(state_change_handler)

        def stop():
            if loop.is_running():
                loop.call_soon_threadsafe(task_queue.put_nowait, None)

        return main_loop(), stop

    def check_cancellation(self):
        loop = asyncio.get_running_loop()
        client = self.client
        check_interval = self.config.get('check_cancellation_interval', 5)
        runner_id = self.id
        if 'flow' in type(self).__name__.lower():
            method = 'get_flowrun'
        else:
            method = 'get_taskrun'
        need_stop = asyncio.Event()

        async def main_loop():
            while not need_stop.is_set():
                await asyncio.sleep(check_interval)
                run = await client.query(method, input=runner_id, field="state { state_type }")
                state = State.from_dict(run['state'])
                if isinstance(state, Cancelling):
                    flowsaber.context.logger.error("Fetched flow cancelling request from server.")
                    interrupt_main()

        def stop():
            if loop.is_running():
                loop.call_soon_threadsafe(need_stop.set)

        return main_loop(), stop

    def maintain_heartbeat(self) -> Tuple[Coroutine, Callable]:
        client = self.client
        loop = asyncio.get_running_loop()

        need_stop = asyncio.Event()
        is_flow = 'flow' in type(self).__name__.lower()
        method = 'update_flowrun' if is_flow else "update_taskrun"
        runner_id = self.id

        async def main_loop():
            while not need_stop.is_set():
                await asyncio.sleep(5)
                run_input = FlowRunInput(id=runner_id) if is_flow else TaskRunInput(id=runner_id)
                await client.mutation(method, input=run_input, field="id")

        def stop():
            if loop.is_running():
                loop.call_soon_threadsafe(need_stop.set)

        return main_loop(), stop
