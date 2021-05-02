import sys
import threading
import time
from inspect import BoundArguments
from typing import TYPE_CHECKING

import flowsaber
from flowsaber.core.base import enter_context
from flowsaber.core.engine.runner import (
    Runner,
    catch_to_failure,
    call_state_change_handlers,
    run_timeout_signal,
    run_timeout_thread,
    redirect_std_to_logger
)
from flowsaber.core.utility.state import (
    State, Pending, Running, Retrying,
    Failure, Cached, Success, Skip, Drop
)
from flowsaber.core.utility.target import File
from flowsaber.server.database import TaskRunInput
from flowsaber.utility.statutils import ResourceMonitor

if TYPE_CHECKING:
    from flowsaber.core.task import Task


class TaskRunner(Runner):
    """The task runner moves the task state forward through a complicated process including:
    retry, cache read/write, skip, drop ...
    """

    def __init__(self, task: 'Task', inputs: BoundArguments, **kwargs):
        super().__init__(**kwargs)
        assert task.initialized
        self.task = task
        self.component = self.task
        self.inputs: BoundArguments = inputs

    def initialize_context(self, *args, **kwargs):
        update_context = {
            'taskrun_id': self.id,
            'server_address': self.server_address
        }
        self.context.update(**update_context)
        if 'context' in kwargs:
            kwargs['context'].update(**update_context)
        flowsaber.context.update(**update_context)

    @enter_context
    @redirect_std_to_logger
    @call_state_change_handlers
    @catch_to_failure
    def start_run(self, state: State = None, **kwargs) -> State:
        state = self.initialize_run(state, **kwargs)
        state = self.set_state(state, Pending)
        state = self.set_state(state, Running)
        # 1. skip if needed
        state = self.check_skip(state)
        if isinstance(state, Skip):
            return state
        retry = self.task.config_dict.get("retry", 1)
        cache_type = self.context.get('cache_type', None)
        while True:
            # 2. use cached result if needed
            if cache_type:
                state = self.read_cache(state)
                if isinstance(state, Cached):
                    return state
            # 3. run the task
            state = self.run_task(state, **kwargs)
            if isinstance(state, Failure):
                if retry > 0:
                    flowsaber.context.logger.warning(f"Run task: {self.task} failed, try to retry "
                                                     f"with {retry - 1} retrying left.")
                    state = self.set_state(state, Retrying)
                    time.sleep(self.task.config_dict.get('retry_delay', 2))
                    state = self.set_state(state, Running)
                    retry -= 1
                    continue
                elif self.task.config_dict.get("skip_error", False):
                    state = self.set_state(state, Drop)
            break
        # 4. write to cache if needed
        if isinstance(state, Success) and cache_type:
            state = self.write_cache(state)

        return state

    @call_state_change_handlers
    def check_skip(self, state: State) -> State:
        if self.task.skip_fn and self.task.need_skip(self.inputs):
            state = Skip.copy(state)
            state.result = tuple(state.inputs.arguments.values())
        return state

    @call_state_change_handlers
    def read_cache(self, state: State) -> State:
        run_workdir = self.context['run_workdir']
        no_cache = object()
        res = flowsaber.context.cache.get(run_workdir, no_cache)
        use_cache = res is not no_cache
        if use_cache:
            cache_valid = True
            values = [res] if not isinstance(res, (list, tuple, set)) else res
            for f in [v for v in values if isinstance(v, File)]:
                if f.initialized:
                    check_hash = f.calculate_hash()
                    if check_hash != f.hash:
                        msg = "Read cache failed from disk " \
                              "because file content task_hash changed."
                        flowsaber.context.logger.warning(msg)
                        cache_valid = False
                        break
            if cache_valid:
                state = Cached.copy(state)
                state.result = res
            else:
                flowsaber.context.cache.remove(run_workdir)
        return state

    @call_state_change_handlers
    @catch_to_failure
    def run_task(self, state: State, **kwargs) -> State:
        with ResourceMonitor() as monitor:
            res = self.run_task_timeout(**kwargs)
        state = Success.copy(state)
        state.result = res
        run_info = {
            'resource_usage': monitor.usage
        }
        self.context.update(run_info)
        flowsaber.context.update(run_info)

        return state

    @call_state_change_handlers
    def write_cache(self, state):
        assert isinstance(state, Success)
        run_workdir = self.context['run_workdir']
        cache = flowsaber.context.cache
        cache.put(run_workdir, state.result)
        cache.persist_single(run_workdir)
        return state

    def run_task_timeout(self, **kwargs):
        """Call task.run with timeout handling by using signal.
        Parameters
        ----------
        kwargs

        Returns
        -------

        """
        run_args = self.inputs.args
        run_kwargs = self.inputs.kwargs

        timeout = self.task.config_dict.get("timeout")
        if timeout:
            if not sys.platform.startswith('win'):
                is_main_thread = threading.current_thread() is threading.main_thread()
                if is_main_thread:
                    flowsaber.context.logger.debug("Run in timeout in main thread in unix system")
                    return run_timeout_signal(timeout, self.task.run, *run_args, **run_kwargs)

            flowsaber.context.logger.debug("Run in timeout using ThreadPoolExecutor")

            @enter_context
            def run_in_context(task):
                return task.run(*run_args, **run_kwargs)

            return run_timeout_thread(timeout, run_in_context, self.task)

        return self.task.run(*run_args, **run_kwargs)

    def serialize(self, state: State, state_only=True):
        info = {'id': self.id, 'state': state.to_dict()}
        if not state_only:
            info.update({
                'flowrun_id': self.context['flowrun_id'],
                'agent_id': self.context.get('agent_id'),
                'task_id': self.context['task_id'],
                'flow_id': self.context['flow_id'],
                'context': {},
            })

        taskrun_input = TaskRunInput(**info)

        return taskrun_input
