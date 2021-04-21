import signal
import time
from inspect import BoundArguments

import flowsaber
from flowsaber.core.engine.runner import Runner, catch_to_failure, call_state_change_handlers, run_within_context
from flowsaber.core.utility.state import (
    State, Scheduled, Pending, Running, Retrying, Failure, Cached, Success, Skip, Drop
)
from flowsaber.core.utility.target import File
from flowsaber.server.database import TaskRunInput
from flowsaber.utility.statutils import ResourceMonitor


class TaskRunner(Runner):
    """The task runner moves the task state forward through a complicated process including:
    retry, cache read/write, skip, drop ...
    """

    def __init__(self, task, inputs: BoundArguments, **kwargs):
        super().__init__(**kwargs)
        self.task = task
        self.inputs: BoundArguments = inputs

    @property
    def context(self):
        return self.task.context

    def serialize(self, old_state: State, new_state: State) -> TaskRunInput:
        taskrun_input = TaskRunInput(id=self.id, state=new_state.to_dict())
        if isinstance(old_state, Scheduled) and isinstance(new_state, Pending):
            taskrun_input.__dict__.update({
                'task_id': flowsaber.context.get('task_id'),
                'flow_id': flowsaber.context.get('flow_id'),
                'agent_id': flowsaber.context.get('agent_id'),
                'flowrun_id': flowsaber.context.get('flowrun_id'),
                'inputs': {},
                'context': flowsaber.context.to_dict()
            })

        return taskrun_input

    def initialize_run(self, state, **kwargs) -> State:
        state = super().initialize_run(state)
        self.context.update(taskrun_id=self.id)
        if 'context' in kwargs:
            kwargs['context'].update(taskrun_id=self.id)
        flowsaber.context.update(taskrun_id=self.id)
        return state

    @run_within_context
    @call_state_change_handlers
    @catch_to_failure
    def run(self, state: State, **kwargs) -> State:
        state = self.initialize_run(state)
        state = self.set_state(state, Pending)
        state = self.set_state(state, Running)
        # 1. skip if needed
        state = self.check_skip(state)
        if isinstance(state, Skip):
            return state
        retry = self.task.config_dict.get("retry", 1)
        while True:
            # 2. use cached result if needed
            state = self.read_cache(state)
            if isinstance(state, Cached):
                return state
            # 3. run the task
            state = self.run_task(state, **kwargs)
            if isinstance(state, Failure):
                if retry > 0:
                    flowsaber.context.logger.info(f"Run task: {self.task} failed, try to retry."
                                                  f"with {retry - 1} retrying left.")
                    state = self.set_state(state, Retrying)
                    time.sleep(self.task.config_dict.get('retry_wait_time', 3))
                    state = self.set_state(state, Running)
                    retry -= 1
                    continue
                elif self.task.config_dict.get("skip_error", False):
                    state = self.set_state(state, Drop)
            break
        # 4. write to cache if needed
        if isinstance(state, Success):
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
        res = self.task.cache.get(run_workdir, no_cache)
        use_cache = res is not no_cache
        if use_cache:
            cache_valid = True
            values = [res] if not isinstance(res, (list, tuple, set)) else res
            for f in [v for v in values if isinstance(v, File)]:
                if f.initialized:
                    check_hash = await self.task.executor.run(f.calculate_hash)
                    if check_hash != f.hash:
                        msg = f"Task {self.task.task_name} read cache failed from disk " \
                              f"because file content task_hash changed."
                        flowsaber.context.logger.debug(msg)
                        cache_valid = False
                        break
            if cache_valid:
                state = Cached.copy(state)
                state.result = res
            else:
                self.task.cache.remove(run_workdir)
        return state

    @call_state_change_handlers
    @catch_to_failure
    def run_task(self, state: State, **kwargs) -> State:
        with ResourceMonitor() as monitor:
            res = self.run_task_timeout(**self.inputs.arguments)
        state = Success.copy(state)
        state.result = res
        state.context['resource_usage'] = monitor.usage

        return state

    @call_state_change_handlers
    def write_cache(self, state):
        assert isinstance(state, Success)
        self.read_cache.put(self.context['run_key'], state.result)
        return state

    def run_task_timeout(self, **kwargs):
        """Call task.run with timeout handling by using signal.
        Parameters
        ----------
        kwargs

        Returns
        -------

        """
        timeout = self.task.config_dict.get("timeout")
        if not timeout:
            return self.task.run(**kwargs)

        def error_handler(signum, frame):
            raise TimeoutError(f"Execution timout out of {timeout}")

        try:
            signal.signal(signal.SIGALRM, error_handler)
            # Raise the alarm if `timeout` seconds pass
            flowsaber.context.logger.debug(f"Sending alarm with {timeout}s timeout...")
            signal.alarm(timeout)
            flowsaber.context.logger.debug(f"Executing function in main thread...")
            return self.task.run_in_context(**kwargs)

        finally:
            signal.alarm(0)


def get_task_runner_cls(*args, **kwargs):
    return TaskRunner
