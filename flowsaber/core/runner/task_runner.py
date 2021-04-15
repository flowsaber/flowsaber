import signal
import time

from .runner import Runner, call_state_change_handlers, catch_to_failure
from ..utils.state import *
from ..utils.target import *
from ...utility.statutils import ResourceMonitor


class TaskRunner(Runner):
    def __init__(self, task, **kwargs):
        super().__init__(**kwargs)
        self.task = task

    def serialize(self, old_state: State, new_state: State, state_only=False) -> RunInput:
        if state_only:
            return TaskRunInput(
                id=self.key,
                state=(new_state - old_state).serialize()
            )
        else:
            return TaskRunInput(
                id=self.key,
                task_id=self.task.key,
                flow_id=self.task.flow_key,
                agent_id=self.agent_id,
                flowrun_id=self.flowrun_key,
                state=new_state.serialize()
            )

    def initialize_run(self, state) -> State:
        state = super().initialize_run(state)
        run_info = self.task.get_run_info(state.inputs)
        state.context.update(run_info)
        return state

    @call_state_change_handlers
    @catch_to_failure
    def run(self, state: State) -> State:
        state = self.initialize_run(state)
        state = self.set_state(state, Running)
        # 1. skip if needed
        state = self.check_skip(state)
        if isinstance(state, Skip):
            return state
        retry = self.task.config.get("retry", 1)
        while True:
            # 2. use cached result if needed
            state = self.read_cache(state)
            if isinstance(state, Cached):
                return state
            # 3. run the task
            state = self.run_task(state)
            if isinstance(state, Failure):
                if retry > 0:
                    self.logger.info(f"Run task: {self.task} failed, try to retry."
                                     f"with {retry - 1} retrying left.")
                    state = self.set_state(state, Retrying)
                    time.sleep(self.task.config.get('retry_wait_time', 3))
                    state = self.set_state(state, Running)
                    retry -= 1
                    continue
                elif self.task.config.get("skip_error", False):
                    state = self.set_state(state, Drop)
            break
        # 4. write to cache if needed
        if isinstance(state, Success):
            state = self.write_cache(state)

        return state

    @call_state_change_handlers
    def check_skip(self, state: State) -> State:
        if self.task.skip_fn and self.task.need_skip(state.inputs):
            state = Skip.copy(state)
            state.result = tuple(state.inputs.arguments.values())
        return state

    @call_state_change_handlers
    def read_cache(self, state: State) -> State:
        run_key = state.context['run_key']
        no_cache = object()
        res = self.task.cache.get(run_key, no_cache)
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
                        self.logger.debug(msg)
                        cache_valid = False
                        break
            if cache_valid:
                state = Cached.copy(state)
                state.result = res
            else:
                self.task.cache.remove(run_key)
        return state

    @call_state_change_handlers
    @catch_to_failure
    def run_task(self, state: State) -> State:
        with context(state.context), ResourceMonitor() as monitor:
            res = self.run_task_timeout(**state.inputs.arguments)
        state = Success.copy(state)
        state.result = res
        state.context['resource_usage'] = monitor.usage

        return state

    @call_state_change_handlers
    def write_cache(self, state):
        assert isinstance(state, Success)
        self.read_cache.put(state.context['run_key'], state.result)
        return state

    def run_task_timeout(self, **kwargs):
        timeout = self.task.config.get("timeout")
        if not timeout:
            return self.task.run(**kwargs)

        def error_handler(signum, frame):
            raise TimeoutError(f"Execution timout out of {timeout}")

        try:
            signal.signal(signal.SIGALRM, error_handler)
            # Raise the alarm if `timeout` seconds pass
            self.logger.debug(f"Sending alarm with {timeout}s timeout...")
            signal.alarm(timeout)
            self.logger.debug(f"Executing function in main thread...")
            return self.task.run(**kwargs)

        finally:
            signal.alarm(0)


def get_task_runner_cls(*args, **kwargs):
    return TaskRunner
