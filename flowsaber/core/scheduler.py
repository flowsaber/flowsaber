import asyncio
import inspect
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Set, Callable, Dict, Awaitable, Any, TypeVar, Sequence, Optional

from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    SpinnerColumn,
    TimeElapsedColumn
)

from flowsaber.context import config
from flowsaber.core.base import TaskConfig
from flowsaber.utility.logtool import get_logger

# TODO The displaying and scheduler should be separated.
process = Progress(
    TextColumn("[bold blue]{task.fields[task]}", justify="right"),
    BarColumn(),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    TextColumn("{task.fields[pending]}"),
    "⌛",
    "•",
    TextColumn("{task.fields[running]}"),
    SpinnerColumn(),
    "•",
    TextColumn("{task.fields[done]}"),
    "✓",
    "|",
    TimeElapsedColumn()
)

logger = get_logger(__name__)

AsyncFunc = Callable[[Any], Awaitable[None]]

Task = TypeVar('Task')


def time(fn):
    from time import time
    st = time()

    def inner(*args, **kwargs):
        return fn(*args, **kwargs)

    print(f"Cost {time() - st} in total.")
    return inner


class Job(asyncio.Future):
    def __init__(self, runner: Callable, args: tuple, kwargs: dict, task: Task):
        super().__init__()
        self.runner = runner
        self.args = args
        self.kwargs = kwargs
        self.task = task
        self.future: Optional[asyncio.Future] = None
        self.async_done_callbacks = []

    def add_async_done_callback(self, callback: AsyncFunc):
        assert inspect.iscoroutinefunction(callback)
        self.async_done_callbacks.append(callback)


@dataclass
class TaskState:
    pending: Set[Job] = field(default_factory=set)
    running: Set[Job] = field(default_factory=set)
    done: Set[Job] = field(default_factory=set)
    wait_q: asyncio.Queue = field(default_factory=asyncio.Queue)
    all_submitted: asyncio.Event = field(default_factory=asyncio.Event)
    task_id: int = 0

    def __iter__(self):
        return iter(self.__dict__.values())

    def __len__(self):
        return len(self.__dict__)

    def __await__(self):
        self.all_submitted.set()
        return self.wait_q.join().__await__()


class Solver(object):
    pass


class GaSolver(Solver):

    def solve(self, jobs: Sequence[Job]) -> Sequence[Job]:
        from pyeasyga import pyeasyga
        if len(jobs) <= 0:
            return jobs
        ga = pyeasyga.GeneticAlgorithm(list(jobs))
        ga.fitness_function = self.fitness
        ga.run()
        cpus, flags = ga.best_individual()
        num_jobs = len(jobs)
        jobs = [job for i, job in enumerate(jobs) if flags[i]]
        logger.debug(f"Best solution cost {cpus} cpus with {len(jobs)} jobs "
                     f"selected from {num_jobs} jobs in total.")
        return jobs

    @staticmethod
    def fitness(individual, jobs: Sequence[Job]):
        resource_cost = TaskConfig().resources()
        for k, v in resource_cost.items():
            resource_cost[k] = 0
        for selected, job in zip(individual, jobs):
            if selected:
                task_config: TaskConfig = job.task.config
                for k, v in task_config.resources().items():
                    resource_cost[k] += v
        num_cpu = resource_cost['cpu']
        for k, v in resource_cost.items():
            if v > config.get(k, 99999):
                num_cpu = 0
                break

        return num_cpu


class Scheduler(object):
    def __init__(self, wait_time=1):
        self.wait_time = wait_time
        self.tasks: Dict[Task, TaskState] = defaultdict(TaskState)
        self.solver = GaSolver()
        self.error_jobs = []
        self.count = 0

    def schedule(self, task: Task, job, *args, **kwargs) -> Job:
        job = Job(job, args, kwargs, task)
        if task not in self.tasks:
            task_id = process.add_task(
                str(task),
                task=task,
                pending=0,
                running=0,
                done=0,
                total=1
            )
            self.tasks[task].task_id = task_id
            process.start_task(task_id)
        self.tasks[task].pending.add(job)
        self.tasks[task].wait_q.put_nowait(1)
        return job

    def get_state(self, task):
        assert task in self.tasks
        return self.tasks[task]

    @property
    def valid_pending_jobs(self):
        jobs = []
        for task, state in self.tasks.items():
            remain_fork = task.config.fork - len(state.running)
            jobs += list(state.pending)[:remain_fork]
        return jobs

    def jobs(self, mode: str):
        jobs = []
        for task, state in self.tasks.items():
            jobs += list(getattr(state, mode))
        return jobs

    @property
    def running_jobs(self):
        return self.jobs('running')

    @property
    def pending_jobs(self):
        return self.jobs('pending')

    @property
    def done_jobs(self):
        return self.jobs('done')

    def run(self, job: Job):
        async def _run():
            state = self.tasks[job.task]
            try:
                res = await job.runner(*job.args, **job.kwargs)
            except Exception as e:
                # record error, since the future are never been waited
                job.set_exception(e)
                self.error_jobs.append(job)
                raise e
            finally:
                # always move to done
                state.running.remove(job)
                state.done.add(job)
                await state.wait_q.get()
                state.wait_q.task_done()

                # call awaitable callbacks
            for callback in job.async_done_callbacks:
                await callback(res)
            job.set_result(res)
            return res

        state = self.tasks[job.task]
        state.pending.remove(job)
        state.running.add(job)
        # cost resources
        job.task.config.cost_resources(config)

        def release_resources(*args, **kwargs):
            job.task.config.release_resources(config)

        job.add_done_callback(release_resources)
        job.future = asyncio.ensure_future(_run())

    async def execute(self, **kwargs):

        process.start()
        while True:
            pending_jobs = self.valid_pending_jobs
            if not (len(self.running_jobs) and len(pending_jobs) < 3):
                try:
                    # TODO sometimes this leads to error
                    jobs = self.solver.solve(pending_jobs)
                except Exception:
                    jobs = pending_jobs
                for job in jobs:
                    self.run(job)
            # handle error
            if len(self.error_jobs):
                for job in self.error_jobs:
                    raise job.exception()
            # update message
            for task, state in self.tasks.items():
                pending = len(state.pending)
                running = len(state.running)
                done = len(state.done)
                # print(pending, running, done)
                process.update(
                    task_id=state.task_id,
                    pending=pending,
                    running=running,
                    done=done
                )

                if state.all_submitted.is_set() and state.wait_q.empty():
                    process.update(state.task_id, completed=1)

            await asyncio.sleep(self.wait_time)
        process.stop()
