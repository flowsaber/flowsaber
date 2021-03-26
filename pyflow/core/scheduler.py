import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Set, Callable, Dict

from pyflow.utility.logtool import get_logger
from .task import Task

logger = get_logger(__name__)


class Solver(object):
    pass


class MkpSolver(Solver):
    def __init__(self, **kwargs):
        pass

    @staticmethod
    def solve(jobs):
        return jobs


@dataclass
class Job:
    runner: Task
    done_callback: Callable
    args: tuple
    kwargs: dict
    task: Task
    future: asyncio.Future = None

    def __hash__(self):
        return id(self)


@dataclass
class TaskState:
    pending: Set[Job] = field(default_factory=set)
    running: Set[Job] = field(default_factory=set)
    done: Set[Job] = field(default_factory=set)
    wait_q: asyncio.Queue = field(default_factory=asyncio.Queue)

    def __iter__(self):
        return iter(self.__dict__.values())

    def __len__(self):
        return len(self.__dict__)


class Scheduler(object):
    def __init__(self):
        self.tasks: Dict[Task, TaskState] = defaultdict(TaskState)
        self.solver = MkpSolver()
        self.error_futures = []

    def schedule(self, task: Task, done_callback, job, *args, **kwargs):
        self.tasks[task].pending.add(Job(job, done_callback, args, kwargs, task))
        self.tasks[task].wait_q.put_nowait(1)

    def get_state(self, task):
        assert task in self.tasks
        return self.tasks[task]

    def get_wait_q(self, task):
        return self.get_state(task).wait_q

    @property
    def pending_jobs(self):
        jobs = []
        for task, state in self.tasks.items():
            remain_fork = task.config.max_fork - len(state.running)
            jobs += list(state.pending)[:remain_fork]
        return jobs

    def run(self, job: Job):
        async def _run():
            state = self.tasks[job.task]
            state.pending.remove(job)
            state.running.add(job)
            logger.debug("run start in scheculer.run._run")
            try:
                res = await job.runner.run_job(*job.args, **job.kwargs)
                await job.done_callback(res)
            except Exception as e:
                # record error, since the future are never been waited
                self.error_futures.append(job.future)
                raise e
            state.running.remove(job)
            state.done.add(job)
            await state.wait_q.get()
            state.wait_q.task_done()
            return res

        job.future = asyncio.ensure_future(_run())
        return

    async def execute(self, **kwargs):
        while True:
            jobs = self.solver.solve(self.pending_jobs)
            for job in jobs:
                self.run(job)
            if len(self.error_futures):
                for fut in self.error_futures:
                    raise fut.exception()
            await asyncio.sleep(0.05)

    def check_error(self):
        for task, state in self.tasks.items():
            if state.pending:
                raise ValueError(f"This jobs is not scheduled: {state.pending}")
            if state.running:
                for job in state.running:
                    if job.future.exception():
                        raise job.future.exception()
                raise ValueError(f"This jobs are still running: {state.running}")
            for job in state.done:
                if job.future.exception():
                    raise job.future.exception()
