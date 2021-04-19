import asyncio
import inspect
from concurrent.futures import ProcessPoolExecutor, Future
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from functools import partial
from typing import Set, Callable, Awaitable, Any, Sequence, Optional, Coroutine, Generator, AsyncGenerator

from flowsaber.utility.logtool import get_logger

logger = get_logger(__name__)

AsyncFunc = Callable[[Any], Awaitable[None]]


class Solver(object):
    def solve(self, *args, **kwargs):
        raise NotImplementedError


class GaSolver(Solver):
    def __init__(self, score_func: Callable = None):
        self.score_func = score_func

    def solve(self, items: Sequence) -> Sequence:
        from pyeasyga import pyeasyga
        if len(items) <= 0 or self.score_func is None:
            return items

        ga = pyeasyga.GeneticAlgorithm(list(items))
        ga.fitness_function = partial(self.fitness, score_func)
        ga.run()
        max_score, flags = ga.best_individual()

        return max_score, flags

    def fitness(self, score_func, individual, items: Sequence):
        assert len(items)
        total = None
        for selected, item in zip(individual, items):
            if selected:
                if total is None:
                    total = item
                else:
                    total += item
        return score_func(total)


class TaskScheduler(object):
    @dataclass
    class Job(asyncio.Future):
        coro: Coroutine
        task: Optional[asyncio.Task] = None
        async_done_callbacks: list = field(default_factory=list)
        cost: Callable = None

        def add_async_done_callback(self, callback: AsyncFunc):
            assert inspect.iscoroutinefunction(callback)
            self.async_done_callbacks.append(callback)

        def remove_async_done_callback(self, callback):
            self.async_done_callbacks.remove(callback)

        def cancel(self, msg: Optional[str] = ...) -> bool:
            if self.task:
                self.task.cancel(msg=msg)
            return super().cancel(msg=msg)

        def __post_init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def __hash__(self):
            return hash(self.coro)

    def __init__(self, wait_time=1, score_func: Callable = None):
        self.pending_jobs: Set["TaskScheduler.Job"] = set()
        self.running_jobs: Set["TaskScheduler.Job"] = set()
        self.done_jobs: Set["TaskScheduler.Job"] = set()
        self.solver = GaSolver(score_func=score_func)
        self.error_jobs = []
        self.wait_time = wait_time

        self._running: Optional[asyncio.Event] = None

    @asynccontextmanager
    async def start(self) -> AsyncGenerator['TaskScheduler']:
        try:
            loop_fut = asyncio.ensure_future(self.start_loop())
            yield self
        finally:
            if self._running is not None:
                self._running.clear()

            await loop_fut

    def create_task(self, coro: Coroutine, cost: Callable = None) -> "TaskScheduler.Job":
        job = self.Job(coro, cost=cost)
        # add to tasks
        self.pending_jobs.add(job)
        # self.tasks[task].wait_q.put_nowait(1)
        return job

    async def start_loop(self, **kwargs):
        self._running = asyncio.Event()
        self._running.set()

        while True:
            # use multiple knapsack problem sover to find the best subset of jobs given score_func
            try:
                # TODO sometimes this leads to error
                maydo = [job for job in self.pending_jobs if job.cost is not None]
                mustdo = [job for job in self.pending_jobs if job.cost is None]
                max_score, selected = self.solver.solve([job.cost() for job in maydo])
                jobs = [job for i, job in enumerate(maydo) if selected[i]] + mustdo
            except Exception:
                jobs = pending_jobs

            for job in jobs:
                job = self.run_job(job)
            # handle error
            if len(self.error_jobs):
                for job in self.error_jobs:
                    raise job.exception()
            # break if not _running and no pending and _running jobs
            if not self._running.is_set() and len(self.running_jobs) + len(self.pending_jobs) == 0:
                break
            await asyncio.sleep(self.wait_time)

        self._running = None

    def run_job(self, job: "TaskScheduler.Job"):
        async def _run_job():
            try:
                res = await job.coro
            except Exception as e:
                # record error, since the future are never been waited
                self.error_jobs.append(job)
                raise e
            finally:
                # always move to done
                self.running_jobs.remove(job)
                self.done_jobs.add(job)
            # call awaitable callbacks
            for callback in job.async_done_callbacks:
                await callback(res)
            return res

        # remove from pending to _running
        self.pending_jobs.remove(job)
        self.running_jobs.add(job)

        # float up exception or result
        def run_job_done_callback(f: asyncio.Future):
            if f.exception():
                job.set_exception(f.exception())
            elif not f.cancelled():
                job.set_result(f.result())
            # cancel should be triggered by job

        job.task = asyncio.create_task(_run_job())
        job.task.add_done_callback(run_job_done_callback)

        return job


class FlowScheduler(object):
    def __init__(self, executor_cls=ProcessPoolExecutor, executor_kwargs: dict = None):
        self.executor = executor_cls(**(executor_kwargs or {}))
        self.futures: Set[Future] = set()

    @contextmanager
    def start(self) -> Generator['FlowScheduler']:
        try:
            yield self
        finally:
            self.executor.shutdown(wait=True)
            self.futures.clear()

    def create_task(self, fn) -> Future:
        fut = self.executor.submit(fn)
        fut.add_done_callback(self.remove_future)
        self.futures.add(fut)
        return fut

    def remove_future(self, fut):
        self.futures.remove(fut)
