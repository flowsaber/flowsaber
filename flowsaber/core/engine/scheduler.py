import asyncio
from concurrent.futures import ProcessPoolExecutor, Future
from dataclasses import dataclass
from functools import partial
from typing import Set, Callable, Awaitable, Any, Sequence, Optional, Coroutine

AsyncFunc = Callable[[Any], Awaitable[None]]


class Solver(object):
    """Multiple knapsack problem solver.

    """

    def solve(self, *args, **kwargs):
        raise NotImplementedError


class GaSolver(Solver):
    """Use genetic algorithm supplied by pyeasyga package to solve the mkp problem.

    """

    def __init__(self, score_func: Callable = None):
        self.score_func = score_func

    def solve(self, items: Sequence) -> Sequence:
        from pyeasyga import pyeasyga
        if len(items) <= 0 or self.score_func is None:
            return items

        ga = pyeasyga.GeneticAlgorithm(list(items))
        ga.fitness_function = partial(self.fitness, self.score_func)
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


class Scheduler(object):
    """Scheduler object should support create_task method that returns a Future object.

    """

    def create_task(self, *args, **kwargs) -> Future:
        raise NotImplementedError


class TaskScheduler(Scheduler):
    """A async task scheduler initialized with a scoring function. Each submited task is associated with a cost
    function, submited tasks will be selected and runned to ensure a maximum score.

    """

    @dataclass
    class Job(asyncio.Future):
        coro: Coroutine
        task: Optional[asyncio.Task] = None
        cost: Callable = None

        def cancel(self, *args, **kwargs) -> bool:
            if self.task and not self.task.done():
                self.task.cancel(*args, **kwargs)
            return super().cancel(*args, **kwargs)

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
        self.loop_fut: Optional[Future] = None

        self._running: Optional[asyncio.Event] = None

    async def __aenter__(self):
        """Becarefull, there may be situation in which error raised in async with, but the loop has
        not been started yet. For this situation, we leave the async within only after the loop has been ran

        Returns
        -------

        """
        self._running = asyncio.Event()
        self.loop_fut = asyncio.ensure_future(self.start_loop())
        await self._running.wait()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._running.clear()
        await self.loop_fut
        self._running = None
        self.loop_fut = None

    def create_task(self, coro: Coroutine, cost: Callable = None) -> "TaskScheduler.Job":
        job = self.Job(coro, cost=cost)
        # add to tasks
        self.pending_jobs.add(job)
        # self.tasks[task].wait_q.put_nowait(1)
        return job

    async def start_loop(self, **kwargs):
        assert self._running is not None, "Please use `async with scheduler` statement to start the scheduler"
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
                jobs = tuple(self.pending_jobs)

            for job in jobs:
                job = self.run_job(job)
            # break if not _running and no pending and _running jobs
            if not self._running.is_set() and len(self.running_jobs) + len(self.pending_jobs) == 0:
                break
            await asyncio.sleep(self.wait_time)

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
                # self.done_jobs.add(job) # no need to store it
            return res

        # remove from pending to _running
        self.pending_jobs.remove(job)
        self.running_jobs.add(job)

        # propogate exception or result
        def run_job_done_callback(f: asyncio.Future):
            if f.cancelled():
                if not job.done():
                    job.cancel()
            elif f.exception():
                job.set_exception(f.exception())
            elif f.done():
                job.set_result(f.result())
                # cancel should be triggered by job
            else:
                raise RuntimeError(f"Unexpected future {f}")

        job.task = asyncio.create_task(_run_job())
        job.task.add_done_callback(run_job_done_callback)

        return job


class FlowScheduler(Scheduler):
    """Internally it's a ProcessExecutor, the wrapping is mainly used for uniformity with TaskScheduler.
    """

    def __init__(self, executor_cls=ProcessPoolExecutor, executor_kwargs: dict = None):
        self.executor_cls = executor_cls
        self.executor_kwargs = executor_kwargs or {}
        self.executor: executor_cls = None
        self.futures: Set[Future] = set()

    def __enter__(self) -> 'FlowScheduler':
        self.executor = self.executor_cls(**self.executor_kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown(wait=True)
        self.executor = None
        self.futures.clear()

    def create_task(self, *args, **kwargs) -> Future:
        fut = self.executor.submit(*args, **kwargs)
        fut.add_done_callback(self.remove_future)
        self.futures.add(fut)
        return fut

    def remove_future(self, fut):
        self.futures.remove(fut)
