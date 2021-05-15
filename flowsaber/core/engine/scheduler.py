import asyncio
from concurrent.futures import ProcessPoolExecutor, Future
from dataclasses import dataclass
from typing import Set, Any, Sequence, Optional, Coroutine


@dataclass
class Job(asyncio.Future):
    coro: Coroutine
    task: Optional[asyncio.Task] = None
    data: Any = None

    def cancel(self, *args, **kwargs) -> bool:
        if self.task and not self.task.done():
            self.task.cancel(*args, **kwargs)
        return super().cancel(*args, **kwargs)

    def __post_init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __hash__(self):
        return hash(self.coro)


class TaskManager(object):

    def select_jobs(self, jobs: Sequence[Job]):
        return [True] * len(jobs)

    def job_start(self, job: Job):
        pass

    def job_end(self, job: Job):
        pass


class Scheduler(object):
    """Scheduler object should support create_task method that returns a Future object.
    """

    def create_task(self, *args, **kwargs) -> Future:
        raise NotImplementedError


class TaskSchedulerError(RuntimeError):
    pass


class TaskScheduler(Scheduler):
    """Async task scheduler with support of a custom task_manger.
    """

    def __init__(self, wait_time=1, task_manger: TaskManager = None):
        self.pending_jobs: Set[Job] = set()
        self.running_jobs: Set[Job] = set()
        self.done_jobs: Set[Job] = set()
        self.error_jobs = []
        # task_manager
        self.task_manager = task_manger or TaskScheduler()
        # loop related
        self.wait_time = wait_time
        self._running: Optional[asyncio.Event] = None
        self._loop = None
        self._loop_task: Optional[asyncio.Task] = None

    async def __aenter__(self):
        """Careful, there may be situation in which error raised in async with, but the loop has
        not been started yet. For this situation, we leave the async within only after the loop has been ran

        Returns
        -------

        """
        loop = asyncio.get_running_loop()
        assert loop.is_running(), "The scheduler should started within a running asyncio event loop"
        self._loop = loop
        # exception handler will not work if we assign it a variable, https://bugs.python.org/issue39839
        self._running = asyncio.Event()
        self._loop_task = asyncio.create_task(self.start_loop())
        await self._running.wait()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._running.clear()
        await self._loop_task
        self._running = None
        self._loop_task = None
        self._loop = None

    def create_task(self, coro: Coroutine, data=None) -> Job:
        job = Job(coro, data=data)
        self.pending_jobs.add(job)
        return job

    async def start_loop(self, **kwargs):
        assert self._running is not None, "Please use `async with scheduler` statement to start the scheduler"
        self._running.set()

        try:
            while True:
                # break if not _running and no pending and _running jobs
                await asyncio.sleep(self.wait_time)
                if not self._running.is_set() and len(self.running_jobs) + len(self.pending_jobs) == 0:
                    break
                # use multiple knapsack problem sover to find the best subset of jobs given score_func
                jobs = list(self.pending_jobs)
                if not jobs:
                    continue
                selected = self.task_manager.select_jobs(jobs)
                jobs = [job for i, job in enumerate(jobs) if selected[i]]
                for job in jobs:
                    job = self.run_job(job)

        except Exception as exc:
            raise TaskSchedulerError from exc
        finally:
            self._running.clear()
            # this ensures the async task related to this schedule will return
            for task in (self.pending_jobs | self.running_jobs):
                task.cancel()

    def run_job(self, job: Job):
        async def async_run_job():
            try:
                # infer task_manager this job is started
                self.task_manager.job_start(job)
                res = await job.coro
            except Exception as e:
                # record error, since the future are never been waited
                self.error_jobs.append(job)
                raise e
            finally:
                # always move to done
                self.running_jobs.remove(job)
                self.done_jobs.add(job)  # no need to store it
                self.task_manager.job_end(job)
            return res

        # remove from pending to _running
        self.pending_jobs.remove(job)
        self.running_jobs.add(job)

        # propagate exception or result
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

        job.task = asyncio.create_task(async_run_job())
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
