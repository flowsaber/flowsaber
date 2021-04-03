"""
Borrowed from prefect.executors.dask
"""

import asyncio
import inspect
import logging
import sys
import uuid
import weakref
from contextlib import asynccontextmanager
from io import StringIO
from typing import Union, Callable, Optional, Any, Iterator
from concurrent.futures import Future

from flowsaber.utility.importtools import import_object
from flowsaber.utility.logtool import get_logger

logger = get_logger(__name__)


class CaptureTerminal(object):
    def __init__(self, stdout=None, stderr=None):
        if isinstance(stdout, str):
            stdout = open(stdout, 'a')
        if isinstance(stderr, str):
            stderr = open(stderr, 'a')
        self.stdout = stdout
        self.stderr = stderr
        self.stringio_stdout = StringIO()
        self.stringio_stderr = StringIO()

    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = self.stdout or self.stringio_stdout
        sys.stderr = self.stderr or self.stringio_stderr
        return self

    def __exit__(self, *args):
        self.stringio_stdout = StringIO()
        self.stringio_stderr = StringIO()
        sys.stdout = self._stdout
        sys.stderr = self._stderr


class Executor(object):
    def __init__(self, **kwargs):
        pass

    async def run(self, fn, *args, **kwargs):
        raise NotImplementedError

    @asynccontextmanager
    async def start(self):
        yield self


class Local(Executor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def run(self, fn, *args, **kwargs):
        if inspect.iscoroutine(fn):
            return await fn(*args, **kwargs)
        else:
            return fn(*args, **kwargs)


class DaskExecutor(Executor):
    """
    An executor that runs all functions using the `dask.distributed` scheduler.

    Check https://docs.dask.org/en/latest/setup.html for all kinds of cluster types.

    By default a temporary `distributed.LocalCluster` is created (and
    subsequently torn down) within the `start_loop()` contextmanager. To use a
    different cluster class (e.g.
    [`dask_kubernetes.KubeCluster`](https://kubernetes.dask.org/)), you can
    specify `cluster_class`/`cluster_kwargs`.

    Alternatively, if you already have a dask cluster running, you can provide
    the address of the scheduler via the `address` kwarg.

    Note that if you have tasks with tags of the form `"dask-resource:KEY=NUM"`
    they will be parsed and passed as
    [Worker Resources](https://distributed.dask.org/en/latest/resources.html)
    of the form `{"KEY": float(NUM)}` to the Dask Scheduler.

    Args:
        - address (string, optional): address of a currently running dask
            scheduler; if one is not provided, a temporary cluster will be
            created in `executor.start_loop()`.  Defaults to `None`.
        - cluster_class (string or callable, optional): the cluster class to use
            when creating a temporary dask cluster. Can be either the full
            class name (e.g. `"distributed.LocalCluster"`), or the class itself.
        - cluster_kwargs (dict, optional): addtional kwargs to pass to the
           `cluster_class` when creating a temporary dask cluster.
        - adapt_kwargs (dict, optional): additional kwargs to pass to `cluster.adapt`
            when creating a temporary dask cluster. Note that adaptive scaling
            is only enabled if `adapt_kwargs` are provided.
        - client_kwargs (dict, optional): additional kwargs to use when creating a
            [`dask.distributed.Client`](https://distributed.dask.org/en/latest/api.html#client).
        - debug (bool, optional): When running with a local cluster, setting
            `debug=True` will increase dask's logging level, providing
            potentially useful debug info. Defaults to the `debug` value in
            your Prefect configuration.

    Examples:

    Using a temporary local dask cluster:

    ```python
    executor = DaskExecutor()
    ```

    Using a temporary cluster running elsewhere. Any Dask cluster class should
    work, here we use [dask-cloudprovider](https://cloudprovider.dask.org):

    ```python
    executor = DaskExecutor(
        cluster_class="dask_cloudprovider.FargateCluster",
        cluster_kwargs={
            "image": "prefecthq/prefect:latest",
            "n_workers": 5,
            ...
        },
    )
    ```

    Connecting to an existing dask cluster

    ```python
    executor = DaskExecutor(address="192.0.2.255:8786")
    ```
    """

    def __init__(
            self,
            address: str = None,
            cluster_class: Union[str, Callable] = None,
            cluster_kwargs: dict = None,
            adapt_kwargs: dict = None,
            client_kwargs: dict = None,
            debug: bool = False,
            **kwargs
    ):
        super().__init__(**kwargs)
        if address is not None:
            if cluster_class is not None or cluster_kwargs is not None:
                raise ValueError(
                    "Cannot specify both `address` and `cluster_class`/`cluster_kwargs`"
                )
        else:
            from distributed import Client
            from distributed.deploy.local import LocalCluster
            if isinstance(cluster_class, str):
                cluster_class = import_object(cluster_class)
            elif not cluster_class:
                cluster_class = LocalCluster

            self.cluster_class = cluster_class

            self.cluster_kwargs = {} if not cluster_kwargs else cluster_kwargs.copy()
            if cluster_class == LocalCluster:
                self.cluster_kwargs.setdefault(
                    'silence_logs', logging.CRITICAL if not debug else logging.WARNING
                )

            self.adapt_kwargs = {} if not adapt_kwargs else adapt_kwargs.copy()
            self.client_kwargs = {} if not client_kwargs else client_kwargs.copy()
            self.client_kwargs.setdefault('set_as_default', False)
            self.client: Optional[Client] = None

            self.address = address
            self._futures = None
            self._should_run_event = None
            self._watch_dask_events_task = None

    @asynccontextmanager
    async def start(self) -> Iterator[None]:
        """
        Context manager for initializing execution.

        Creates a `dask.distributed.Client` and yields it.
        """
        from distributed import Client

        try:
            if self.address is not None:
                async with Client(self.address, **self.client_kwargs, asynchronous=True) as client:
                    self.client = client
                    try:
                        self._pre_start_yield()
                        yield self
                    finally:
                        self._post_start_yield()
            else:
                # fk, all this two class need set asynchronous=True and use async with
                async with self.cluster_class(**self.cluster_kwargs,
                                              asynchronous=True,
                                              threads_per_worker=1) as cluster:  # type: ignore
                    if self.adapt_kwargs:
                        cluster.adapt(**self.adapt_kwargs)
                    async with Client(cluster, **self.client_kwargs, asynchronous=True) as client:
                        self.client = client
                        try:
                            self._pre_start_yield()
                            yield self
                        finally:
                            self._post_start_yield()
        finally:
            self.client = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._post_start_yield()
        self.client = None

    async def run(
            self, fn: Callable, *args: Any, extra_context: dict = None, **kwargs: Any
    ) -> Future:
        """
        Submit a function to the executor for execution. Returns a Future object.

        Args:
            - fn (Callable): function that is being submitted for execution
            - *args (Any): arguments to be passed to `fn`
            - extra_context (dict, optional): an optional dictionary with extra information
                about the submitted task
            - **kwargs (Any): keyword arguments to be passed to `fn`

        Returns:
            - Future: a Future-like object that represents the computation of `fn(*args, **kwargs)`
        """
        if self.client is None:
            raise ValueError("This executor has not been started.")

        kwargs.update(self._prep_dask_kwargs(extra_context))
        if self._should_run_event is None:
            fut = self.client.submit(fn, *args, **kwargs)
        else:
            fut = self.client.submit(
                self._maybe_run, self._should_run_event.name, fn, *args, **kwargs
            )
        res = await fut
        return res

    async def _watch_dask_events(self) -> None:
        scheduler_comm = None
        comm = None
        from distributed.core import rpc

        try:
            scheduler_comm = rpc(
                self.client.scheduler.address,  # type: ignore
                connection_args=self.client.security.get_connection_args("client"),  # type: ignore
            )
            # due to a bug in distributed's inproc comms, letting cancellation
            # bubble up here will kill the listener. wrap with a shield to
            # prevent that.
            comm = await asyncio.shield(scheduler_comm.live_comm())
            await comm.write({"op": "subscribe_worker_status"})
            _ = await comm.read()
            while True:
                try:
                    msgs = await comm.read()
                except OSError:
                    break
                for op, msg in msgs:
                    if op == "add":
                        for worker in msg.get("workers", ()):
                            logger.debug("Worker %s added", worker)
                    elif op == "remove":
                        logger.debug("Worker %s removed", msg)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug(
                "Failure while watching dask worker events", exc_info=True
            )
        finally:
            if comm is not None:
                try:
                    await comm.close()
                except Exception:
                    pass
            if scheduler_comm is not None:
                scheduler_comm.close_rpc()

    def _pre_start_yield(self) -> None:
        from distributed import Event

        is_inproc = self.client.scheduler.address.startswith("inproc")  # type: ignore
        if self.address is not None or is_inproc:
            self._futures = weakref.WeakSet()
            self._should_run_event = Event(
                f"flowsaber-{uuid.uuid4().hex}", client=self.client
            )
            self._should_run_event.set()

        self._watch_dask_events_task = asyncio.run_coroutine_threadsafe(
            self._watch_dask_events(), self.client.loop.asyncio_loop  # type: ignore
        )

    def _post_start_yield(self) -> None:
        from distributed import wait

        if self._watch_dask_events_task is not None:
            try:
                self._watch_dask_events_task.cancel()
            except Exception:
                pass
            self._watch_dask_events_task = None

        if self._should_run_event is not None:
            # Multipart cleanup, ignoring exceptions in each stage
            # 1.) Stop pending tasks from starting
            try:
                self._should_run_event.clear()
            except Exception:
                pass
            # 2.) Wait for all running tasks to complete
            try:
                futures = [f for f in list(self._futures) if not f.done()]  # type: ignore
                if futures:
                    logger.info(
                        "Stopping executor, waiting for %d active tasks to complete",
                        len(futures),
                    )
                    wait(futures)
            except Exception:
                pass
        self._should_run_event = None
        self._futures = None

    def _prep_dask_kwargs(self, extra_context: dict = None) -> dict:
        if extra_context is None:
            extra_context = {}

        dask_kwargs = {"pure": False}  # type: dict

        # set a key for the dask scheduler UI
        key = self._make_task_key(**extra_context)
        if key is not None:
            dask_kwargs["key"] = key

        # infer from context if dask resources are being utilized
        task_tags = extra_context.get("task_tags", [])
        dask_resource_tags = [
            tag for tag in task_tags if tag.lower().startswith("dask-resource")
        ]
        if dask_resource_tags:
            resources = {}
            for tag in dask_resource_tags:
                prefix, val = tag.split("=")
                resources.update({prefix.split(":")[1]: float(val)})
            dask_kwargs.update(resources=resources)

        return dask_kwargs

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state.update(
            {
                k: None
                for k in [
                "client",
                "_futures",
                "_should_run_event",
                "_watch_dask_events_task",
            ]
            }
        )
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)

    @staticmethod
    def _make_task_key(
            task_name: str = "", task_index: int = None, **kwargs: Any
    ) -> Optional[str]:
        """A helper for generating a dask task key from fields set in `extra_context`"""
        if task_name:
            suffix = uuid.uuid4().hex
            if task_index is not None:
                return f"{task_name}-{task_index}-{suffix}"
            return f"{task_name}-{suffix}"
        return None

    @staticmethod
    def _maybe_run(event_name: str, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Check if the task should run against a `distributed.Event` before
        starting the task. This offers stronger guarantees than distributed's
        current cancellation mechanism, which only cancels pending tasks."""
        import dask
        from distributed import Event, get_client

        try:
            # Explicitly pass in the timeout from dask's config. Some versions of
            # distributed hardcode this rather than using the value from the
            # config.  Can be removed once we bump our min requirements for
            # distributed to >= 2.31.0.
            timeout = dask.config.get("distributed.comm.timeouts.connect")
            event = Event(event_name, client=get_client(timeout=timeout))
            should_run = event.is_set()
        except Exception:
            # Failure to create an event is usually due to connection errors. These
            # are either due to flaky behavior in distributed's comms under high
            # loads, or due to the scheduler shutting down. Either way, the safest
            # course here is to assume we *should* run the task still. If we guess
            # wrong, we're either doing a bit of unnecessary work, or the cluster
            # is shutting down and the task will be cancelled anyway.
            should_run = True

        if should_run:
            return fn(*args, **kwargs)


def get_executor(executor: str = 'dask', *args, **kwargs):
    executors = {
        'local': Local,
        'dask': DaskExecutor
    }
    if executor not in executors:
        raise ValueError(f"{executor} not supported, please choose one of {executors.keys()}")
    return executors[executor](*args, **kwargs)
