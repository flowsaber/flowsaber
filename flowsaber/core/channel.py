import asyncio
import inspect
from collections import abc
from collections import deque
from typing import Union, Sequence, Optional, List

import flowsaber
from flowsaber.core.utility.target import END, End
from flowsaber.server.database.models import ChannelInput


class EventChannelCheckError(RuntimeError):
    pass


class Fetcher(object):
    """Fetch simple provide a for/async for method support for classes implemented with get/get_nowait methods.
    The end of __next__/__anext__ is triggered by the appearance of END fetched from get/get_nowait.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.get()
        if isinstance(value, End):
            raise StopAsyncIteration
        else:
            return value

    def __iter__(self):
        return self

    def __next__(self):
        value = self.get_nowait()
        if isinstance(value, End):
            raise StopIteration
        else:
            return value

    async def get(self):
        return self.get_nowait()

    def get_nowait(self):
        raise NotImplementedError


class ConstantQueue(object):
    """A async queue will emit it's internal value infinitely. Like ordinary queue, the first element
    needs to be enqueued before fetching"""
    NOTSET = object()

    def __init__(self):
        self.value = self.NOTSET
        self.has_value = asyncio.Event()

    def put_nowait(self, item):
        if not self.has_value.is_set():
            self.has_value.set()
            self.value = item  # only set once

    async def put(self, item):
        self.put_nowait(item)

    def get_nowait(self):
        if self.value is self.NOTSET:
            raise RuntimeError("The ConstantQueue is not initialized, please use ch.put/ch.put_nowait "
                               "to set the initial value")
        return self.value

    async def get(self):
        await self.has_value.wait()
        return self.get_nowait()

    def empty(self):
        return self.value is not self.NOTSET


class LazyAsyncQueue(Fetcher):
    """Internally it's an asyncio.Queue, but the inner queue will only be created when meets the first call
    it's fetcher/putter methods. It's designed like this to handler pickle or coroutine-loop problems.

    """

    def __init__(self, ch, queue_factory, **kwargs):
        super().__init__(**kwargs)
        self.ch: Channel = ch
        self.queue_factory = queue_factory
        self.queue: Optional[Union[asyncio.Queue, ConstantQueue]] = None

    def initialize_queue(self):
        if self.queue is None:
            self.queue = self.queue_factory()

    async def get(self):
        self.initialize_queue()
        if not self.ch.initialized:
            self.ch.initialize()
        return await self.queue.get()

    def get_nowait(self):
        self.initialize_queue()
        return self.queue.get_nowait()

    def put_nowait(self, item):
        self.initialize_queue()
        return self.queue.put_nowait(item)

    async def put(self, item):
        self.initialize_queue()
        if not self.ch.initialized:
            self.ch.initialize()
        return await self.queue.put(item)

    def empty(self):
        return self.queue.empty()


class ChannelBase(object):
    """A channel it self is an object for storing items by using put/put_nowait. To consume data from the channel,
    users must call ChannelBase.create_queue method to get a fetcher LazyAsyncQueue, and then call it's get/get_nowait method
    to fetched the data emitted by the channel.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    def put_nowait(self, item):
        raise NotImplementedError

    async def put(self, item):
        return self.put_nowait(item)

    def create_queue(self) -> LazyAsyncQueue:
        raise NotImplementedError

    def __lshift__(self, other):
        """
        ch << 1 == ch.put_nowait(1)
        """
        self.put_nowait(other)
        return self

    def __rshift__(self, tasks) -> Union['Channel', Sequence['Channel']]:
        """
        ch >> task                   -> task(ch)
        ch >> [task1, _tasks, task3]  -> [task1(ch), task2(ch), task3(ch)]
        """
        if not isinstance(tasks, abc.Sequence):
            return tasks(self)

        outputs = [task(self) for task in tasks]
        if isinstance(tasks, tuple):
            outputs = tuple(tasks)
        return outputs

    def __or__(self, tasks) -> Union['Channel', Sequence['Channel']]:
        """
        ch | [a, b, c, d] equals to ch >> [a, b, c, d]
        """
        return self >> tasks

    def __getitem__(self, key) -> "Channel":
        """
        ch = Channel.value({'name': 'asd', 'age': 2})
        name_channel = ch['name'] equals to GetItem('name')(ch)
        """
        from flowsaber.core.operators import GetItem
        getitem_task = GetItem(key=key)
        return getitem_task(self)

    @classmethod
    def value(cls, value, **kwargs) -> 'ConstantChannel':
        """
        Channel._output(1)
        """
        if callable(value):
            raise ValueError("You has passed a callable object as inputs, "
                             "you should explicitly specify the argument name like:"
                             "`ch.map(by=lambda x : x)`.")
        ch = ConstantChannel(**kwargs)
        ch.put_nowait(value)
        return ch

    @classmethod
    def end(cls, **kwargs) -> 'ConstantChannel':
        return cls.value(END)

    @classmethod
    def values(cls, *args) -> 'Channel':
        """
        Channel.values(1, 2, 3, 4, 5)
        QueueChannel created by this method will always include a END signal
        """
        ch = Channel()
        for item in args:
            ch.put_nowait(item)
        ch.put_nowait(END)
        return ch

    @classmethod
    def from_list(cls, items: Sequence) -> "Channel":
        """
        Channel.from_list([1, 2, 3, 4, 5])
        QueueChannel created by this method will always include a END signal
        """
        return cls.values(*items)

    @classmethod
    def async_channel(cls, *async_funcs, **kwargs) -> "AsyncChannel":
        async_ch = AsyncChannel(**kwargs)
        for async_func in async_funcs:
            async_ch(async_func)
        return async_ch

    @classmethod
    def event(cls, *funcs, interval: int = 5, value=None, **kwargs) -> "EventChannel":
        event_ch = EventChannel(interval, value, **kwargs)
        for func in funcs:
            event_ch(func)
        return event_ch


class Channel(ChannelBase):
    """Subclass of ChannelBase implemented create_queue method, the mechanism for sending data to all created queue is
    simple, it just loop over all queues and push the item specified by call of put/put_nowait into all queues.
    Furthermore, to make LazyAsyncQueue initialized only in a running event loop, Channel uses a buffer to buffer all
    items pushed before entering the event loop.
    """

    def __init__(self, queue_factory: type = asyncio.Queue, **kwargs):
        super().__init__(**kwargs)
        self.buffer: deque = deque()
        self.initialized = False
        self.queues: List[LazyAsyncQueue] = []
        self.queue_factory = queue_factory
        # context info
        self.id = flowsaber.context.random_id
        self.task_id = flowsaber.context.get('task_id', None)
        self.flow_id = flowsaber.context.get('flow_id', None)

    def serialize(self) -> ChannelInput:
        return ChannelInput(
            id=self.id,
            task_id=self.task_id,
            flow_id=self.flow_id
        )

    def initialize(self):
        if not self.initialized:
            self.initialized = True
            if self.buffer:
                while self.buffer:
                    self.put_nowait(self.buffer.popleft())

    def put_nowait(self, item):
        if self.initialized:
            for q in self.queues:
                q.put_nowait(item)
        else:
            self.buffer.append(item)

    async def put(self, item):
        self.initialize()
        for q in self.queues:
            await q.put(item)

    def create_queue(self) -> LazyAsyncQueue:
        q = LazyAsyncQueue(ch=self, queue_factory=self.queue_factory)
        self.queues.append(q)
        return q


class ConstantChannel(Channel):
    """A channel use ConstantQueue as it's fetcher queue factory.
    """

    def __init__(self, **kwargs):
        super().__init__(queue_factory=ConstantQueue, **kwargs)


class AsyncChannel(Channel):
    """AsyncChannel schedules asynchronous tasks when initialized, the async task accept the channel as the only parameter,
    and it's expected to call self.put_nowait/self.put method as to send items into output queues.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._async_tasks = []
        self._async_futures = []
        self._num_done = 0

    def __call__(self, async_func):
        # TODO assert func is async func
        assert not self.initialized, "Can only add async task before initialization."
        self._async_tasks.append(async_func)
        return self

    def check_all_task_done(self, *args, **kwargs):
        self._num_done += 1
        if self._num_done == len(self._async_futures):
            self.put_nowait(END)

    def initialize(self):
        if not self.initialized:
            super().initialize()
            loop = asyncio.get_running_loop()

            for a_func in self._async_tasks:
                task = loop.create_task(a_func(self))
                task.add_done_callback(self.check_all_task_done)
                self._async_futures.append(task)


class EventChannel(AsyncChannel):
    """A subclass of AsyncChannel, it's only async task runs in a while loop and periodically call the registered
    trigger methods
    """

    def __init__(self, interval: int = 5, value=None, **kwargs):
        super().__init__(**kwargs)
        self.interval = interval
        self.value = value
        self._async_tasks.append(self.check_triggers_loop)
        self._predicates = []

    @staticmethod
    async def check_triggers_loop(self: "EventChannel"):
        while self._predicates:
            remove_predicates = []
            for i, predicate in enumerate(self._predicates):
                try:
                    # handle four cases
                    # 1. generator 2. async_generator 3. func 4. async_func
                    if hasattr(predicate, '__next__'):
                        res = predicate.__next__()
                    elif hasattr(predicate, "__anext__"):
                        res = await predicate.__anext__()
                    else:
                        res = predicate(self)
                        if inspect.iscoroutine(res):
                            res = await res
                except (StopIteration, StopAsyncIteration) as e:
                    remove_predicates.append(predicate)
                except Exception as e:
                    raise EventChannelCheckError("Error met when call registered trigger methods with error: e") from e
                else:
                    if res:
                        self.put_nowait(self.value)
            for rm_predicate in remove_predicates:
                self._predicates.remove(rm_predicate)

            await asyncio.sleep(self.interval)

    def __call__(self, func):
        self._predicates.append(func)
        return self


class Consumer(Fetcher):
    """Consumer is an object used for simultaneously fetching data emitted by multiple channels and output tuples.
    Empty consumer will emit only once. The end of __next__/__anext__ will be triggered if any of it's source channel
    emits a END object. For simplicity, if there is only a single source channel, the output of consumer will not be
    a tuple.
    """

    def __init__(self, *queues: LazyAsyncQueue, **kwargs):
        super().__init__(**kwargs)
        self.queues: List[LazyAsyncQueue] = list(queues)
        assert all(isinstance(q, LazyAsyncQueue) for q in self.queues)
        self.num_emitted = 0

    @property
    def empty(self):
        return len(self.queues) == 0

    @property
    def single(self):
        return len(self.queues) == 1

    def __len__(self):
        return len(self.queues)

    async def get(self):
        # make sure empty inputs only emit once
        if self.empty and self.num_emitted >= 1:
            return END
        values = []
        for q in self.queues:
            value = await q.get()
            if isinstance(value, End):
                return END
            values.append(value)
        self.num_emitted += 1
        # emit single _input without tuple
        res = tuple(values) if not self.single else values[0]
        return res

    def get_nowait(self):
        # make sure empty inputs only emit once
        if self.empty and self.num_emitted >= 1:
            return END
        values = []
        for q in self.queues:
            value = q.get_nowait()
            if isinstance(value, End):
                return END
            values.append(value)
        self.num_emitted += 1
        # emit single _input without tuple
        return tuple(values) if not self.single else values[0]

    @classmethod
    def from_channels(cls, *channels: Sequence[Union[Channel, object]], **kwargs) -> 'Consumer':
        channels = list(channels)
        for i, ch in enumerate(channels):
            if not isinstance(ch, Channel):
                if isinstance(ch, (tuple, list)) and any(isinstance(v, Channel) for v in ch):
                    raise ValueError(f"The _input: {ch} is a list/tuple of channels, "
                                     f"please unwrap it before pass into a Task/Flow")
                else:
                    channels[i] = Channel.value(ch)
            else:
                if ch.initialized:
                    raise ValueError("Can not create consumer from activated Channel, try to create"
                                     " the consumer before _running the flow.")
        queues = [ch.create_queue() for ch in channels]
        return cls(*queues, **kwargs)


def _a(*args: Union[object, Channel]):
    pass


ARGS_SIG = list(inspect.signature(_a).parameters.values())[0]

Output = Union[Sequence[Channel], Channel]


def _b() -> Output:
    pass


OUTPUT_ANNOTATION = inspect.signature(_b).return_annotation
