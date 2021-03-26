import asyncio
from collections import abc
from queue import SimpleQueue
from typing import Union, Sequence, Dict, Optional

from pyflow.core.target import END, End
from pyflow.utility.logtool import get_logger

logger = get_logger(__name__)


class DataObject(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)


class Fetcher(DataObject):

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


class Queue(Fetcher):
    def __init__(self, ch, consumer, queue_factory, **kwargs):
        super().__init__(**kwargs)
        self.consumer = consumer
        self.ch: 'Channel' = ch
        self.queue_factory = queue_factory
        self._queue: Optional[asyncio.Queue] = None

    @property
    def queue(self):
        if self._queue is None:
            self._queue = self.queue_factory()
        return self._queue

    async def get(self):
        if not self.ch.async_activated:
            self.ch.activate()
        return await self.queue.get()

    def get_nowait(self):
        return self.queue.get_nowait()

    def put_nowait(self, item):
        return self.queue.put_nowait(item)

    async def put(self, item):
        return await self.queue.put(item)

    def empty(self):
        return self.queue.empty()


class Channel(DataObject):

    def __init__(self, name="", task=None, queue_factory: type = asyncio.Queue, **kwargs):
        super().__init__(**kwargs)
        # info
        self.task = task
        self.name = name
        self.name_with_id = True
        self.name = f"{task or ''}-{self}".lstrip('-')
        # _input
        self.buffer = SimpleQueue()
        self.async_activated = False
        self.queues: Dict[str, Queue] = {}
        self.queue_factory = queue_factory

    def __repr__(self):
        name = f"{self.name}|{type(self).__name__}({type(self).__bases__[0].__name__})"
        if self.name_with_id:
            name += f"[{hex(hash(self))}]"
        return name.lstrip('|')

    def put_nowait(self, item):
        if self.async_activated:
            for q in self.queues.values():
                q.put_nowait(item)
        else:
            self.buffer.put(item)

    def activate(self):
        if not self.async_activated:
            self.async_activated = True
            if self.buffer.qsize():
                # always put a END
                self.buffer.put(END)
                while not self.buffer.empty():
                    _item = self.buffer.get()
                    for q in self.queues.values():
                        q.put_nowait(_item)

    async def put(self, item):
        for q in self.queues.values():
            await q.put(item)

    def create_queue(self, consumer, key=None):
        q = Queue(ch=self, consumer=consumer, queue_factory=self.queue_factory)
        key = key or str(hash(q))
        if key in self.queues:
            raise ValueError(f"The chosen key {key} exists in this channel. Use a different key.")
        self.queues[key] = q
        return q

    def __lshift__(self, other):
        """
        ch << 1 == ch.put_nowait(1)
        """
        self.put_nowait(other)
        return self

    def __rshift__(self, tasks) -> Union['Channel', Sequence['Channel']]:
        """
        ch >> task                   -> task(ch)
        ch >> [task1, tasks, task3]  -> [task1(ch), task2(ch), task3(ch)]
        """
        if isinstance(tasks, abc.Sequence):
            outputs = [task(self) for task in tasks]
            if isinstance(tasks, tuple):
                outputs = tuple(tasks)
            return outputs

        else:
            return tasks(self)

    def __or__(self, tasks) -> Union['Channel', Sequence['Channel']]:
        """
        ch | [a, b, c, d] equals to ch >> [a, b, c, d]
        """
        return self >> tasks

    @staticmethod
    def value(value, **kwargs):
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

    @staticmethod
    def end():
        return ConstantChannel()

    @staticmethod
    def values(*args):
        """
        Channel.values(1, 2, 3, 4, 5)
        QueueChannel created by this method will always include a END signal
        """
        ch = Channel()
        for item in args:
            ch.put_nowait(item)
        ch.put_nowait(END)
        return ch

    @staticmethod
    def from_list(items: Sequence):
        """
        Channel.from_list([1, 2, 3, 4, 5])
        QueueChannel created by this method will always include a END signal
        """
        return Channel.values(*items)

    @staticmethod
    def from_path(path: Union[str, Sequence[str]], hidden: bool = False, type="file", check_exit: bool = True):
        """
        files = Channel.fromPath( '_input/**.fa' )
        """
        pass

    @staticmethod
    def from_file_pairs(path: str):
        """
        Channel.from_file_pairs('/my/_input/SRR*_{1,2}.fastq')

        [SRR493366, [/my/_input/SRR493366_1.fastq, /my/_input/SRR493366_2.fastq]]
        """
        pass

    @staticmethod
    def from_SRA(sra_id: Union[str, Sequence[str]]):
        """
        Channel.from_SRA('SRP043510')
        [SRR1448794, ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR144/004/SRR1448794/SRR1448794.fastq.gz]
        [SRR1448795, ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR144/005/SRR1448795/SRR1448795.fastq.gz]

        https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch
        """
        pass

    @staticmethod
    def watch_paths(paths: Union[str, Sequence[str]]):
        pass

    # Operators defined for use of ide, the true definition is dynamically generated


class ConstantQueue(object):
    def __init__(self):
        self.value = END

    def put_nowait(self, item):
        self.value = item

    async def put(self, item):
        return self.put_nowait(item)

    def get_nowait(self):
        return self.value

    async def get(self):
        return self.get_nowait()

    @staticmethod
    def empty():
        return False


class ConstantChannel(Channel):
    def __init__(self, **kwargs):
        super().__init__(queue_factory=ConstantQueue, **kwargs)


class Consumer(Fetcher):
    def __init__(self, queues: Sequence[Queue], **kwargs):
        super().__init__(**kwargs)
        self.queues = list(queues)
        assert all(isinstance(q, Queue) for q in self.queues)

    def __len__(self):
        return len(self.queues)

    async def get(self) -> Union[tuple, End]:
        values = []
        for q in self.queues:
            value = await q.get()
            if isinstance(value, End):
                return END
            else:
                values.append(value)
        if len(self.queues) == 1:
            return values[0]
        else:
            return tuple(values)

    def get_nowait(self) -> Union[tuple, End]:
        values = []
        for q in self.queues:
            value = q.get_nowait()
            if isinstance(value, End):
                return END
            else:
                values.append(value)
        if len(self.queues) == 1:
            return values[0]
        else:
            return tuple(values)

    @classmethod
    def from_channels(cls, channels: Sequence[Union[Channel, object]], consumer=None, **kwargs) -> 'Consumer':
        if not isinstance(channels, abc.Sequence):
            channels = [channels]
        else:
            channels = list(channels)
        for i, ch in enumerate(channels):
            if not isinstance(ch, Channel):
                if isinstance(ch, (tuple, list)) and any(isinstance(v, Channel) for v in ch):
                    raise ValueError(f"The _input: {ch} is a list/tuple of channels, "
                                     f"please unwrap it before pass into a Task/Flow")
                else:
                    channels[i] = Channel.values(ch)
        queues = []
        for ch in channels:
            queues.append(ch.create_queue(consumer=consumer))
        return cls(queues, **kwargs)
