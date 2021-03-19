import asyncio
from collections.abc import Iterable
from typing import Union, Sequence, Iterator


class Target(object):
    def __init__(self, data_type=None):
        pass


class End(Target):
    def __repr__(self):
        return "[END]"


class Channel(object):

    def __init__(self, name="", task=None, **kwargs):
        self.task = task
        self.flows = []
        self.name = name
        self.name_with_id = True
        self.name = f"{task or ''}-{self}".lstrip('-')
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    def __repr__(self):
        name = f"{self.name}|{type(self).__name__}({type(self).__bases__[0].__name__})"
        if self.name_with_id:
            name += f"[{hex(hash(self))}]"
        return name.lstrip('|')

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.get()
        if value is END:
            raise StopAsyncIteration
        else:
            return value

    def __iter__(self):
        yield self

    def __len__(self):
        return 1

    def __getitem__(self, index):
        assert index == 0, "This is a single Channel, index must be 0"
        return self

    async def get(self):
        return self.get_nowait()

    def get_nowait(self):
        raise NotImplementedError

    async def put(self, item):
        return self.put_nowait(item)

    def put_nowait(self, item):
        raise NotImplementedError

    def __lshift__(self, other):
        """
        ch << 1 == ch.put_nowait(1)
        """
        self.put_nowait(other)
        return self

    def empty(self):
        raise NotImplemented

    @staticmethod
    def value(value, **kwargs):
        """
        Channel.output(1)
        """
        return ValueChannel(value, **kwargs)

    def subscribe(self, on_next, on_complete):
        """
        How to implement ?
        """
        raise NotImplemented

    @staticmethod
    def end():
        return ValueChannel(END)

    @staticmethod
    def values(*args):
        """
        Channel.values(1, 2, 3, 4, 5)
        QueueChannel created by this method will always include a END signal
        """
        return QueueChannel(args)

    @staticmethod
    def from_list(items: Sequence):
        """
        Channel.from_list([1, 2, 3, 4, 5])
        QueueChannel created by this method will always include a END signal
        """
        return QueueChannel(items)

    @staticmethod
    def from_path(path: Union[str, Sequence[str]], hidden: bool = False, type="file", check_exit: bool = True):
        """
        files = Channel.fromPath( 'data/**.fa' )
        """
        pass

    @staticmethod
    def from_file_pairs(path: str):
        """
        Channel.from_file_pairs('/my/data/SRR*_{1,2}.fastq')

        [SRR493366, [/my/data/SRR493366_1.fastq, /my/data/SRR493366_2.fastq]]
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


class ValueChannel(Channel):
    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value
        if callable(value):
            raise ValueError("You has passed a callable object as inputs, "
                             "you should explicitly specify the argument name like:"
                             "`ch.map(by=lambda x : x)`.")

    def get_nowait(self):
        return self.value

    def put_nowait(self, item):
        self.value = item

    def empty(self):
        return False


class QueueChannel(Channel):
    def __init__(self, items: Union[None, Sequence] = None, **kwargs):
        super().__init__(**kwargs)
        assert items is None or isinstance(items, Iterable)
        self.initial_items = items
        self.async_queue = None

    @property
    def queue(self) -> asyncio.Queue:
        if self.async_queue is None:
            self.async_queue = asyncio.Queue()
            # as long as initial initial_items is not None, there will be a END
            if self.initial_items is not None:
                for item in self.initial_items:
                    self.queue.put_nowait(item)
                    # as end of stream signal
                self.async_queue.put_nowait(END)
        return self.async_queue

    def __getattribute__(self, item: str):
        if item in ['queue', 'async_queue']:
            return super().__getattribute__(item)
        if not item.startswith('_') and hasattr(asyncio.Queue, item):
            return getattr(self.queue, item)
        else:
            return super().__getattribute__(item)


class ChannelList(Channel):
    def __init__(self, channels: Sequence[Union['ChannelList', Channel]], **kwargs):
        super().__init__(**kwargs)
        channels = list(channels)
        for i, ch in enumerate(channels):
            if not isinstance(ch, Channel):
                channels[i] = Channel.value(ch, **kwargs)
        self.channels = channels
        # Note: when a channelist passed as a single channel, we use it's channels
        if len(channels) == 1 and isinstance(channels[0], ChannelList):
            self.channels = channels[0].channels

    def __iter__(self) -> Iterator[Channel]:
        for ch in self.channels:
            yield ch

    def __len__(self):
        return len(self.channels)

    def __getitem__(self, index) -> Channel:
        assert isinstance(index, int), "Can only index with intergers."
        return self.channels[index]

    async def get(self):
        if len(self.channels) == 0:
            return END
        values = []
        for ch in self.channels:
            value = await ch.get()
            if value is END:
                return END
            else:
                values.append(value)
        if len(values) == 1:
            return values[0]
        else:
            return tuple(values)

    def get_nowait(self):
        if len(self.channels) == 0:
            return END
        values = []
        for ch in self.channels:
            value = ch.get_nowait()
            if value is END:
                return END
            else:
                values.append(value)
        if len(values) == 1:
            return values[0]
        else:
            return tuple(values)

    async def put(self, data):
        for ch in self.channels:
            await ch.put(data)

    def put_nowait(self, data):
        for ch in self.channels:
            ch.put(data)

    def empty(self):
        return not self.channels or all(ch.empty() for ch in self.channels)


Var = ValueChannel
END = End()
