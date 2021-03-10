import asyncio
from collections import UserDict
from collections.abc import Iterable
from typing import Union, Sequence, Iterator
from .store import get_top_flow, flow_stack


class ChannelData(object):
    def to_value(self):
        raise NotImplemented


class ChannelDictData(ChannelData, UserDict):
    def to_value(self):
        res = tuple(self.values())
        if len(res):
            if len(res) == 1:
                res = res[0]
            return res
        else:
            return None


class End(ChannelDictData):
    def to_value(self):
        return END

    def __repr__(self):
        return "END"


class Channel(object):

    def __init__(self, name="", task=None):
        self.flows = []
        self.task = task
        self.name = '-'.join([str(flow) for flow in flow_stack] + [name, str(hash(self))])
        self.top_flow = get_top_flow()

    def __repr__(self):
        return f"{self.task}-{self.__class__.__name__}-{hash(self)}"

    async def get(self):
        return self.get_nowait()

    def get_nowait(self):
        raise NotImplemented

    async def put(self, item):
        return self.put_nowait(item)

    def put_nowait(self, item):
        raise NotImplemented

    def __lshift__(self, other):
        """
        ch << 1 == ch.put_nowait(1)
        """
        self.put_nowait(other)
        return self

    def empty(self):
        raise NotImplemented

    def copy(self):
        from copy import deepcopy
        return deepcopy(self)

    @staticmethod
    def value(value):
        """
        Channel.value(1)
        """
        return ValueChannel(value)

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
    def __init__(self, value=None, key=None, **kwargs):
        super().__init__(**kwargs)
        check_list_of_channels(value)
        self.value = value
        self.key = key

    def get_nowait(self):
        return self.value

    def put_nowait(self, item):
        self.value = item
        return True

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


class WatchPathsChannel(Channel):
    pass


class ChannelList(Channel):
    def __init__(self, channels: Sequence[Channel], **kwargs):
        super().__init__(**kwargs)
        self.channels = channels


class ChannelDict(Channel):
    """
    Represents a collection of channel. Used as Task's input Channel
    """

    def __init__(self, args_dict=None, **kwargs):
        super().__init__(**kwargs)
        self.channels = {}
        self.link_channels(args_dict)

    def link_channels(self, args_dict: dict):
        for k, v in (args_dict or {}).items():
            if isinstance(v, Channel):
                self.channels[k] = v
            else:
                self.channels[k] = Channel.value(v)

    async def get(self) -> ChannelDictData:
        items = ChannelDictData()
        for k, ch in self.channels.items():
            item = await ch.get()
            if item is END:
                return END
            items[k] = item
        return items

    def get_nowait(self) -> ChannelDictData:
        items = ChannelDictData()
        for k, ch in self.channels.items():
            item = ch.get_nowait()
            if item is END:
                return END
            items[k] = item

        return items

    def put_nowait(self, items):
        raise NotImplementedError

    def empty(self):
        return not self.channels or all(ch.empty() for ch in self.channels)

    def __iter__(self) -> Iterator[Channel]:
        for ch in self.channels.values():
            yield ch

    def __len__(self):
        return len(self.channels)


def check_list_of_channels(*args, **kwargs):
    for value in list(args) + list(kwargs.values()):
        try:
            for item in value:
                if isinstance(item, Channel):
                    raise ValueError(f"The value {value} is a List/Tuple of Channel."
                                     f"You should not pass a list of Channels as input."
                                     f"A Possible fix is to use `*input` before pass to flow/task.")
        except TypeError:
            pass


Var = ValueChannel
END = End()
