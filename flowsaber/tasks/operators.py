import asyncio
import builtins
from collections import defaultdict, abc
from typing import Callable, Any, Union

from flowsaber.core.channel import ConstantChannel, END, Consumer, Queue, Channel
from flowsaber.core.task import BaseTask, Data
from flowsaber.utility.utils import extend_method, class_to_method

"""
Only Map and Reduce uses executor, other operators runs locally
"""

Predicate = Callable[[Any], bool]
POS_INF = 9999999999999999999999999999999999999999999999999999999999
NEG_INF = -999999999999999999999999999999999999999999999999999999999


class Merge(BaseTask):
    """Merge channels into a channel with _output of tuple.
    Even if there is only one channel _input, always _output a tuple
    """

    async def handle_input(self, data, *args, **kwargs):
        if data is not END:
            await self._output.put(data)


class Split(BaseTask):
    """Used when _output is tuple/list, use split to split each item of the tuple into a unique channel.
    """

    def __init__(self, num: int, **kwargs):
        assert num >= 2, "Number of outputs must be at least 2."
        super().__init__(num_out=num, **kwargs)

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:
            if not isinstance(data, (tuple, list)) or len(data) != self.num_out:
                raise RuntimeError(f"The _input _input {data} can't be split into {self.num_out} channels")
            for out_ch, value in zip(self._output, data):
                await out_ch.put(value)


class Select(BaseTask):
    """Similar to split, used for tuple/list _output, select will only _output one channel, with the selected index
    """

    def __init__(self, index: int, **kwargs):
        super().__init__(**kwargs)
        self.index = index

    async def handle_input(self, data, *args, **kwargs):
        if data is not END:
            try:
                await self._output.put(data[self.index])
            except TypeError as e:
                raise ValueError(f"The _output _input {data} can be be indexed with {self.index}. error is: {e}")


class Mix(BaseTask):
    """Data emitted bu channels are mixed into a single channel.
    """

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        num_constant_ch = builtins.sum(isinstance(q.ch, ConstantChannel) for q in consumer.queues)
        if num_constant_ch > 0:
            raise ValueError("Can not mix with constant channel.")

        async def pump_queue(q: Queue):
            async for data in q:
                await self._output.put(data)

        done, pending = await asyncio.wait(
            [asyncio.ensure_future(pump_queue(q)) for q in consumer.queues],
            return_when=asyncio.FIRST_EXCEPTION
        )
        for task in done:
            if task.exception() is not None:
                raise task.exception()


class Concat(BaseTask):
    """Data in channels are concatenated in the order of _input channels.
    """

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        for q in consumer.queues:
            async for data in q:
                await self._output.put(data)


class Collect(BaseTask):
    """Opposite to flatten, turns a channel into a tuple
    """

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        values = []
        async for data in consumer:
            values.append(data)
        await self._output.put(tuple(values))


class Branch(BaseTask):
    """Dispatch _input into specified number of channels base on the returned index of the predicate function.
    """

    def __init__(self, num: int, by: Callable, **kwargs):
        assert num > 1 and callable(by)
        super().__init__(num_out=num, **kwargs)
        self.fn = by

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        async for data in consumer:
            chosen_index = self.fn(data)
            if chosen_index >= self.num_out:
                raise RuntimeError(f"The returned index of {self.fn} >= number of _output channels")
            await self._output[chosen_index].put(data)


class Sample(BaseTask):
    """Randomly sample at most `num` number of _input emitted by the channel
    using reservoir algorithm(should visit all elements.).
    """

    def __init__(self, num: int = 1, **kwargs):
        assert num >= 1, "Sample size must be at lest 1"
        super().__init__(**kwargs)
        self.num = num
        self.reservoir = []

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        import random
        cur = 0
        async for data in consumer:
            if cur < self.num:
                self.reservoir.append(data)
            else:
                i = random.randint(0, cur)
                if i < self.num:
                    self.reservoir[i] = data
            cur += 1
        for data in self.reservoir[0:cur]:
            await self._output.put(data)


"""Map Task"""


class Subscribe(BaseTask):
    """specify on_next or on_complete function as callbacks of these two event.
    """

    def __init__(self, on_next: Callable = None, on_complete: Callable = None, **kwargs):
        super().__init__(**kwargs)
        self.on_next = on_next
        self.on_complete = on_complete

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        async for data in consumer:
            if self.on_next:
                self.on_next(data)
            await self._output.put(data)
        if self.on_complete:
            self.on_complete()


class View(Subscribe):
    """Print each item emitted by the channel, Equals to call Subscribe(on_next=print)
    """

    def __init__(self, **kwargs):
        super().__init__(on_next=print, **kwargs)


class Map(BaseTask):
    """Map each item to another item return by the specified map function into a new channel.
    """

    # TODO should we use executor?
    def __init__(self, by: Callable, **kwargs):
        super().__init__(**kwargs)
        self.fn = by

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:
            res = self.fn(data)
            await self._output.put(res)


class Reduce(BaseTask):
    """Similar to normal reduce. results = f(n, f(n - 1))
    """
    NOTSET = object()

    def __init__(self, by: Callable[[Any, Any], Any], result=NOTSET, **kwargs):
        super().__init__(**kwargs)
        self.fn = by
        self.result = result
        self.prev_result = self.NOTSET

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:
            result = self.fn(self.result, data)
            self.prev_result, self.result = self.result, result
        else:
            if self.prev_result is not self.NOTSET:
                await self._output.put(self.result)
            self.result = self.NOTSET


class Flatten(BaseTask):
    """Flatten the _output of channel.
    """

    def __init__(self, max_level: int = None, **kwargs):
        super().__init__(**kwargs)
        self.max_level = max_level or 999999999999

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:

            flattened_items = []

            def traverse(items, level):
                try:
                    if level > self.max_level + 1:
                        raise TypeError
                    if isinstance(items, abc.Sequence) and not isinstance(items, str):
                        for _item in items:
                            traverse(_item, level + 1)
                    else:
                        raise TypeError
                except TypeError:
                    flattened_items.append(items)

            traverse(data, 1)

            for item in flattened_items:
                await self._output.put(item)


class Group(BaseTask):
    """return a new channel with item of Tuple(key_fn(_input), grouped_data_tuple),
    the group size can be specified.
    """

    def __init__(self, by: Callable = lambda x: x[0], num: int = POS_INF, keep: bool = True, **kwargs):
        assert num > 1
        super().__init__(**kwargs)
        self.key_fn = by
        self.group_size = num
        self.keep_rest = keep
        self.groups = defaultdict(list)

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:
            key = self.key_fn(data)
            group = self.groups[key]
            group.append(data)
            if len(group) >= self.group_size:
                self._output.put((key, tuple(group)))
                del self.groups[key]
        else:
            if self.keep_rest:
                for k, values in self.groups.items():
                    await self._output.put((k, tuple(values)))
                self.groups = defaultdict(list)


class Filter(BaseTask):
    """Filter item by the predicate function or the comparing identity.
    """

    def __init__(self, by: Union[Predicate, object] = lambda x: x, **kwargs):
        super().__init__(**kwargs)
        self.by = by

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:
            # two cases, filter by predicate or by _output
            keep = self.by(data) if callable(self.by) else data == self.by
            if keep:
                await self._output.put(data)


class Unique(Filter):
    """Emit items at most once(no duplicate).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cache = set()
        self.by = self.filter

    def filter(self, data):
        if data in self.cache:
            return False
        self.cache.add(data)
        return True


class Distinct(Filter):
    """Remove continuously duplicated item.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.by = self.filter
        self.prev = object()

    def filter(self, data):
        if data == self.prev:
            return False
        self.prev = data
        return True


class Take(BaseTask):
    """Only take the first `num` number of items
    """

    def __init__(self, num: int = 1, **kwargs):
        assert num >= 1
        super().__init__(**kwargs)
        self.num = num

    async def handle_input(self, data, *args, **kwargs):
        if data is not END and self.num > 0:
            await self._output.put(data)
            self.num -= 1


class First(Take):
    """Take the first item"""

    def __init__(self, **kwargs):
        super().__init__(num=1, **kwargs)


class Last(BaseTask):
    """Take the last item"""
    NOTSET = object()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.prev = self.NOTSET

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:
            self.prev = data
        else:
            if self.prev is not self.NOTSET:
                await self._output.put(self.prev)
            self.prev = self.NOTSET


class Until(BaseTask):
    """"Take items until meet a stop marker. the stop item will not be included"""

    def __init__(self, by: Union[Predicate, object], **kwargs):
        super().__init__(**kwargs)
        self.by = by
        self.stop = False

    async def handle_input(self, data, *args, **kwargs):
        if data is not END and not self.stop:
            self.stop = self.by(data) if callable(self.by) else data == self.by
            if not self.stop:
                await self._output.put(data)


class Min(Reduce):
    def __init__(self, result: float = POS_INF, **kwargs):
        super().__init__(by=min, result=result, **kwargs)


class Max(Reduce):
    def __init__(self, result: float = NEG_INF, **kwargs):
        super().__init__(by=max, result=result, **kwargs)


class Sum(Reduce):
    def __init__(self, **kwargs):
        super().__init__(by=sum, result=0, **kwargs)


class Count(Reduce):
    def __init__(self, **kwargs):
        super().__init__(by=lambda a, b: a + 1, result=0, **kwargs)


# TODO though this more simple, but operators can not be detected by ide
# operators = set()
# operators_map = {}
for var in tuple(locals().values()):
    if isinstance(var, type) and issubclass(var, BaseTask) and var is not BaseTask:
        # operator = class_to_func(var)
        # operators_map[operator.__name__] = operator
        # operators.add(operator)
        extend_method(Channel)(class_to_method(var))
# locals().update(operators_map)


merge = Merge()
flatten = Flatten()
mix = Mix()
concat = Concat()
view = View()
unique = Unique()
distinct = Distinct()
first = First()
collect = Collect()
last = Last()
count = Count()
min_ = Min()
sum_ = Sum()
max_ = Max()


def split(num: int):
    return Split(num=num)


def select(index: int):
    return Select(index=index)


def branch(num: int, by: Callable):
    return Branch(num=num, by=by)


def map_(by: Callable):
    return Map(by=by)


def reduce_(by: Callable[[Any, Any], Any], result=Reduce.NOTSET):
    return Reduce(by=by, result=result)


def filter_(by: Union[Predicate, object] = lambda x: x):
    return Filter(by=by)


def group(by: Callable = lambda x: x[0], num: int = POS_INF, keep: bool = True):
    return Group(by=by, num=num, keep=keep)


def subscribe(on_next: Callable = None, on_complete: Callable = None):
    return Subscribe(on_next=on_next, on_complete=on_complete)


def sample(num: int = 1):
    return Sample(num=num)


def take(num: int = 1):
    return Take(num=num)


def until(by: Union[Predicate, object]):
    return Until(by=by)
