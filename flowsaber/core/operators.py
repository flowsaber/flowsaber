import asyncio
import builtins
from collections import defaultdict, abc
from typing import Callable, Any, Union

from flowsaber.core.channel import ConstantChannel, END, Consumer, LazyAsyncQueue, ChannelBase
from flowsaber.core.task import BaseTask
from flowsaber.core.utility.target import Data
from flowsaber.core.utils import extend_method, class_to_method

Predicate = Callable[[Any], bool]
POS_INF = float("inf")
NEG_INF = float("-inf")


class Operator(BaseTask):
    """Base class for all operators, subclass of BaseTask, all operators runs in the main loop in sequence
    and do not have runners and run states.
    """

    def initialize_context(self):
        super().initialize_context()
        self.config_dict['name'] = "Operator" + self.config_dict['name']


class Merge(Operator):
    """Merge channels into a channel with _output of tuple.
    Even if there is only one channel _input, always _output a tuple
    """
    pass


class Split(Operator):
    """Used when _output is tuple/list, use split to split each item of the tuple into a unique channel.
    """

    def __init__(self, num: int, **kwargs):
        assert num >= 2, "Number of outputs must be at least 2."
        super().__init__(num_out=num, **kwargs)


class GetItem(Operator):
    """Get item from the output of the input channel with specified key, like `obj[key]`. The input channel is excepted
    to emit objects with `__getitem__` method. For example, tuple, list, dict ....

    """

    def __init__(self, key: Any, default: Any = None, **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.default = default

    async def handle_input(self, data, *args, **kwargs):
        if data is not END:
            try:
                res = data[self.key]
            except (TypeError, KeyError, IndexError) as e:
                # if not exit, return default, like dict's get
                from copy import deepcopy
                res = deepcopy(self.default)

            await self.enqueue_res(res)


class Select(GetItem):
    """Alias for GetItem task.
    """
    pass


class Get(GetItem):
    """Alias for GetItem task
    """
    pass


class Mix(Operator):
    """Data emitted bu channels are mixed into a single channel.
    """

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        num_constant_ch = builtins.sum(isinstance(q.ch, ConstantChannel) for q in consumer.queues)
        if num_constant_ch > 0:
            raise ValueError("Can not mix with constant channel.")

        async def pump_queue(q: LazyAsyncQueue):
            async for data in q:
                await self.enqueue_res(data)

        done, pending = await asyncio.wait(
            [asyncio.ensure_future(pump_queue(q)) for q in consumer.queues],
            return_when=asyncio.FIRST_EXCEPTION
        )
        for task in done:
            if task.exception() is not None:
                raise task.exception()


class Concat(Operator):
    """Data in channels are concatenated in the order of _input channels.
    """

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        for q in consumer.queues:
            async for data in q:
                await self.enqueue_res(data)


class Collect(Operator):
    """Opposite to flatten, turns a channel into a tuple
    """

    async def handle_consumer(self, consumer: Consumer, **kwargs):
        values = []
        async for data in consumer:
            values.append(data)
        await self.enqueue_res(tuple(values))


class Branch(Operator):
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
            await self.enqueue_res(data, chosen_index)


class Sample(Operator):
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
            await self.enqueue_res(data)


"""Map Task"""


class Subscribe(Operator):
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
            await self.enqueue_res(data)
        if self.on_complete:
            self.on_complete()


class View(Subscribe):
    """Print each item emitted by the channel, Equals to call Subscribe(on_next=print)
    """

    def __init__(self, fmt="{x}", **kwargs):
        super().__init__(on_next=self.print, **kwargs)
        self.fmt = fmt

    def print(self, value):
        print(self.fmt.format(x=value, self=self))


class Map(Operator):
    """Map each item to another item return by the specified map function into a new channel.
    """

    def __init__(self, by: Callable, **kwargs):
        super().__init__(**kwargs)
        self.fn = by

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is not END:
            res = self.fn(data)
            await self.enqueue_res(res)


class Reduce(Operator):
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
                await self.enqueue_res(self.result)
            self.result = self.NOTSET


class Flatten(Operator):
    """Flatten the _output of channel.
    """

    def __init__(self, max_level: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.max_level = max_level or 999999999999

    async def handle_input(self, data: Data, *args, **kwargs):
        if data is END:
            return

        def traverse(items, level):
            try:
                if level >= self.max_level + 1:
                    raise TypeError
                if isinstance(items, abc.Sequence) and not isinstance(items, str):
                    for _item in items:
                        traverse(_item, level + 1)
                else:
                    raise TypeError
            except TypeError:
                flattened_items.append(items)

        flattened_items = []
        traverse(data, 1)

        for item in flattened_items:
            await self.enqueue_res(item)


class Group(Operator):
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
                await self.enqueue_res((key, tuple(group)))
                del self.groups[key]
        else:
            if self.keep_rest:
                for key, values in self.groups.items():
                    await self.enqueue_res((key, tuple(values)))
                self.groups = defaultdict(list)


class Filter(Operator):
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
                await self.enqueue_res(data)


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


class Take(Operator):
    """Only take the first `num` number of items
    """

    def __init__(self, num: int = 1, **kwargs):
        assert num >= 1
        super().__init__(**kwargs)
        self.num = num

    async def handle_input(self, data, *args, **kwargs):
        if data is not END and self.num > 0:
            await self.enqueue_res(data)
            self.num -= 1
            if self.num <= 0:
                return END


class First(Take):
    """Take the first item"""

    def __init__(self, **kwargs):
        super().__init__(num=1, **kwargs)


class Constant(First):
    def initialize_output(self):
        self.output = ConstantChannel()


class Last(Operator):
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
                await self.enqueue_res(self.prev)
            self.prev = self.NOTSET


class Until(Operator):
    """"Take items until meet a stop marker. the stop item will not be included"""

    def __init__(self, by: Union[Predicate, object], **kwargs):
        super().__init__(**kwargs)
        self.by = by
        self.stop = False

    async def handle_input(self, data, *args, **kwargs):
        if data is not END and not self.stop:
            self.stop = self.by(data) if callable(self.by) else data == self.by
            if not self.stop:
                await self.enqueue_res(data)


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


# though this more simple, but operators can not be detected by ide
# operators = set()
# operators_map = {}
for var in tuple(locals().values()):
    if isinstance(var, type) and issubclass(var, Operator) and var is not Operator:
        # operator = class_to_func(var)
        # operators_map[operator.__name__] = operator
        # operators.add(operator)
        extend_method(ChannelBase)(class_to_method(var))
# locals().update(operators_map)


merge = Merge()
flatten = Flatten()
mix = Mix()
concat = Concat()
view = View()
unique = Unique()
distinct = Distinct()
first = First()
constant = Constant()
collect = Collect()
last = Last()
count = Count()
min_ = Min()
sum_ = Sum()
max_ = Max()


def split(num: int):
    return Split(num=num)


def getitem(key: Any, default: Any = None):
    return GetItem(key=key, default=default)


def select(key: Any, default: Any = None):
    return Select(key=key, default=default)


def get(key: Any, default: Any = None):
    return Get(key=key, default=default)


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
