import asyncio
from collections import defaultdict, abc
from typing import Callable, Sequence, Any, Union

from pyflow.core.channel import Channel, ValueChannel, END, ChannelList
from pyflow.core.flow import Flow
from pyflow.core.task import BaseTask, DATA
from pyflow.utility.utils import extend_method, class_to_func, class_to_method

"""
Only Map and Reduce uses executor, other operators runs locally
"""

Predicate = Callable[[Any], bool]
POS_INF = 9999999999999999999999999999999999999999999999999999999999
NEG_INF = -999999999999999999999999999999999999999999999999999999999


################################################## Compose Task #######################################################


# overrite handle_channel_input
class Merge(BaseTask):
    """
    The input of merge operator may originally be a ChannelList, In this case merge channels within it.
    """

    async def handle_channel_input(self, input_ch: ChannelList):
        async for data in input_ch:
            await self.output.put(data)


class Split(BaseTask):
    """
    Multiple outputs of a task.run will be wrapped into a tuple, use split to split each output into a channel
    """

    def __init__(self, num: int, **kwargs):
        assert num >= 2, "Number of outputs must be at least 2."
        super().__init__(**kwargs)
        self.num_output_ch = num

    async def handle_data_input(self, input_data: DATA):
        if input_data is END:
            return
        if not isinstance(input_data, tuple) or len(input_data) != self.num_output_ch:
            raise RuntimeError(f"The input data {input_data} can't be splitted into {self.num_output_ch} channels")
        for out_ch, value in zip(self.output, input_data):
            await out_ch.put(value)


class Mix(BaseTask):
    async def handle_channel_input(self, input_ch: ChannelList):
        num_constant_ch = sum(isinstance(ch, ValueChannel) for ch in input_ch)
        if num_constant_ch > 0:
            raise ValueError("Can not mix with output channel.")

        async def pump_queue(ch: Channel):
            async for data in ch:
                await self.output.put(data)
            return END

        done, pending = await asyncio.wait(
            [asyncio.ensure_future(pump_queue(ch)) for ch in input_ch],
            return_when=asyncio.FIRST_EXCEPTION
        )
        for task in done:
            if task.exception() is not None:
                raise task.exception()


class Concat(BaseTask):
    async def handle_channel_input(self, input_ch: ChannelList):
        for ch in tuple(input_ch):
            async for data in ch:
                await self.output.put(data)


class Clone(BaseTask):
    def __init__(self, num: int = 2, **kwargs):
        assert num > 1, "The number of clones should be at least 2"
        super().__init__(**kwargs)
        self.num_output_ch = num

    async def handle_channel_input(self, input_ch: ChannelList):
        async for data in input_ch:
            await self.output.put(data)


class Branch(BaseTask):
    def __init__(self, by: Sequence[Callable], **kwargs):
        assert isinstance(by, (list, tuple)) and len(by) > 0, "Must specify a list with at least 1 branch function."
        super().__init__(**kwargs)
        self.fns = by
        self.num_output_ch = len(self.fns) + 1

    async def handle_channel_input(self, input_ch: ChannelList):
        async for data in input_ch:
            for i, fn in enumerate(self.fns):
                match = fn(data)
                if match:
                    await self.output[i].put(data)
                    break
                else:
                    await self.output[-1].put(data)


class Sample(BaseTask):
    def __init__(self, num: int = 1, **kwargs):
        assert num >= 1, "Sample size must be at lest 1"
        super().__init__(**kwargs)
        self.num = num
        self.reservoir = []

    async def handle_channel_input(self, input_ch: ChannelList):
        import random
        cur = 0
        async for data in input_ch:
            if cur < self.num:
                self.reservoir.append(data)
            else:
                i = random.randint(0, cur)
                if i < self.num:
                    self.reservoir[i] = data
            cur += 1
        for data in self.reservoir[0:cur]:
            await self.output.put(data)


########################################################## Map Task ###########################################3
# overrite handle_data_input

class Subscribe(BaseTask):
    def __init__(self, on_next: Callable = None, on_complete: Callable = None, **kwargs):
        super().__init__(**kwargs)
        self.on_next = on_next
        self.on_complete = on_complete

    async def handle_channel_input(self, input_ch: ChannelList):
        async for data in input_ch:
            if self.on_next:
                self.on_next(data)
            await self.output.put(data)
        if self.on_complete:
            self.on_complete()


class View(Subscribe):
    def __init__(self, **kwargs):
        super().__init__(on_next=lambda x: print(x), **kwargs)


class Map(BaseTask):
    # the only task uses executor
    def __init__(self, by: Callable = lambda x: x, **kwargs):
        super().__init__(**kwargs)
        self.fn = by

    async def handle_data_input(self, input_data: DATA):
        if input_data is END:
            return
        res = self.fn(input_data)
        await self.output.put(res)


class Reduce(BaseTask):
    NOTSET = object()

    def __init__(self, by: Callable[[Any, Any], Any], result=NOTSET, **kwargs):
        super().__init__(**kwargs)
        self.fn = by
        self.result = result
        self.prev_result = Reduce.NOTSET

    async def handle_data_input(self, input_data: DATA):
        # what if input_ch is None
        if input_data is END:
            if self.prev_result is not Reduce.NOTSET:
                await self.output.put(self.result)
        else:
            result = self.fn(self.result, input_data)
            self.prev_result, self.result = self.result, result


class Flatten(BaseTask):
    def __init__(self, max_level: int = None, **kwargs):
        super().__init__(**kwargs)
        self.max_level = max_level or 999999999999

    async def handle_data_input(self, input_data: DATA):
        if input_data is END:
            return
        # {}, or {1: 2, 2: 3} will flatten to be None

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

        traverse(input_data, 1)

        for item in flattened_items:
            await self.output.put(item)


#################################################### Group Task #######################################################


class Group(BaseTask):
    """
    return stream of Tuple(group_key, group_members)
    """

    def __init__(self, by: Callable = lambda x: x[0], **kwargs):
        super().__init__(**kwargs)
        self.key_fn = by
        self.groups = defaultdict(list)

    async def handle_data_input(self, input_data: DATA):
        if input_data is END:
            for k, values in self.groups.items():
                await self.output.put((k, values))
        else:
            key = self.key_fn(input_data)
            self.groups[key].append(input_data)


class Join(BaseTask):
    def __init__(self, key=lambda x: x[0], **kwargs):
        super().__init__(**kwargs)
        self.key_fn = key

    async def handle_data_input(self, input_data: DATA):
        raise NotImplementedError


##################################################### Filter Task #####################################################

# TODO if the filter function should be run in executor ?
class Filter(BaseTask):
    def __init__(self, by: Union[Predicate, object] = lambda x: True, **kwargs):
        super().__init__(**kwargs)
        self.fn = by

    async def handle_data_input(self, input_data: DATA):
        if input_data is END:
            return
        # two cases, filter by predicate or by output
        value = input_data
        if callable(self.fn):
            keep = self.fn(value)
        else:
            keep = value == self.fn
        if keep:
            await self.output.put(value)


class Unique(Filter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cache = set()
        self.fn = self.filter

    def filter(self, inputs_data):
        if inputs_data in self.cache:
            return False
        self.cache.add(inputs_data)
        return True


class Distinct(Filter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fn = self.filter
        self.prev_item = object()

    def filter(self, input_data):
        if input_data == self.prev_item:
            return False
        self.prev_item = input_data
        return True


##################################################### Collect Task ##################################################

class Take(BaseTask):
    def __init__(self, num: int, **kwargs):
        super().__init__(**kwargs)
        self.num = num

    async def handle_channel_input(self, input_ch: ChannelList):
        if self.num > 0:
            async for data in input_ch:
                await self.output.put(data)
                self.num -= 1
                if self.num <= 0:
                    break


class First(Take):
    def __init__(self, **kwargs):
        super().__init__(num=1, **kwargs)


class Last(BaseTask):
    NOTSET = object()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.prev = self.NOTSET

    async def handle_data_input(self, input_data: DATA):
        if input_data is END:
            if self.prev is not self.NOTSET:
                await self.output.put(self.prev)
        else:
            self.prev = input_data


class Until(BaseTask):
    def __init__(self, fn: Union[Predicate, object], **kwargs):
        super().__init__(**kwargs)
        self.fn = fn

    async def handle_channel_input(self, input_ch: ChannelList):
        async for data in input_ch:
            if callable(self.fn):
                stop = self.fn(data)
            else:
                stop = data == self.fn
            if stop:
                break
            await self.output.put(data)


######################################################## Math Task ####################################################
class Min(Reduce):
    def __init__(self, result: float = POS_INF, **kwargs):
        super().__init__(fn=min, result=result, **kwargs)


class Max(Reduce):
    def __init__(self, result: float = NEG_INF, **kwargs):
        super().__init__(fn=max, result=result, **kwargs)


class Sum(Reduce):
    def __init__(self, **kwargs):
        super().__init__(fn=lambda a, b: a + b, result=0, **kwargs)


class Count(Reduce):
    def __init__(self, **kwargs):
        super().__init__(fn=lambda a, b: a + 1, result=0, **kwargs)


######################################################## Utility Task #################################################


operators = set()
operators_map = {}
for var in tuple(locals().values()):
    if isinstance(var, type) and issubclass(var, BaseTask) and var is not BaseTask:
        operator = class_to_func(var)
        operators_map[operator.__name__] = operator
        operators.add(operator)
        extend_method(Channel)(class_to_method(var))
locals().update(operators_map)


@extend_method(Channel)
def __rshift__(self, others):
    if isinstance(others, Sequence):
        if not all(isinstance(task, (BaseTask, Flow)) for task in others):
            raise ValueError("Only Task/Flow object are supported")

        if len(others) <= 1:
            raise ValueError("Must contain at least two Task/Flow object")
        else:
            cloned_chs = self.clone(num=len(others))
            outputs = []
            for task, ch in zip(others, cloned_chs):
                outputs.append(task(ch))
            return ChannelList(outputs)

    else:
        if not isinstance(others, (BaseTask, Flow)) and others not in operators:
            raise ValueError("Only Task/Flow object are supported")
        return others(self)


@extend_method(Channel)
def __or__(self, others):
    """
    ch | [a, b, c, d] equals to mix(ch >> [a, b, c, d])

    """
    outputs = self >> others
    if isinstance(others, Sequence):
        return Mix()(*outputs)
    else:
        return outputs
