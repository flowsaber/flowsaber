import asyncio

from collections import defaultdict, abc
from typing import Callable, Sequence, Any, Union, Tuple

from .channel import Channel, ValueChannel, QueueChannel, ChannelDict, ChannelDictData, END
from .task import BaseTask, Task, ArgsTask, MapTask, OUTPUT
from .executor import get_executor
from .utils import extend_method, generate_operator, generate_method
from .flow import Flow

"""
Only Map and Reduce uses executor, other operators runs locally
"""

Predicate = Callable[[Any], bool]
POS_INF = 9999999999999999999999999999999999999999999999999999999999
NEG_INF = -999999999999999999999999999999999999999999999999999999999


################################################## Compose Task #######################################################
# overrite handle_channel_inputs
class Merge(ArgsTask):
    async def handle_channel_inputs(self, inputs: ChannelDict):
        while True:
            items = []
            for ch in inputs:
                item = await ch.get()
                if item is END:
                    await self.output.put(END)
                    return END
                items.append(item)
            await self.output.put(tuple(items))


class Mix(ArgsTask):
    async def handle_channel_inputs(self, inputs: ChannelDict):
        num_constant_ch = sum(isinstance(ch, ValueChannel) for ch in inputs)
        if num_constant_ch > 0:
            raise ValueError("Can not mix with constant channel.")

        async def pump_queue(ch: Channel):
            while True:
                item = await ch.get()
                if item is END:
                    return END
                await self.output.put(item)

        done, pending = await asyncio.wait(
            [asyncio.ensure_future(pump_queue(ch)) for ch in inputs],
            return_when=asyncio.FIRST_EXCEPTION
        )
        await self.output.put(END)
        for task in done:
            if task.exception() is not None:
                raise task.exception()
        return END


class Concat(ArgsTask):
    async def handle_channel_inputs(self, inputs: ChannelDict):
        for ch in tuple(inputs)[::-1]:
            while True:
                item = await ch.get()
                if item is END:
                    break
                await self.output.put(item)

        await self.output.put(END)
        return END


class Clone(ArgsTask):
    def __init__(self, num: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.num = num
        self.outputs: Tuple[QueueChannel] = tuple()

    def initialize_output(self) -> OUTPUT:
        self.output = self.outputs = tuple(QueueChannel(task=self) for i in range(self.num))
        if self.num == 1:
            self.output = self.outputs[0]
        return self.output

    async def handle_channel_inputs(self, inputs: ChannelDict):
        while True:
            value = (await inputs.get()).to_value()
            for out_ch in self.outputs:
                await out_ch.put(value)
            if value is END:
                return END


class Branch(ArgsTask):
    def __init__(self, by: Sequence[Callable], **kwargs):
        super().__init__(**kwargs)
        self.fns = by

    def initialize_output(self) -> Tuple[QueueChannel]:
        self.output = tuple(QueueChannel(task=self) for i in range(len(self.fns) + 1))
        return self.output

    async def handle_channel_inputs(self, inputs: ChannelDict):
        while True:
            value = (await inputs.get()).to_value()
            if value is END:
                for out_ch in self.output:
                    await out_ch.put(END)
                return END
            for i, fn in enumerate(self.fns):
                match = fn(value)
                if match:
                    await self.output[i].put(value)
                    break
            else:
                await self.output[-1].put(value)


class Sample(ArgsTask):
    def __init__(self, num: int, **kwargs):
        super().__init__(**kwargs)
        self.num = num
        self.reservoir = []

    async def handle_end(self):
        for item in self.reservoir:
            await self.output.put(item)
        await self.output.put(END)
        return END

    async def handle_channel_inputs(self, inputs: ChannelDict):
        import random
        for i in range(self.num):
            value = (await inputs.get()).to_value()
            if value is END:
                return await self.handle_end()
            else:
                self.reservoir.append(value)

        cur = self.num
        while True:
            value = (await inputs.get()).to_value()
            if value is END:
                return await self.handle_end()
            else:
                i = random.randint(0, cur)
                if i < self.num:
                    self.reservoir[i] = value
            cur += 1


########################################################## Map Task ###########################################3
# overrite handle_data_inputs

class Subscribe(MapTask):
    def __init__(self, on_next: Callable = None, on_complete: Callable = None, **kwargs):
        super().__init__(**kwargs)
        self.on_next = on_next
        self.on_complete = on_complete

    async def handle_data_inputs(self, inputs: ChannelDictData):
        if inputs is END:
            if self.on_complete:
                self.on_complete()
            return
        value = inputs.to_value()
        if self.on_next:
            self.on_next(value)
        await self.output.put(value)


class View(Subscribe):
    def __init__(self, **kwargs):
        super().__init__(on_next=lambda x: print(x), **kwargs)


class Map(MapTask):
    # the only task uses executor
    def __init__(self, by: Callable = lambda x: x, **kwargs):
        super().__init__(**kwargs)
        self.fn = by

    async def handle_data_inputs(self, inputs: ChannelDictData):
        if inputs is END:
            return
        res = await get_executor().run(self.fn, inputs.to_value())
        await self.output.put(res)


class Reduce(MapTask):
    NOTSET = object()

    def __init__(self, fn: Callable[[Any, Any], Any], result=NOTSET, **kwargs):
        super().__init__(**kwargs)
        self.fn = fn
        self.result = result
        self.prev_result = Reduce.NOTSET

    async def handle_data_inputs(self, inputs: ChannelDictData):
        # what if inputs is None
        if inputs is END:
            if self.prev_result is not Reduce.NOTSET:
                await self.output.put(self.result)
            return

        result = await get_executor().run(self.fn, self.result, inputs.to_value())
        self.prev_result, self.result = self.result, result


class Flatten(MapTask):
    def __init__(self, max_level: int = None, **kwargs):
        super().__init__(**kwargs)
        self.max_level = max_level or 999999999999

    async def handle_data_inputs(self, inputs: ChannelDictData):
        if inputs is END:
            return
        # {}, or {1: 2, 2: 3} will flatten to be None
        if len(inputs) != 1:
            await self.output.put(None)
            return

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

        traverse(inputs.to_value(), 1)

        for item in flattened_items:
            await self.output.put(item)


#################################################### Group Task #######################################################


class Group(MapTask):
    """
    return stream of Tuple(group_key, group_members)
    """

    def __init__(self, by: Callable = lambda x: x[0], **kwargs):
        super().__init__(**kwargs)
        self.key_fn = by
        self.groups = defaultdict(list)

    async def handle_data_inputs(self, inputs: ChannelDictData):
        if inputs is END:
            for k, values in self.groups.items():
                await self.output.put((k, values))
            return
        value = inputs.to_value()
        key = self.key_fn(value)
        self.groups[key].append(value)


class Join(MapTask):
    def __init__(self, key=lambda x: x[0], **kwargs):
        super().__init__(**kwargs)
        self.key_fn = key

    async def handle_data_inputs(self, inputs: ChannelDictData):
        pass


##################################################### Filter Task #####################################################

# TODO if the filter function should be run in executor ?
class Filter(MapTask):
    def __init__(self, by: Union[Predicate, object] = lambda x: True, **kwargs):
        super().__init__(**kwargs)
        self.fn = by

    async def handle_data_inputs(self, inputs: ChannelDictData):
        if inputs is END:
            return
        # two cases, filter by predicate or by value
        value = inputs.to_value()
        if callable(self.fn):
            keep = self.fn(value)
        else:
            keep = value == self.fn
        if keep:
            await self.output.put(value)
        else:
            pass


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

class Take(ArgsTask):
    def __init__(self, num: int, **kwargs):
        super().__init__(**kwargs)
        self.num = num

    async def handle_channel_inputs(self, inputs: ChannelDict):
        while True:
            value = (await inputs.get()).to_value()
            if value is END:
                await self.output.put(END)
                return END
            if self.num > 0:
                await self.output.put(value)
                self.num -= 1
            else:
                await self.output.put(END)
                return END


class First(Take):
    def __init__(self, **kwargs):
        super().__init__(num=1, **kwargs)


class Last(MapTask):
    NOTSET = object()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.prev = self.NOTSET

    async def handle_data_inputs(self, inputs: ChannelDictData):
        if inputs is END:
            if self.prev is not self.NOTSET:
                await self.output.put(self.prev)
        else:
            self.prev = inputs.to_value()


class Until(ArgsTask):
    def __init__(self, fn: Union[Predicate, object], **kwargs):
        super().__init__(**kwargs)
        self.fn = fn

    async def handle_channel_inputs(self, inputs: ChannelDict):
        while True:
            value = (await inputs.get()).to_value()
            if value is END:
                await self.output.put(END)
                return END
            if callable(self.fn):
                stop = self.fn(value)
            else:
                stop = value == self.fn
            if stop:
                await self.output.put(END)
                return END
            else:
                await self.output.put(value)


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

class CollectFile(Task):
    pass


class SplitFasta(Task):
    pass


class SplitFastq(Task):
    pass


class SplitText(Task):
    pass


operators = {}
for var in tuple(locals().values()):
    if isinstance(var, type) and issubclass(var, ArgsTask) and var not in (ArgsTask, MapTask):
        operator = generate_operator(var)
        operators[operator.__name__] = operator
        extend_method(Channel)(generate_method(var))
locals().update(operators)


@extend_method(Channel)
def __rshift__(self, others):
    if isinstance(others, Sequence):

        if not all(isinstance(task, (BaseTask, Flow)) for task in others):
            raise ValueError("Only Task/Flow object are supported")

        if len(others) == 0:
            raise ValueError("Must contain at least one Task/Flow object")
        else:
            cloned_chs = self.clone(len(others))
            if len(others) == 1:
                cloned_chs = [cloned_chs]
            outputs = []
            for task, ch in zip(others, cloned_chs):
                outputs.append(task(ch))
            return tuple(outputs)

    else:
        if not isinstance(others, (BaseTask, Flow)):
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
        return self >> others
