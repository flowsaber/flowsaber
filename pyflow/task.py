import inspect
from abc import ABC
from typing import Callable, Union, Tuple

from .channel import Channel, QueueChannel, ChannelDict, ChannelDictData, END, check_list_of_channels
from .executor import get_executor
from .store import get_up_flow, get_flow_stack
from .utils import INPUTS, OUTPUT, get_sig_param


def initialize_inputs(self, *args, **kwargs) -> dict:
    signature = inspect.signature(self.run)
    args_dict = dict(signature.bind(*args, **kwargs).arguments)

    # three type:
    # other POSITIONAL_OR_KEYWORD
    # *args VAR_POSITIONAL
    # **kwargs VAR_KEYWORD
    kwargs_vars = get_sig_param(signature, inspect.Parameter.VAR_KEYWORD)
    args_vars = get_sig_param(signature, inspect.Parameter.VAR_KEYWORD)
    if args_vars:
        raise ValueError("Run method does not support *args arguments")
    for var in kwargs_vars:
        args_dict.update(args_dict.pop(var.name, {}))

    return args_dict


def default_execute():
    raise ValueError("The executor should be initialized by user.")


class BaseTask(object):
    def __init__(self, **kwargs):
        self.inputs: ChannelDict = None
        self.output: OUTPUT = None
        self.execute: Callable = default_execute
        self.future = None
        self.name = None

    def copy(self):
        from copy import deepcopy
        new_task = deepcopy(self)
        new_task.name = str(get_flow_stack())
        return new_task

    def initialize_inputs(self, *args, **kwargs) -> ChannelDict:
        raise NotImplementedError

    def initialize_output(self) -> OUTPUT:
        self.output = QueueChannel(name=type(self).__name__, task=self)
        return self.output

    def __call__(self, *args, **kwargs) -> OUTPUT:
        check_list_of_channels(*args, **kwargs)
        task = self.copy()
        inputs = task.initialize_inputs(*args, **kwargs)
        output = task.initialize_output()

        async def execute():
            return await task.handle_channel_inputs(inputs)

        task.execute = execute
        up_flow = get_up_flow()
        assert task not in up_flow.tasks
        up_flow.tasks.setdefault(task, {})

        return output

    async def handle_channel_inputs(self, inputs: ChannelDict):
        raise NotImplementedError

    async def handle_data_inputs(self, inputs: ChannelDictData):
        return NotImplemented

    def __repr__(self):
        return str(self.name) + "-" + self.__class__.__name__ + str(hash(self))

    def __ror__(self, lch: Channel) -> Channel:
        return self(lch)


class Task(BaseTask):
    def run(self, *args, **kwargs):
        raise NotImplementedError

    def initialize_inputs(self, *args, **kwargs) -> ChannelDict:
        args_dict = initialize_inputs(self, *args, **kwargs)
        self.inputs = ChannelDict(args_dict)
        return self.inputs

    async def handle_channel_inputs(self, inputs: ChannelDict):
        while True:
            channel_inputs = await inputs.get()
            if channel_inputs is END:
                await self.handle_data_inputs(channel_inputs)
                await self.output.put(END)
                return END
            else:
                await self.handle_data_inputs(channel_inputs)

    async def handle_data_inputs(self, inputs: ChannelDictData):
        if inputs is END:
            return
        # may put jobs into another queue, than schedule jobs in the queue
        res = await get_executor().run(self.run, **inputs)
        await self.output.put(res)


class ArgsTask(BaseTask, ABC):
    def initialize_inputs(self, *args, **kwargs) -> ChannelDict:
        unknown_names = list("unknown_" + str(i) for i in range(len(args)))
        kwargs.update(dict(zip(unknown_names, args)))
        self.inputs = ChannelDict(kwargs)
        return self.inputs


class MapTask(ArgsTask):
    async def handle_channel_inputs(self, inputs: ChannelDict):
        while True:
            channel_inputs = await inputs.get()
            if channel_inputs is END:
                await self.handle_data_inputs(channel_inputs)
                await self.output.put(END)
                return END
            else:
                await self.handle_data_inputs(channel_inputs)
