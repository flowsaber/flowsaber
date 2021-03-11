from .channel import Channel, QueueChannel, END, ChannelList
from .executor import get_executor
from .store import get_up_flow, get_flow_stack, get_top_flow
from .utils import INPUT, OUTPUT, DATA


class BaseTask(object):
    def __init__(self, name: str = None, retry: int = 1, **kwargs):
        self.input_args = None
        self.input_kwargs = None
        self.input: INPUT = None
        self.output: OUTPUT = None

        self.up_flow = None
        self.top_flow = None

        self.name = name or f"{get_flow_stack()}-{hash(self)}"
        self.retry = 1
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    def initialize_input(self, *args, **kwargs) -> ChannelList:
        self.input_args = args
        self.input_kwargs = kwargs
        self.input = ChannelList(list(args) + list(kwargs.values()), name=str(self), task=self)
        return self.input

    def initialize_output(self) -> OUTPUT:
        self.output = QueueChannel(name=str(self), task=self)
        return self.output

    def __call__(self, *args, **kwargs) -> OUTPUT:
        # deepcopy or copy
        from copy import copy, deepcopy
        task = deepcopy(self)
        task.initialize_input(*args, **kwargs)
        task.initialize_output()
        task.up_flow = get_up_flow()
        task.top_flow = get_top_flow()

        up_flow = get_up_flow()
        assert task not in up_flow.tasks
        up_flow.tasks.setdefault(task, {})

        return task.output

    async def execute(self):
        res = await self.handle_channel_input(self.input)
        await self.output.put(END)
        return res

    async def handle_channel_input(self, input_ch: ChannelList):
        # How to handle retry properly ?
        self.up_flow.tasks[self]['Exceptiion'] = []
        retry = self.retry
        async for data in input_ch:
            while True:
                try:
                    await self.handle_data_input(data)
                except Exception as e:
                    self.up_flow.tasks[self]['Exception'].append(e)
                    if retry <= 0:
                        raise e
                    else:
                        retry -= 1
                        continue
                break
        await self.handle_data_input(END)

    async def handle_data_input(self, input_data: DATA):
        return NotImplemented

    def __repr__(self):
        return str(self.name) + "-" + self.__class__.__name__ + str(hash(self))

    def __ror__(self, lch: Channel) -> Channel:
        return self(lch)


class Task(BaseTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cache = {}

    def run(self, *args, **kwargs):
        raise NotImplementedError

    async def handle_data_input(self, input_data: DATA):
        if input_data is END:
            return
        if input_data in self.cache:
            res = self.cache.get(input_data)
        else:
            # may put jobs into another queue, than schedule jobs in the queue
            args, kwargs = self.bind_input_params(input_data)
            res = await get_executor().run(self.run, *args, **kwargs)
        await self.output.put(res)

    def bind_input_params(self, input_data: DATA):
        if len(self.input) == 1:
            input_data = (input_data,)
        len_args = len(self.input_args)
        args = input_data[:len_args]
        kwargs = {}
        for i, k in enumerate(self.input_kwargs.keys()):
            kwargs[k] = input_data[len_args + i]
        return args, kwargs
