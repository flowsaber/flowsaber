from .channel import Channel, QueueChannel, END, ChannelList
from .executor import get_executor
from .store import get_up_flow, get_top_flow, get_flow_stack
from .utils import INPUT, OUTPUT, DATA


class FlowComponent(object):
    def __init__(self, name: str = "", retry: int = 1, **kwargs):
        self.input_args = None
        self.input_kwargs = None
        self.input: INPUT = None
        self.output: OUTPUT = None

        self.name = name
        self.name_with_id = True
        self.retry = retry
        self.up_flow: 'Flow' = None
        self.top_flow: 'Flow' = None
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)

    def __repr__(self):
        name = f"{self.name}|{type(self).__name__}({type(self).__bases__[0].__name__})"
        if self.name_with_id:
            name += f"[{hex(hash(self))}]"

        return name.lstrip('|')

    def __call__(self, *args, **kwargs) -> OUTPUT:
        raise NotImplementedError

    def copy_new(self):
        from copy import deepcopy
        # must deepcopy, otherwise all flow will share self.task_futures and other container attrs
        new = deepcopy(self)
        new.up_flow = get_up_flow()
        new.top_flow = get_top_flow() or self
        up_flow_name = str(new.up_flow.name) if new.up_flow else ""
        new.name = f"{up_flow_name}-{new}".lstrip('-')
        print(new.name)
        return new

    def initialize_input(self, *args, **kwargs) -> ChannelList:
        self.input_args = args
        self.input_kwargs = kwargs
        self.input = ChannelList(list(args) + list(kwargs.values()), name=str(self), task=self)
        return self.input


class BaseTask(FlowComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.num_output_ch = 1

    def initialize_output(self) -> OUTPUT:
        if self.num_output_ch == 1:
            self.output = QueueChannel(task=self)
        else:
            channels = tuple(QueueChannel(task=self) for i in range(self.num_output_ch))
            self.output = ChannelList(channels, task=self)
        return self.output

    def register_graph(self, *args, **kwargs):
        g = self.top_flow.graph
        kwargs = dict(
            style="filled",
            colorscheme="svg"
        )
        g.node(self.name, **kwargs)
        for ch in self.input:
            src = ch.task or ch
            shape = 'box' if isinstance(self, Task) else 'ellipse'
            if f"\t{src.name}" not in g.source:
                g.node(src.name, shape=shape, **kwargs)
            g.edge(src.name, self.name)

    def __call__(self, *args, **kwargs) -> OUTPUT:
        task = self.copy_new()
        task.initialize_input(*args, **kwargs)
        task.initialize_output()
        task.register_graph(*args, **kwargs)

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
