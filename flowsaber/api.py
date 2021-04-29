"""
Expose all variables in core sub-package.
"""
# noinspection PyUnresolvedReferences
import flowsaber
# noinspection PyUnresolvedReferences
from flowsaber.core.channel import *
# noinspection PyUnresolvedReferences
from flowsaber.core.engine.flow_runner import *
# noinspection PyUnresolvedReferences
from flowsaber.core.engine.task_runner import *
# noinspection PyUnresolvedReferences
from flowsaber.core.flow import *
# noinspection PyUnresolvedReferences
from flowsaber.core.operators import *
# noinspection PyUnresolvedReferences
from flowsaber.core.task import *
# noinspection PyUnresolvedReferences
from flowsaber.core.task import *
# noinspection PyUnresolvedReferences
from flowsaber.core.utility.cache import *
# noinspection PyUnresolvedReferences
from flowsaber.core.utility.executor import *
# noinspection PyUnresolvedReferences
from flowsaber.core.utility.state import *
# noinspection PyUnresolvedReferences
from flowsaber.core.utility.target import *
# noinspection PyUnresolvedReferences
from flowsaber.core.utils import *

run = class_deco(RunTask, 'run')
command = class_deco(CommandTask, 'command')
task = class_deco(Task, 'run')
shell_task = class_deco(ShellTask, 'run')
flow = class_deco(Flow, 'run')


def shell(command_fn: Callable = None, **kwargs) -> Union[Callable, Flow]:
    if command_fn is None:
        return partial(shell, **kwargs)
    name = kwargs.pop('name', command_fn.__name__[0].upper() + command_fn.__name__[1:])
    command_task = command(command_fn, **kwargs, name=name + "CommandTask")
    shell_task = ShellTask(**kwargs, name=name + "ShellTask")

    @with_signature(inspect.signature(type(command_task).run))
    def run(self, *args, **kwargs) -> Output:
        module = self.context.get('module', None)
        conda = self.context.get('conda', None)
        image = self.context.get('image', None)

        # 1. compose command
        cmd_ch, cmd_output_ch = command_task(*args, **kwargs)
        # 2. execute the command
        env_ch = None
        if module or conda or image:
            # try to use the same env for all tasks
            env_ch = EnvTask(module, conda, image, **kwargs, name="ENV")()
        output_ch = shell_task(cmd_ch, cmd_output_ch, env=env_ch)

        return output_ch

    return type(name + "Flow", (Flow,), {"run": run})(**kwargs, name=name + "Flow")


def run(flow: Flow, server_address: str = None,
        agent_id: str = None, context: dict = None,
        id: str = None, name: str = None, labels: list = None, **kwargs):
    """

    Parameters
    ----------
    flow
    server_address
    agent_id
    context
    id
    name
    labels
    kwargs

    Returns
    -------

    """
    from flowsaber.client.client import Client, GraphqlError

    assert flow.state == Flow.INITIALIZED, "The flow must be initialized and not being executed yet."
    context = context or {}
    with flowsaber.context(context) as context:
        merged_context = context.to_dict()

    if not agent_id:
        flowsaber.context.logger.info("Run the flow in local.")
        runner = FlowRunner(flow, server_address=server_address)
        runner.run(context=merged_context)
    else:
        assert server_address, "Must specify a server to schedule flowrun in remote agent."
        flowrun_input = FlowRunInput(
            id=id or flowsaber.context.random_id,
            name=name or flowsaber.context.random_id,
            labels=labels or [],
            context=merged_context,
            state=Scheduled().to_dict(),
            flow_id=flow.config_dict['id'],
            agent_id=agent_id,
            **kwargs
        )

        async def upload_can_run():
            client = Client(server_address)

            flowsaber.context.logger.info(f"Upload the flow onto the server: {server_address}")
            try:
                # test if the flow already exists in server.
                flow_data = await client.query("get_flow", flow.config.id, "id")
            except GraphqlError:
                await client.mutation("create_flow", flow.serialize(), "id")
                flow_data = await client.query("get_flow", flow.config.id, "id")

            flowsaber.context.logger.info(f"Scheduling the flow run in server: {server_address}")
            flowrun_data = await client.mutation("update_flowrun", flowrun_input, 'id name labels')
            flowsaber.context.logger.info(f"Scheduled the flow run: {flowrun_data} in server: {server_address} ")

            await client.close()
            return flowrun_data

        return asyncio.run(upload_can_run())
