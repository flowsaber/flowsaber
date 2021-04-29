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
