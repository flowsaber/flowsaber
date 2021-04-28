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

    command_task = command(command_fn, **kwargs)
    shell_task = ShellTask(**kwargs)

    @with_signature(inspect.signature(command_task.run))
    def run(self, *args, **kwargs) -> Output:
        module = self.context.get('module', None)
        conda = self.context.get('conda', None)
        image = self.context.get('image', None)

        # 1. compose command
        cmd_ch, cmd_output_ch = command_task(*args, **kwargs)
        # 2. execute the command
        if module or conda or image:
            env_ch = EnvTask(module, conda, image)()
            output_ch = shell_task(cmd_ch, cmd_output_ch, env=env_ch)
        else:
            output_ch = shell_task(cmd_ch, cmd_output_ch)

        return output_ch

    flow_cls_name = command.__name__[0].upper() + command.__name__[1:] + "Flow"

    return type(flow_cls_name, (Flow,), {"run": run})(**kwargs)
