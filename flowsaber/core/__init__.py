from flowsaber.core.utils.cache import *
from flowsaber.core.utils.env import *
from flowsaber.core.utils.target import *
from .base import *
from .executor import *
from .flow import *
from .scheduler import *
from .task import *
from ..utility.utils import SELF_SIG

task = class_deco(Task, 'run')
command_task = command = class_deco(CommandTask, 'run')
shell_task = class_deco(ShellTask, 'run')
flow = class_deco(Flow, 'run')


def shell(command_fn: Callable = None, **kwargs) -> Union[Callable, Flow]:
    """
    Wrap
        def fn(param1, param2)

    into a FLow instance:
        class _Flow(Flow):
            def run(self, param1, param2):
                compose_cmd = command(fn)
                cmd_ch, cmd_output_ch = compose_cmd(param1, param2)
                env = Env()
                return shell_task(cmd=cmd_ch, output=cmd_output_chd)
        _flow = _Flow()
    """
    if command_fn is None:
        return partial(shell, **kwargs)
    # copy signature of command_fn to Flow.run
    sig = inspect.signature(command_fn)
    sigs = list(sig.parameters.values())
    if not (sigs[0].name == 'self' and sigs[0].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD):
        sigs.insert(0, SELF_SIG)

    flow_cls_name = command.__name__[0].upper() + command.__name__[1:] + "_flow"

    @with_signature(inspect.Signature(sigs))
    def run(self, *args, **kwargs) -> TaskOutput:
        compose_cmd = command(command_fn)
        cmd_ch, cmd_output_ch = compose_cmd(*args, **kwargs)

        if context.module or context.conda or context.image:
            env = EnvTask(context.module, context.conda, context.image)()
            output_ch = shell_task(cmd_ch, cmd_output_ch, env=env)
        else:
            output_ch = shell_task(cmd_ch, cmd_output_ch)

        return output_ch

    return type(flow_cls_name, (Flow,), {"run": run})(**kwargs)
