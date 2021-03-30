
# Task

There are four types of `Task` in total:

- `BaseTask` is base class for all tasks, and all operators are subclassed from it. Codes in
`BaseTask` will always be run in the main thread and event loop. Due to this limitation, you should not
put computation-cost logic in it, otherwise the whole eventloop will be blocked and hinder other task's execution.
- `Task` for running python functions. Execution of `run()` method will be scheduled and further executed by executor
configured.
- `ShellTask` for executing bash script. Support `conda` and `image` option compared to `Task`. When these options are
specified, additional bash command for handling enviroment creation, activation will be run before the
execution of user defined bash commnd.



### EnvTask




### Operators

All operators has three ways to used:
- Operator's class name, a task object needs to be created before using. Usage:
`Map()(channel)`
- Operator's function name, all operator has been wraped into a function accepts the same argument as the
original operator class. Usage: `merge(ch1, ch2, ch3)`
- `Channel`'s method name, all operator has been added as a method of `Channel`.
Usage: `ch1.map(fn)`


All predefined operators are:
- `Merge()`
- `Flatten()`
- `Mix()`
- ....


#### add custom operator
- Since operators are all `Tasks`, you can define your own operator task and even add
it as a `Channel` method.

