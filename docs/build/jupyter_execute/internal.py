# Internal


1. The second call will generate a new task instance depends on `(ch1, ch2, ch3)` and return a new `Channel`.

```
task = Task()
output_ch = task(ch1, ch2, ch3)
```


2. For each task instance, will get a unique `task_key` equals to it's working directory, since a task
object can be used multiple times, so there may be multiple tasks share the same `task_key`.
```
task.task_key = classname-id(task)-hash(task.run.__code__ + task.run.__annotation__ + other_info)
task.workdir = task.config.workdir/task.task_key
```

3. For each input of `task.run`, the run will have a `run_key` equals to it's working directory
```
run.workdir = run_key = task.workdir/hash(inputs, other_info)
```

3. All task objects with the same `task_key` will share the same `lock pool` and `cache`, different
runs will be scheduled in parallel but runs with the same `run_key` will compete for a sharing `run_key` lock
to avoid conflictions.


4. For task receives no input channels, the task's will only be ran once
```
output_ch = task()
```

5. `EnvTask` is task with no input channels, it's `output_channel` is a `ConstantChannel`, it's implementation is like:

```python
@shell
class EnvTask():
    def command():
        Shell("Create ENV commands")
        env = Env()
        return env
```

6. For any task with env-related options(conda, image..) settled, will be converted functionally like the
below codes to build correct dependency graph.

```python
task(ch1, ch2, ch3)  # converted to
env = EnvTask(conda='test.yaml')
task(ch1, ch2, ch3, env=env)
```