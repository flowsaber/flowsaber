"""
schema:

    task = Task()
    output_ch = task(ch1, ch2, ch3)
        -> will generate a new task instance, depends on (ch1, ch2, ch3) and return a ch

    for each task instance, will get a unique task_key or working directory
        task_key = classname-id(task)-hash(task.run.__code__ + task.run.__annotation__)
        workdir = task.config.workdir/task_key


    for each inputs of task.run will have a run_key equals to working directory
        run.workdir = run_key = task.workdir/hash(inputs, additional_info)


    all task with the same task_key will share the same lock pool and cache

    different runs will be scheduled in parallel but runs with the same run_key will share a lock to avoid conflict


    for task receive no input channels, the task's run will be run once
        output_ch = task()

    EnvTask is task with no input channels, it's output_ch is a ConstantChannel, it's implementation is like:
        @shell
        def envTask():
        def command():
            Shell("Create ENV commands")
            return self.env

    for any task with env-related options settled, will be converted functionally like:
        @shell(conda="test.yaml")
        def test(*args, **kwargs):
            pass

        test(ch1, ch2, ch3)  ->     env = EnvTask(conda='test.yaml'); test(ch1, ch2, ch3, env=env)

    by this way, the dependency on the environment is built

"""
