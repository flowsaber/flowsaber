from flowsaber.api import *


def test_quick_start():
    @task
    def add(self, num):  # self is optional
        return num + 1

    @task
    def multiply(num1, num2):
        return num1 * num2

    @shell
    def write(num):
        """echo '{num}' > 1.txt"""
        return '*.txt'

    @task
    def read(f: File):
        return open(str(f)).readlines()

    @flow
    def sub_flow(num):
        return add(num) | map_(lambda x: x ** 2) | add

    @flow
    def my_flow(num):
        [sub_flow(num), sub_flow(num)] | multiply \
        | write | read | flatten \
        | map_(lambda x: int(x.strip())) \
        | view

    num_ch = Channel.values(1, 2, 3, 4, 5)
    # resolve dependencies
    with flowsaber.context({"task_config": {"executor_type": 'dask', 'cache_type': 'local'}}):
        workflow = my_flow(num=num_ch)

    runner = FlowRunner(workflow)
    runner.run()


if __name__ == "__main__":
    test_quick_start()
