from flowsaber.api import *


def test_flow():
    @task
    def add(num):
        # print("This is meesage send by print to stdout in task")
        # print("This is meesage send by print to stderr in task", file=sys.stderr)
        a = 1
        for i in range(10000000):
            a += 1
        return num + 1

    @task
    def multiply(num1, num2):
        return num1 * num2

    @flow
    def myflow(num):
        num1 = num | add | add | view
        return multiply(num, num1)

    num_ch = Channel.values(*list(range(5)))
    f = myflow(num_ch)
    with flowsaber.context({
        'logging': {'level': "DEBUG"},
        'flow_config': {
            'resource_limit': {
                'fork': 7
            }
        },
        'task_config': {
            'executor_type': "dask"
        }
    }):
        run(f)


if __name__ == "__main__":
    test_flow()
