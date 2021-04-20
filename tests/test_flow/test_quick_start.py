import sys

import matplotlib.image as mpimg
import matplotlib.pyplot as plt

sys.path.insert(0, '../../')
import flowsaber
from flowsaber import *

EnvTask.DEFAULT_CONFIG = {'task_workdir': '/tmp/Env'}  # make the EnvTask cache at a global place


def test_quick_start():
    @task
    def add(self, num):  # self is optional
        return num + 1

    @task
    def multiply(num1, num2):
        return num1 * num2

    @shell
    def write(num):
        """echo {num} > 1.txt"""
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

    config.update({
        'cpu': 8,
        Task: {
            'executor': 'ray'
        }
    })

    # set input
    num_ch = Channel.values(1, 2, 3, 4, 5, 6, 7, 8)
    # resolve dependencies
    workflow = my_flow(num=num_ch)
    # now can generate dag
    workflow.graph.render('quick_start_dag', view=False, format='png', cleanup=True)
    # try run the flow
    asyncio.run(flowsaber.run(workflow))

    # visualize the flow
    img = mpimg.imread('quick_start_dag.png')
    imgplot = plt.imshow(img)
    plt.show(block=False)


if __name__ == "__main__":
    test_quick_start()
