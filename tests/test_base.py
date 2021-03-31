import sys

sys.path.insert(0, '../')

from flowsaber import *


@task
def add(num):
    return num + 1


@flow
def myflow(num):
    return num | add | add | view | add | view


num_ch = Channel.values(0)
f = myflow(num_ch)
# f.graph.render('dag', view=True, format='pdf', cleanup=True)
asyncio.run(run(f))
