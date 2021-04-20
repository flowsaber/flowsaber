# sys.path.insert(0, '../')

from flowsaber import *


@task
def add(num):
    return num + 1


@flow
def myflow(num):
    return num | add | add | view | add | view


num_ch = Channel.values(*list(range(100)))
f = myflow(num_ch)

if __name__ == "__main__":
    # f.graph.render('dag', view=True, format='pdf', cleanup=True)
    asyncio.run(run(f))
