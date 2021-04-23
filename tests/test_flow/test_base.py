from flowsaber.core.api import *


def test_flow():
    @task
    def add(num):
        import sys
        print("This is meesage send by print to stdout in task")
        print("This is meesage send by print to stderr in task", file=sys.stderr)
        return num + 1

    @flow
    def myflow(num):
        return num | add | add | view | add | view

    num_ch = Channel.values(*list(range(100)))
    f = myflow(num_ch)

    runner = FlowRunner(f)
    runner.run()


if __name__ == "__main__":
    # f.graph.render('dag', view=True, format='pdf', cleanup=True)
    test_flow()
