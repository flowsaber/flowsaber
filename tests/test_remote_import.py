import flowsaber
from flowsaber import *
from flowsaber.utility import load


def test_remote_import():
    url = "https://gist.githubusercontent.com/zhqu1148980644/2eafbe8d25883919ecf11a729f1fdb9a/raw/28c1be8056cb0ac46da227c0e537107b99952f0f"
    with load.remote_repo(['testtask'], url):
        from testtask import task1, task2

    @flow
    def myflow(name):
        return task2(task1(name), name)

    names = Channel.values("qwe", 'asd', 'zxcx', 'hhh')

    runner, workflow = FlowRunner(myflow).run(names)
    consumer = Consumer.from_channels(workflow._output)
    runner.execute()
    results = []
    for data in consumer:
        results.append(data)
    print("Results are: ")
    for res in results:
        print(res, type(res))


if __name__ == "__main__":
    test_remote_import()
