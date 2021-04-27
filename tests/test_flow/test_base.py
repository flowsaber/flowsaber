from flowsaber.core.api import *


def time(fn):
    import time
    st = time.time()
    fn()
    print(time.time() - st)


def test_flow():
    @task
    def add(num):
        import sys
        # print("This is meesage send by print to stdout in task")
        # print("This is meesage send by print to stderr in task", file=sys.stderr)
        a = 1
        for i in range(10000000):
            a += 1
        return num + 1

    @flow
    def myflow(num):
        return num | add | add | view | add | view

    num_ch = Channel.values(*list(range(10)))
    initial_context = {
        'logging': {'level': "DEBUG"},
        'task_config': {
            'executor_type': "dask"
        }
    }
    with flowsaber.context(initial_context):
        f = myflow(num_ch)
    from concurrent.futures import ProcessPoolExecutor

    runner = FlowRunner(f, server_address='http://127.0.0.1:8123')
    run_context = {

    }
    with ProcessPoolExecutor(max_workers=1) as executor:
        from flowsaber.cli import Cli
        fut = executor.submit(Cli().server, port=8123)
        import time
        time.sleep(3)
        st = time.time()
        runner.run(context=run_context)
        print("cost ", time.time() - st)


if __name__ == "__main__":
    # f.graph.render('dag', view=True, format='pdf', cleanup=True)
    test_flow()
