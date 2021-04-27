from concurrent.futures import ProcessPoolExecutor

from flowsaber.cli import Cli
from flowsaber.core.api import *


def test_flow():
    @task
    def add(num):
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
    # make sure mongodb is installed and started
    runner = FlowRunner(f, server_address='http://127.0.0.1:8123')
    run_context = {

    }
    with ProcessPoolExecutor(max_workers=1) as executor:
        # start the server if not started manually
        # fut = executor.submit(Cli().server, port=8123)
        time.sleep(3)

        # run the flow
        st = time.time()
        runner.run(context=run_context)
        print("cost ", time.time() - st)

    async def check_data():
        from flowsaber.server.database.db import get_db
        db = get_db("mongodb://127.0.0.1:27017")
        flowruns = await db.flowrun.find().to_list(100)
        return flowruns

    # check writed data in database
    flowruns = asyncio.run(check_data())
    print(flowruns)


if __name__ == "__main__":
    # f.graph.render('dag', view=True, format='pdf', cleanup=True)
    test_flow()
