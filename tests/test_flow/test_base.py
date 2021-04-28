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
    runner = FlowRunner(f)
    run_context = {

    }
    time.sleep(3)

    st = time.time()
    runner.run(context=run_context)
    print("cost ", time.time() - st)


def create_flow_and_schedule_running(flow: Flow):
    import flowsaber
    from flowsaber.client.client import Client
    c = Client("http://127.0.0.1:8123")
    flow_data = await c.mutation("create_flow", flow.serialize(), "id")
    await c.mutation(
        "update_flowrun",
        FlowRunInput(
            id=flowsaber.context.random_id,
            name="test",
            labels=[],
            context={},
            flow_id=flow_data['id'],
            agent_id="test",
            state=Scheduled().to_dict()
        ),
        'id'
    )


if __name__ == "__main__":
    test_flow()
