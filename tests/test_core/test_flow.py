from flowsaber.core.api import *


def test_flow_serialization():
    @task
    def add(num):
        return num + 1

    @flow
    def myflow(num):
        return num | add | add | view | add | view

    num_ch = Channel.values(*list(range(100)))
    tes_flow = myflow(num_ch)

    flow_input = tes_flow.serialize()

    deserialized_flow = Flow.deserialize(flow_input.serialized_flow)

    assert deserialized_flow.initialized
    assert deserialized_flow == tes_flow
