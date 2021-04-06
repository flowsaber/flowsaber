import os


class FlowRunner(object):
    def __init__(self):
        pass

    def register_runner(self):
        pass

    def start(self):
        self.register_runner()

        max_workers = min(32, (os.cpu_count() or 1) + 4)


# a = {'data': {'flow_run': [{'id': '19bd49bb-c619-4c06-8e44-35e6ccb51c5f', 'version': 2, 'state': 'Scheduled',
#                             'serialized_state': {'type': 'Scheduled', '_result': {'type': 'NoResultType',
#                                                                                   '__version__': '0.14.14+49.gbe505a13e'},
#                                                  'context': {}, 'message': 'Flow run scheduled.',
#                                                  'start_time': '2021-04-03T10:40:42.355267+00:00',
#                                                  '__version__': '0.14.14+49.gbe505a13e', 'cached_inputs': {}},
#                             'parameters': {}, 'scheduled_start_time': '2021-04-03T10:40:42.355267+00:00',
#                             'run_config': {
#                                 'type': 'UniversalRun', 'labels': [], '__version__': '0.14.14'},
#                             'flow': {'environment': None, 'storage': {'path': None, 'type': 'Local', 'flows': {
#                                 'flow': '/Users/bakezq/.prefect/flows/flow/2021-04-03t08-53-47-134692-00-00'},
#                                                                       'secrets': [],
#                                                                       'directory': '/Users/bakezq/.prefect/flows',
#                                                                       '__version__': '0.14.14',
#                                                                       'stored_as_script': False}, 'version': 1,
#                                      'name': 'flow', 'id': '6ee148ab-ab9b-482e-bcdb-f0277c14ae06',
#                                      'core_version': '0.14.14'}, 'task_runs': []}]}}
#
# a = {'query': {'query': {
#     'flow_run(where: { id: { _in: ["19bd49bb-c619-4c06-8e44-35e6ccb51c5f"] }, _or: [{ state: { _eq: "Scheduled" } }, { state: { _eq: "Running" }, task_runs: { state_start_time: { _lte: "2021-04-03T10:40:40.761648+00:00" } } }] })': {
#         'id': True, 'version': True, 'state': True, 'serialized_state': True,
