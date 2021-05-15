import operator
from typing import Sequence, Callable, Any, TYPE_CHECKING

import flowsaber
from flowsaber.core.engine.scheduler import Job, TaskManager

if TYPE_CHECKING:
    from flowsaber.core.task import Task
    from flowsaber.core.flow import Flow


class Solver(object):
    """Multiple knapsack problem solver.
    """

    def solve(self, items: Sequence):
        raise NotImplementedError


class GaSolver(Solver):
    """Use genetic algorithm supplied by pyeasyga package to solve the mkp problem.
    """

    def __init__(self, score: Callable):
        self.score: Callable = score

    def solve(self, items: Sequence) -> Sequence[bool]:
        from pyeasyga import pyeasyga
        if len(items) <= 0:
            return []
        elif len(items) == 1:
            return [self.score(items) > 0]
        # TODO sometimes this leads to error
        ga = pyeasyga.GeneticAlgorithm(list(items))
        ga.fitness_function = self.fitness
        ga.run()
        max_score, selected = ga.best_individual()
        flowsaber.context.logger.debug(f"score: {max_score} selected: {selected}")
        return selected

    def fitness(self, individual, items: Sequence):
        return self.score([item for selected, item in zip(individual, items) if selected])


class ResourceManager(TaskManager):
    def __init__(self, flow: "Flow", solver: Solver = None):
        self.flow: "Flow" = flow
        self.solver = solver or GaSolver(score=self.score)

    def score(self, jobs: Sequence[Job]):
        if not jobs:
            return 0
        from copy import deepcopy
        flow_limit = deepcopy(self.flow.config_dict.get("resource_limit", {}))
        flow_limit.setdefault('cpu', 100000)
        pre_cpu = flow_limit['cpu']
        for job in jobs:
            task: "Task" = job.data
            task_limit = task.config_dict.get("resource_limit", {})
            if not self.operate_resource(task_limit, task.config_dict, is_valid=lambda x: x >= 0):
                return -1
            if not self.operate_resource(flow_limit, task.config_dict, is_valid=lambda x: x >= 0):
                return -1
        # by default, cost as much cpu as possible
        cpu_cost = pre_cpu - flow_limit['cpu']
        assert cpu_cost >= 0
        return cpu_cost

    def select_jobs(self, jobs: Sequence[Job]):
        return self.solver.solve(jobs)

    def job_start(self, job: Job):
        task, flow = job.data, self.flow
        self.operate_resource(task.config_dict.get('resource_limit'), task.config_dict)
        self.operate_resource(flow.config_dict.get('resource_limit'), task.config_dict)

    def job_end(self, job: Job):
        task, flow = job.data, self.flow
        self.operate_resource(task.config_dict.get('resource_limit'), task.config_dict, operator.add)
        self.operate_resource(flow.config_dict.get('resource_limit'), task.config_dict, operator.add)

    @staticmethod
    def operate_resource(limit_dict: dict, cost_dict: dict,
                         binary_op: Callable[[Any, Any], Any] = operator.sub,
                         is_valid: Callable = None):
        for resource, limit in limit_dict.items():
            if resource in cost_dict:
                limit_dict[resource] = binary_op(limit_dict[resource], cost_dict[resource])
                if is_valid and not is_valid(limit_dict[resource]):
                    return False
        return True
