from inspect import BoundArguments

from typing import Any, Optional


class State(object):
    def __init__(self,
                 inputs: BoundArguments = None,
                 result: Any = None,
                 context: dict = None,
                 message: str = None):
        self.state_type: str = type(self).__name__.upper()
        self.inputs: Optional[BoundArguments] = inputs
        self.result: Optional[Any] = result
        self.context: dict = context or {}
        self.message: str = message or ""

    def __repr__(self):
        message = f":{self.message}" if self.message else ""
        return f"<{self.state_type}>{message}"

    @classmethod
    def copy(cls, state: 'State'):
        # TODO, use copy or construction?
        from copy import copy
        if not issubclass(cls, type(state)):
            raise ValueError(f"The copy source's class must be a supper class of {cls}")
        new = cls()
        new.__dict__ = copy(state.__dict__)
        return new


class Pending(State):
    pass


class Retrying(Pending):
    """This state comes from Pending, means the task is waiting for rerun due to retry's waiting time"""
    pass


class Running(State):
    pass


class Done(State):
    """Represent the end state of a task run, should not be directly used. Use Success/Failure instead."""
    pass


class Success(Done):
    pass


class Cached(Success):
    """The result of the input is cached."""
    pass


class Failure(Done):
    """Means some Exception has been raised."""
    pass


class Skip(Success):
    """This state means this output should be skipped and directly send to the output channel"""
    pass


class Drop(Failure):
    """This state means the output should be dropped and will not be passed to the output channel.
    Usually this is caused by settled skip on error option in task.config"""
    pass


class Cancelling(State):
    pass


class Cancelled(State):
    pass
