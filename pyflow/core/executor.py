import inspect


class Executor(object):
    async def run(self, fn, *args):
        raise NotImplementedError


class Local(Executor):
    async def run(self, fn, *args, **kwargs):
        if inspect.iscoroutine(fn):
            await fn(*args, **kwargs)
        else:
            return fn(*args, **kwargs)


local = Local()


def get_executor():
    return local
