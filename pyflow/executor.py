class Executor(object):
    async def run(self, fn, *args):
        raise NotImplementedError


class Local(Executor):
    async def run(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


local = Local()


def get_executor():
    return local
