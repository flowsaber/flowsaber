import shutil
from inspect import BoundArguments
from pathlib import Path

from dask.base import tokenize

from pyflow.utility.logtool import get_logger

logger = get_logger(__name__)


class Serializer(object):
    def load(self, file):
        raise NotImplementedError

    def dump(self, value, file):
        raise NotImplementedError


class CloudPickleSerializer(Serializer):
    def load(self, file):
        from cloudpickle import load
        return load(file)

    def dump(self, value, file):
        from cloudpickle import dump
        return dump(value, file)


class CacheInvalidError(Exception):
    pass


class Cache(object):
    NO_CACHE = object()

    def __init__(self, task_key: str, cache: bool = True):
        self.task_key = task_key
        self.enable = cache

    async def hash(self, input_args: BoundArguments, **kwargs):
        raise NotImplementedError

    async def contains(self, input_key: str) -> bool:
        raise NotImplementedError

    def put(self, input_key: str, output):
        raise NotImplementedError

    def get(self, input_key: str):
        raise NotImplementedError

    def persist(self):
        raise NotImplementedError


class LocalCache(Cache):
    VALUE_FILE = ".__Cache_value__"

    def __init__(self, serializer: Serializer = CloudPickleSerializer(), **kwargs):
        super().__init__(**kwargs)
        path = Path(self.task_key)
        assert path.is_dir()
        self.path = path.expanduser().resolve()
        self.task_name = str(path.name)
        self.cache = {}
        self.serializer = serializer

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def hash(self, input_args: BoundArguments, **kwargs):
        # we do not count for parameter names, we only care about orders
        if not self.enable:
            return ""
        hash_dict = {
            'run_args': tuple(input_args.arguments.values()),
            **kwargs,
        }
        hash_key = tokenize(hash_dict)
        return hash_key

    def remove(self, input_key):
        path = self.path / Path(input_key)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            return self.cache.pop(input_key, None)

    def get(self, input_key: str, default=Cache.NO_CACHE) -> object:
        if not self.enable:
            self.remove(input_key)
            logger.debug("Clean original cache if possible since cache set to False")
            return default
        # case 1, in memory cache
        if input_key in self.cache:
            logger.debug(f"Task {self.task_name} read cache succeed with key: {input_key} from memory.")
        else:
            # case 2, in disk cache
            value_path = self.path / Path(input_key) / Path(self.VALUE_FILE)
            try:
                with value_path.open('rb') as f:
                    value = self.serializer.load(f)
            except Exception as e:
                self.remove(input_key)
                logger.debug(f"Task {self.task_name} read cache failed with key: {input_key} from disk with error: {e}")
                return default
            self.cache[input_key] = value
        return self.cache[input_key]

    def put(self, input_key: str, output):
        if not self.enable:
            return
        self.cache[input_key] = output

    def persist(self):
        """
        This should be called before python program ends
        """
        for key, value in self.cache.items():
            p = self.path / Path(key)
            p.mkdir(parents=True, exist_ok=True)
            with (p / Path(self.VALUE_FILE)).open('wb') as f:
                self.serializer.dump(value, f)


def get_cache_cls(cache_type: str = 'local'):
    return {
        'local': LocalCache
    }[cache_type]
