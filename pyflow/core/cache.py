import shutil
from inspect import BoundArguments
from pathlib import Path

import cloudpickle
from dask.base import tokenize

from pyflow.core.target import File
from pyflow.utility.logtool import get_logger

logger = get_logger(__name__)


class Cache(object):
    NO_CACHE = object()

    def __init__(self, task_key: str, enable: bool = True):
        self.task_key = task_key
        self.enable = enable

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

    def __init__(self, serializer=cloudpickle, **kwargs):
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

    async def hash(self, input_args: BoundArguments, **kwargs):
        # we do not count for parameter names, we only care about orders
        if not self.enable:
            return ""
        hash_dict = {
            'run_args': tuple(input_args.arguments.values()),
            **kwargs,
        }
        hash_key = tokenize(hash_dict)
        return hash_key

    async def contains(self, input_key: str) -> bool:
        if not self.enable:
            return False

        def cleanup(msg=""):
            path = self.path / Path(input_key)
            if path.exists():
                shutil.rmtree(path)
            logger.debug(msg)
            return False

        # case 1, in memory cache
        if input_key in self.cache:
            logger.debug(f"Task {self.task_name} read cache succeed with key: {input_key} from memory.")
            return True
        # case 2, in disk cache
        value_path = self.path / Path(input_key) / Path(self.VALUE_FILE)
        try:
            with value_path.open('rb') as f:
                value = self.serializer.load(f)
        except Exception as e:
            return cleanup(f"Task {self.task_name} read cache failed with key: {input_key} from disk with error: {e}")
        # make sure each File's checksum didn't change
        values = [value] if not isinstance(value, (list, tuple, set)) else value
        for f in [v for v in values if isinstance(v, File)]:
            if not await f.check_hash():
                return cleanup(f"Task {self.task_name} read cache failed with key: {input_key}"
                               f" from disk because file content hash changed.")
        self.cache[input_key] = value
        logger.debug(f"Task {self.task_name} read cache succeed with key: {input_key} from disk.")
        return True

    def put(self, input_key: str, output):
        if not self.enable:
            return
        self.cache[input_key] = output

    def get(self, input_key: str):
        return self.cache.get(input_key, self.NO_CACHE)

    def persist(self):
        """
        This should be called before python program ends
        """
        for key, value in self.cache.items():
            p = self.path / Path(key)
            p.mkdir(parents=True, exist_ok=True)
            with (p / Path(self.VALUE_FILE)).open('wb') as f:
                self.serializer.dump(value, f)
