import shutil
from pathlib import Path
from typing import Any

from dask.base import tokenize

import flowsaber


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


NO_CACHE = object()


class Cache(object):
    """Cache is used for persisting results of met inputs. The cache should produce unique keys for unique inputs by
    implementing `hash` method. Hash of inputs can be further used to write/read related data.
    """

    def hash(self, **kwargs):
        raise NotImplementedError

    def put(self, input_key: str, output):
        raise NotImplementedError

    def get(self, input_key: str, default=NO_CACHE):
        raise NotImplementedError

    def remove(self, input_key: str):
        raise NotImplementedError

    def persist(self):
        raise NotImplementedError

    def persist_single(self, key):
        raise NotImplementedError


class LocalCache(Cache):
    """LocalCache treat hash keys as directories in disk and dump/load python objects in the corresponding directories.
    """
    VALUE_FILE = ".__Cache_value__"

    def __init__(self, serializer: Serializer = CloudPickleSerializer(), **kwargs):
        super().__init__(**kwargs)
        self.cache = {}
        self.serializer = serializer

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def hash(self, **kwargs) -> str:
        return tokenize(kwargs)

    def remove(self, key):
        path = Path(key)
        if path.exists():
            for f in path.glob('*'):
                if not f.name.startswith('._'):
                    if f.is_file():
                        f.unlink(missing_ok=True)
                    else:
                        shutil.rmtree(f, ignore_errors=True)

            return self.cache.pop(key, None)

    def get(self, key: str, default=NO_CACHE) -> object:
        # case 1, in memory cache
        if key in self.cache:
            flowsaber.context.logger.info(f"Read cache:{key} succeed from memory.")
        else:
            # case 2, in disk cache
            value_path = Path(key, self.VALUE_FILE)
            try:
                with value_path.open('rb') as f:
                    value = self.serializer.load(f)
            except Exception as e:
                self.remove(key)
                flowsaber.context.logger.debug(f"Read cache:{key} failed from disk with error: {e}")
                return default
            self.cache[key] = value
            flowsaber.context.logger.info(f"Read cache:{key} succeed from disk.")
        return self.cache[key]

    def put(self, key: str, data: Any):
        flowsaber.context.logger.debug(f"set cache:{key}")
        self.cache[key] = data

    def persist(self):
        """
        This should be called before python program ends
        """
        for key in tuple(self.cache.keys()):
            self.persist_single(key)

    def persist_single(self, key):
        flowsaber.context.logger.info(f"write cache {key}.")
        value = self.cache[key]
        f = Path(key, self.VALUE_FILE)
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open('wb') as wf:
            self.serializer.dump(value, wf)


def get_cache(cache: str = 'local', *args, **kwargs):
    cache_cls = {
        'local': LocalCache
    }
    if cache not in cache_cls:
        raise ValueError(f"cache type {cache} not supported, choose one of {cache_cls.keys()}")

    return cache_cls[cache](*args, **kwargs)
