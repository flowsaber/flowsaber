import hashlib
from pathlib import Path
from typing import Union

from flowsaber.core.executor import get_executor
from flowsaber.utility.logtool import get_logger

logger = get_logger(__name__)


class Target(object):
    """Theoretically all item emitted by Channel should be wrapped by a Target
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)


class File(Target):
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        obj.path = Path(*args).expanduser().resolve()
        return obj

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self._hash_key = None

        if not self.path.is_file():
            logger.debug(f"File {self.path} does not exists.")

    def __getattr__(self, item):
        return getattr(self.path, item)

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f"File('{self}')"

    @property
    def hash(self) -> str:
        if not self._hash_key:
            raise ValueError("Should run File.calculate_hash before accessing hash_code.")
        return self._hash_key

    @hash.setter
    def hash(self, value: str):
        if self._hash_key:
            raise ValueError("Hash of file could only be settled one.")
        self._hash_key = value

    @property
    def initialized(self) -> bool:
        return self._hash_key is not None

    def calculate_hash(self) -> str:
        h = hashlib.md5()
        with self.open('rb') as f:
            # Read and update hash_code string _output in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                h.update(byte_block)
        return h.hexdigest()

    async def check_hash(self) -> bool:
        assert self.initialized
        new_hash = await get_executor().run(self.calculate_hash)
        return new_hash == self.hash

    def initialize_hash(self):
        if self.initialized:
            return
        new_hash = self.calculate_hash()
        self.hash = new_hash

    def __dask_tokenize__(self):
        return self.hash


class Stdout(File):
    """Ugly way, use file to store stdout.
    In the idealist implementation, stdout and _stdin can be piped/linked across machines over network.
    """

    def __str__(self):
        # Note: different to File, stdout should be string by default
        return self.path.read_text()


class Stdin(Target):
    def __init__(self, src: Union[str, File], **kwargs):
        assert isinstance(src, (str, File)), "Only string and File-like object can be converted to Stdin"
        super().__init__(**kwargs)
        self.src: Union[str, File] = src

    def __dask_tokenize__(self):
        if isinstance(self.src, str):
            return self.src
        else:
            return self.src.hash

    def __str__(self):
        if isinstance(self.src, str):
            return f"echo \"{self.src}\" | "
        else:
            return f"cat {self.src} | "


class End(Target):
    def __repr__(self):
        return "[END]"

    def __copy__(self):
        return self

    def __hash__(self):
        return hash("[END]")


class Skip(Target):
    def __repr__(self):
        return "[SKIP]"

    def __copy__(self):
        return self

    def __hash__(self):
        return hash("[SKIP]")


END = End()
SKIP = Skip()
