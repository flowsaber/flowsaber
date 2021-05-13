import hashlib
from pathlib import Path
from typing import Union

import flowsaber


class Target(object):
    """Target represents item emitted by Channel. Theoretically all item emitted by Channel should be wrapped by a Target
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)


class File(Target):
    """Wrapping of `pathlib.Path`, with features including integrity checking ...
    """

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        obj.path = Path(*args).expanduser().resolve()
        return obj

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self._hash_key = None

        if not self.path.is_file():
            flowsaber.context.logger.debug(f"File {self.path} does not exists.")

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
        new_hash = await flowsaber.context.executor.run(self.calculate_hash)
        return new_hash == self.hash

    def initialize_hash(self):
        if self.initialized:
            return
        new_hash = self.calculate_hash()
        self.hash = new_hash

    def __dask_tokenize__(self):
        return self.hash


class Folder(Target):
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        obj.path = Path(*args).expanduser().resolve()
        return obj

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)

        if not self.path.is_dir():
            flowsaber.context.logger.debug(f"Folder {self.path} does not exists.")

    def __getattr__(self, item):
        return getattr(self.path, item)

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f"Folder('{self}')"


class Stdout(File):
    """Ugly way, Use File to store stdout.
    In the idealist implementation, stdout and stdin can be piped/linked across machines over network.
    """


class Stdin(Target):
    """Represents stdin from a string or File.
    """

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
    """End signal of a Channel.
    """

    def __repr__(self):
        return "[END]"

    def __copy__(self):
        return self

    def __hash__(self):
        return hash("[END]")


END = End()
Data = Union[tuple, End]
