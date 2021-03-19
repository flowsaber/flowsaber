import hashlib
from pathlib import Path

from pyflow.core.executor import get_executor


class File(type(Path())):

    def __init__(self, *args, **kwargs):
        # all is done in __new__
        super().__init__()
        if not self.is_file():
            raise ValueError(f"File {self} does not exists.")
        self._hash_key = ''

    def __reduce__(self):
        return self.__class__, tuple(self._parts), self.__dict__

    def __str__(self):
        return super().__str__()

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
        return self._hash_key != ""

    def calculate_hash(self) -> str:
        h = hashlib.md5()
        with open(str(self), "rb") as f:
            # Read and update hash_code string output in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                h.update(byte_block)
        return h.hexdigest()

    async def check_hash(self) -> bool:
        assert self.initialized
        new_hash = await get_executor().run(self.calculate_hash)
        return new_hash == self.hash

    async def initialize_hash(self):
        if self.initialized:
            return
        new_hash = await get_executor().run(self.calculate_hash)
        self.hash = new_hash

    def __dask_tokenize__(self):
        return self.hash
