from typing import (
    Any, Callable, Dict, Union, Optional, Tuple,
    KeysView, ValuesView, ItemsView, Mapping
)

SAFETY_OFF: int
SAFETY_NORMAL: int
SAFETY_FULL: int

STATE_INITIALIZED: int
STATE_OPENED: int
STATE_CLOSED: int

SEEK_EQ: int
SEEK_LE: int
SEEK_GE: int
SEEK_LEFAST: int



class Cursor:
    state: int
    seek_mode: int

    def close(self) -> None:...
    def first(self) -> None: ...
    def last(self) -> None: ...
    def seek(
        self, key: Union[bytes, str], seek_mode: int = SEEK_EQ
    ) -> bool: ...
    def retrieve(self) -> Optional[
        Tuple[Union[bytes, str], Union[bytes, str]]
    ]: ...
    def next(self) -> bool: ...
    def prev(self) -> bool: ...
    def compare(self, key: Union[bytes, str]) -> int: ...
    def __enter__(self) -> "Cursor": ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...


class LSM:
    path: str
    compressed: bool
    state: int
    page_size: int
    block_size: int
    safety: int
    autowork: int
    autocheckpoint: int
    mmap: bool
    use_log: bool
    automerge: int
    max_freelist: int
    multiple_processes: bool
    readonly: bool
    compress: str
    compress_level: int

    def __init__(
        self, path: str, *,
        autoflush: int = 1024,
        page_size: int = 4096,
        safety: int = SAFETY_NORMAL,
        block_size: int = 1024,
        automerge: int = 4,
        max_freelist: int = 24,
        autocheckpoint: int = 2048,
        autowork: bool = True,
        mmap: bool = True,
        use_log: bool = True,
        multiple_processes: bool = True,
        readonly: bool = False,
        binary: bool = True,
        logger: Callable[[str, int], Any] = None,
        compress: str = None,
        compress_level: int = None,
    ): ...

    def open(self) -> bool: ...
    def close(self) -> bool: ...
    def info(self) -> Dict[str, int]: ...
    def work(
        self, *, nmerge: int = 4, nkb: int = 1024, complete: bool = True
    ) -> int: ...
    def flush(self) -> bool: ...
    def __enter__(self) -> "LSM": ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
    def checkpoint(self) -> int: ...
    def cursor(self) -> Cursor: ...
    def insert(
        self, key: Union[bytes, str], value: Union[bytes, str]
    ) -> None: ...
    def delete(self, key: Union[bytes, str]) -> None: ...
    def delete_range(
        self, start: Union[bytes, str], end: Union[bytes, str]
    ) -> None: ...
    def begin(self) -> bool: ...
    def commit(self) -> bool: ...
    def rollback(self) -> bool: ...
    def __getitem__(
        self, item: Union[bytes, str, slice]
    ) -> Union[bytes, str, ItemsView[Union[bytes, str], Union[bytes, str]]]: ...
    def __delitem__(self, key: Union[bytes, str]): ...
    def __setitem__(self, key: Union[bytes, str], value: Union[bytes, str]): ...
    def keys(self) -> KeysView[Union[bytes, str]]: ...
    def values(self) -> ValuesView[Union[bytes, str]]: ...
    def items(self) -> ItemsView[Union[bytes, str], Union[bytes, str]]: ...
    def update(
        self, value: Mapping[Union[bytes, str], Union[bytes, str]]
    ) -> None: ...
