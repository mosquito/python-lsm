import asyncio
from abc import abstractmethod, ABC
from enum import IntEnum
from queue import Queue
from threading import Thread
from typing import Any, Set, Optional, Union, Tuple

import lsm


StrBytes = Union[bytes, str]


class TaskTypes(IntEnum):
    GET_ITEM = 1
    SET_ITEM = 2


class AsyncLSMBase(ABC):
    def __init__(self):
        self.__loop: Optional[asyncio.AbstractEventLoop] = None

    def __thread_inner(self, queue: Queue, func, path, **kwargs):
        with lsm.LSM(path, **kwargs) as db:
            while True:
                item = queue.get()

                if item is None:
                    return

                task: Any
                future: asyncio.Future

                task_type, args, future = item

                try:
                    self.__loop.call_soon_threadsafe(
                        future.set_result, func(db, task_type, args)
                    )
                except Exception as e:
                    self.__loop.call_soon_threadsafe(
                        future.set_exception, e
                    )
                finally:
                    queue.task_done()

    def _run_thread(self, queue, path, callback, **kwargs):
        if self.__loop is None:
            self.__loop = asyncio.get_event_loop()

        thread = Thread(
            target=self.__thread_inner,
            args=(
                queue,
                callback,
                path,
            ),
            kwargs=kwargs
        )

        self.__loop.call_soon_threadsafe(thread.start)
        return thread

    @abstractmethod
    async def open(self):
        pass


class AsyncLSM(AsyncLSMBase):
    def __init__(self, path: Any, **kwargs):
        super().__init__()
        self.__path = path
        self.__kwargs = kwargs
        self.__queue: Optional[Queue] = Queue()

    def __op_callback(self, db: lsm.LSM, task_type: TaskTypes,
                      args: Tuple[Any, ...]):
        if task_type == TaskTypes.GET_ITEM:
            key, = args
            return db[key]
        elif task_type == TaskTypes.SET_ITEM:
            key, value = args
            db[key] = value
            return

    async def open(self):
        self.__loop = asyncio.get_event_loop()

        self._run_thread(
            self.__queue,
            self.__path,
            self.__op_callback,
            **self.__kwargs
        )

        return True

    def close(self):
        if self.__queue is None:
            return

        self.__queue.put_nowait(None)
        self.__queue = None

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __execute(self, task_type: TaskTypes, *args) -> asyncio.Future:
        future = self.__loop.create_future()
        self.__queue.put((task_type, args, future))
        return future

    def get(self, item: StrBytes) -> asyncio.Future:
        return self.__execute(TaskTypes.GET_ITEM, item)

    def set(self, item: StrBytes, value: StrBytes) -> asyncio.Future:
        return self.__execute(TaskTypes.SET_ITEM, item, value)


class AsyncLSMReadOnly(AsyncLSMBase):
    def __init__(self, path: Any, readers: int = 4, **kwargs):
        super().__init__()
        self.__path = path
        self.__kwargs = kwargs
        self.__queue: Optional[Queue] = Queue()
        self.__readers: int = readers

        self.__kwargs.update({'readonly': True})

    def __op_callback(self, db: lsm.LSM, task_type: TaskTypes,
                      args: Tuple[Any, ...]):
        if task_type == TaskTypes.GET_ITEM:
            key, = args
            return db[key]

    async def open(self):
        self.__loop = asyncio.get_event_loop()

        read_kwargs = dict(self.__kwargs)
        read_kwargs.update({'readonly': True})

        for _ in range(self.__readers):
            self._run_thread(
                self.__queue,
                self.__path,
                self.__op_callback,
                **self.__kwargs
            )

        return True

    def close(self):
        if self.__queue is None:
            return

        for _ in range(self.__readers):
            self.__queue.put_nowait(None)

        self.__queue = None

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __execute(self, task_type: TaskTypes, *args) -> asyncio.Future:
        future = self.__loop.create_future()
        self.__queue.put((task_type, args, future))
        return future

    def get(self, item: StrBytes) -> asyncio.Future:
        return self.__execute(TaskTypes.GET_ITEM, item)
