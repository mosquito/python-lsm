import asyncio
from multiprocessing.pool import ThreadPool
from pathlib import Path
from time import sleep

import pytest

from lsm import LSM


# @pytest.fixture
# def pool():
#     return ThreadPool(4)
#
#
# def test_call(pool: ThreadPool):
#     pool.apply(print, ("foo",))
#     pool.apply(lambda: print('bar'))
#     sleep(0.1)
