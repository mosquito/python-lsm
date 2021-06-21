import gc
import tracemalloc
from time import monotonic, sleep
from typing import Any
from weakref import finalize

import lsm
import pytest


class DeallocCases:
    TIMEOUT = 1

    def test_tracemalloc(self, instance_maker):
        tracemalloc.start()
        snap_before = tracemalloc.take_snapshot()
        instance_maker()
        gc.collect()
        snap_after = tracemalloc.take_snapshot()

        filters = (
            tracemalloc.Filter(True, lsm.__file__),
        )

        snap_after = snap_after.filter_traces(filters)
        snap_before = snap_before.filter_traces(filters)

        assert not snap_after.compare_to(snap_before, 'lineno')
        tracemalloc.stop()
        gc.collect()

    def test_weakref(self, instance_maker):
        state = {}
        finalize(instance_maker(), lambda: state.update({"ok": True}))

        start = monotonic()

        while not state.get("ok"):
            if monotonic() - start > self.TIMEOUT:
                raise TimeoutError
            sleep(0.005)


class TestDeallocClass(DeallocCases):
    """
    Just for test base class
    """

    class Instance:
        pass

    @pytest.fixture
    def instance_maker(self) -> Any:
        def maker():
            return self.Instance()
        return maker


class TestLSMDealloc(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path) -> Any:
        def maker():
            return lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
        return maker


class TestLSMKeysDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            return db.keys()
        return maker


class TestLSMValuesDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            return db.values()
        return maker


class TestLSMItemsDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            return db.items()
        return maker


class TestLSMSliceDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            return db[::-1]
        return maker


class TestLSMCursorDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            return db.cursor()
        return maker


class TestLSMTransactionDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            return db.transaction()
        return maker


@pytest.fixture(params=[1, 10, 25, 50, 100, 256, 1024, 2048])
def filler(request):
    def fill_db(db: lsm.LSM):
        for i in range(request.param):
            db[str(i).encode()] = str(i).encode()
    return fill_db


class TestFilledLSMDealloc(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return db
        return maker


class TestFilledLSMKeysDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return db.keys()
        return maker


class TestFilledLSMValuesDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return db.values()
        return maker


class TestFilledLSMItemsDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return db.items()
        return maker


class TestFilledAndCheckLSMDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)

            for key, value in db.items():
                assert key == value, (key, value)

            return db.items()
        return maker
