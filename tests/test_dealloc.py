from concurrent.futures import Future
from typing import Any
from weakref import finalize

import lsm
import pytest


# noinspection PyMethodMayBeStatic,SpellCheckingInspection
class DeallocCases:
    TIMEOUT = 1

    def test_weakref_finalize(self, instance_maker):
        future = Future()
        finalize(instance_maker(), lambda: future.set_result(True))
        future.result(timeout=1)


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


class TestFilledIterLSMDealloc(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return iter(db)
        return maker


class TestFilledIterLSMKeysDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return iter(db.keys())
        return maker


class TestFilledIterLSMValuesDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return iter(db.values())
        return maker


class TestFilledIterLSMItemsDeallocCtx(DeallocCases):
    @pytest.fixture(params=["none", "lz4", "zstd"])
    def instance_maker(self, request, tmp_path, filler) -> Any:
        def maker():
            db = lsm.LSM(
                tmp_path / ("db.lsm." + request.param),
                compress=request.param
            )
            db.open()
            filler(db)
            return iter(db.items())
        return maker


class TestFilledIterAndCheckLSMDeallocCtx(DeallocCases):
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

            return iter(db.items())
        return maker
