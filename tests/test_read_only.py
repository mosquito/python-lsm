from contextlib import contextmanager
from pathlib import Path

import pytest
from lsm import LSM

from tests import comp_algo


@pytest.fixture(params=comp_algo, ids=comp_algo)
def db(request, tmp_path: Path):
    db_path = tmp_path / ("readonly.lsm" + request.param)
    with LSM(db_path, binary=False, multiple_processes=False) as db:
        db.update({"k{}".format(i): str(i) for i in range(100000)})

    with LSM(
        db_path, readonly=True,
        binary=False, multiple_processes=False
    ) as db:
        yield db


@contextmanager
def ensure_readonly():
    with pytest.raises(PermissionError) as e:
        yield

    assert e.value.args[0] == "Read only"


def test_readonly_update(db: LSM):
    with ensure_readonly():
        db.update({"foo": "bar"})


def test_readonly_setitem(db: LSM):
    with ensure_readonly():
        db["foo"] = "bar"


def test_readonly_insert(db: LSM):
    with ensure_readonly():
        db.insert("foo", "bar")


def test_readonly_flush(db: LSM):
    with ensure_readonly():
        db.flush()


def test_readonly_work(db: LSM):
    with ensure_readonly():
        db.work()


def test_readonly_checkpoint(db: LSM):
    with ensure_readonly():
        db.checkpoint()


def test_readonly_delete(db: LSM):
    with ensure_readonly():
        db.delete("foo")


def test_readonly_delitem(db: LSM):
    with ensure_readonly():
        del db["foo"]


def test_readonly_delete_range(db: LSM):
    with ensure_readonly():
        db.delete_range("foo", "bar")


def test_readonly_delitem_slice(db: LSM):
    with ensure_readonly():
        del db["foo":"bar"]


def test_readonly_begin(db: LSM):
    with ensure_readonly():
        db.begin()


def test_readonly_commit(db: LSM):
    with ensure_readonly():
        db.commit()


def test_readonly_rollback(db: LSM):
    with ensure_readonly():
        db.rollback()


def test_readonly_tx(db: LSM):
    with ensure_readonly():
        with db.tx():
            raise RuntimeError("Impossible case")


def test_readonly_transaction(db: LSM):
    with ensure_readonly():
        with db.transaction():
            raise RuntimeError("Impossible case")
