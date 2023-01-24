import struct

import pytest
from lsm import LSM, SEEK_LE, SEEK_GE, SEEK_EQ, SEEK_LEFAST

from tests import comp_algo


@pytest.fixture(params=comp_algo)
def db(request, tmp_path):
    with LSM(
        tmp_path / ("db.lsm" + request.param),
        compress=request.param,
        binary=False
    ) as db:
        yield db


@pytest.fixture(params=comp_algo)
def db_binary(request, tmp_path):
    with LSM(
        tmp_path / ("db.lsm" + request.param),
        compress=request.param,
        binary=True
    ) as db:
        yield db


@pytest.mark.parametrize("n", range(1, 10))
def test_ranges(n, subtests, db_binary: LSM):
    def make_key(*args):
        result = struct.pack("!" + "b" * len(args), *args)
        return result

    for i in range(n):
        for j in range(n):
            key = make_key(i, j, 0)
            db_binary[key] = b"\x00"

    with subtests.test("one key"):
        s = list(db_binary[make_key(0):make_key(1)])
        assert len(s) == n, s


def test_insert_select(subtests, db):
    with subtests.test("one key"):
        db["foo"] = "bar"
        assert db['foo'] == 'bar'
        assert 'foo' in db
        assert len(db) == 1
        assert list(db) == ['foo']
        assert list(db.keys()) == ['foo']
        assert list(db.values()) == ['bar']
        assert list(db.items()) == [('foo', 'bar')]

        del db['foo']

        assert 'foo' not in db
        assert len(db) == 0
        assert list(db) == []
        assert list(db.keys()) == []
        assert list(db.values()) == []
        assert list(db.items()) == []

    with subtests.test("100 keys"):
        for i in range(100):
            db['k{}'.format(i)] = str(i)

        assert len(db) == 100
        assert set(db) == set('k{}'.format(i) for i in range(100))
        assert set(db.keys()) == set('k{}'.format(i) for i in range(100))
        assert set(db.values()) == set(str(i) for i in range(100))
        assert set(db.items()) == set(
            ('k{}'.format(i), str(i)) for i in range(100)
        )

    with subtests.test("slice select ['k90':'k99']"):
        assert list(db['k90':'k99']) == list(
            ('k{}'.format(i), str(i)) for i in range(90, 100)
        )

    with subtests.test("slice select ['k90':'k99':-1]"):
        assert list(db['k90':'k99':-1]) == list(
            ('k{}'.format(i), str(i)) for i in range(99, 89, -1)
        )

    with subtests.test("select ['k90xx', SEEK_LE]"):
        assert db['k90xx', SEEK_LE] == '90'

    with subtests.test("select ['k90xx', SEEK_LEFAST]"):
        assert db['k90xx', SEEK_LEFAST]

    with subtests.test("select ['k90xx', SEEK_GE]"):
        assert db['k90xx', SEEK_GE] == '91'

    with subtests.test("select ['k90xx', SEEK_GE]"):
        with pytest.raises(KeyError):
            _ = db['k90xx', SEEK_EQ]

    with subtests.test("delete range ['k':'z']"):
        del db['k':'l']
        assert len(db) == 0

    with subtests.test("update"):
        db.update({"k{}".format(i): str(i) for i in range(100)})
        assert len(db) == 100
        assert db['k19'] == '19'


@pytest.mark.parametrize("comp", comp_algo)
def test_info(comp, tmp_path):
    with LSM(tmp_path / ("test.lsm." + comp), compress=comp,
             binary=False) as db:
        for i in map(str, range(10000)):
            db[i] = i

        info = db.info()
        assert 'checkpoint_size_result' in info
        assert 'nread' in info
        assert 'nwrite' in info
        assert 'tree_size' in info

    with LSM(tmp_path / ("test.lsm." + comp), binary=False,
             compress=comp, readonly=True) as db:
        info = db.info()
        assert 'checkpoint_size_result' not in info
        assert 'nread' in info
        assert 'nwrite' not in info
        assert 'tree_size' not in info


@pytest.mark.parametrize("comp", comp_algo)
def test_len(comp, tmp_path):
    with LSM(tmp_path / ("test.lsm." + comp), compress=comp,
             binary=False) as db:
        for i in map(str, range(10000)):
            db[i] = i

        assert len(db) == 10000
        assert len(db.keys()) == 10000
        assert len(db.values()) == 10000
        assert len(db.items()) == 10000
