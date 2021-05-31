import pytest
from lsm import LSM, SEEK_LE, SEEK_GE, SEEK_EQ


@pytest.fixture(params=["none", "lz4", "zstd"])
def db(request, tmp_path):
    with LSM(
        tmp_path / ("db" + request.param),
        compress=request.param,
        binary=False
    ) as db:
        yield db


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

