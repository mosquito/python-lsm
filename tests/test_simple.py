import pytest
from lsm import LSM


def test_db_binary(subtests, tmp_path):
    with LSM(str(tmp_path / "test.lsm"), binary=True) as db:
        with subtests.test(msg="str to binary mode"):
            with pytest.raises(ValueError):
                _ = db['foo']

            with pytest.raises(ValueError):
                db['foo'] = 'bar'

        with subtests.test(msg="test KeyError"):
            with pytest.raises(KeyError):
                _ = db[b'foo']

            with pytest.raises(KeyError):
                del db[b'foo']

        with subtests.test(msg="test mapping-like set"):
            assert b'foo' not in db
            db[b'foo'] = b'bar'
            assert b'foo' in db

        with subtests.test(msg="test mapping-like get"):
            assert db[b'foo'] == b'bar'

        with subtests.test(msg="test KeyError"):
            del db[b'foo']
            with pytest.raises(KeyError):
                _ = db[b'foo']


def test_db_strings(subtests, tmp_path):
    with LSM(str(tmp_path / "test.lsm"), binary=False) as db:
        with subtests.test(msg="bytes to string mode"):
            with pytest.raises(ValueError):
                _ = db[b'foo']

            with pytest.raises(ValueError):
                db[b'foo'] = b'bar'

        with subtests.test(msg="test KeyError"):
            with pytest.raises(KeyError):
                _ = db['foo']

            with pytest.raises(KeyError):
                del db['foo']

        with subtests.test(msg="test mapping-like set"):
            assert 'foo' not in db
            db['foo'] = 'bar'
            assert 'foo' in db

        with subtests.test(msg="test mapping-like get"):
            assert db['foo'] == 'bar'

        with subtests.test(msg="test KeyError"):
            del db['foo']
            with pytest.raises(KeyError):
                _ = db['foo']
