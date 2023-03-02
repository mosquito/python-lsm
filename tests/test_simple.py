import pytest
import lsm


def test_multiple_open(subtests, tmp_path):
    kwargs = {
        'autocheckpoint': 8 * 1024,  # 8 MB
        'autoflush': 8 * 1024,  # 8 MB
        'multiple_processes': False,
        'safety': lsm.SAFETY_OFF,  # do not fsync manually
        'use_log': False,
        'binary': False,
    }

    count = 2048

    for prefix in ("k", "z", "a", "f", "1"):
        with lsm.LSM(str(tmp_path / "test.lsm"), **kwargs) as db:
            for i in range(count):
                db['{}{}'.format(prefix, i)] = str(i)

    with lsm.LSM(str(tmp_path / "test.lsm"), binary=False, readonly=True) as db:
        for prefix in ("k", "z", "a", "f", "1"):
            with subtests.test(msg="prefix {}".format(i)):
                for i in range(count):
                    assert db['{}{}'.format(prefix, i)] == str(i)

                for key, value in db.items():
                    assert key[1:] == value, (key, value)


def test_db_binary(subtests, tmp_path):
    with lsm.LSM(str(tmp_path / "test.lsm"), binary=True) as db:
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
    with lsm.LSM(str(tmp_path / "test.lsm"), binary=False) as db:
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


def test_db_cursors(subtests, tmp_path):
    with lsm.LSM(str(tmp_path / "test.lsm"), binary=False) as db:
        for i in range(10):
            db[f"key_{i}"] = str(i)

        with subtests.test(msg="basic"):
            with db.cursor() as cursor:
                cursor.first()

                key1, value1 = cursor.retrieve()
                key2, value2 = cursor.retrieve()

                assert cursor.compare(key1) >= 0
                assert cursor.compare(key1[1:]) < 0

                assert key1 == key2
                assert value1 == value2
