import pytest
from lsm import LSM


def test_db(subtests, tmp_path):
    with LSM(str(tmp_path / "test.lsm"), binary=False) as db:
        with subtests.test(msg="test KeyError"):
            with pytest.raises(KeyError):
                _ = db['foo']

        with subtests.test(msg="test mapping-like set"):
            db['foo'] = 'bar'

        with subtests.test(msg="test mapping-like get"):
            assert db['foo'] == 'bar'
