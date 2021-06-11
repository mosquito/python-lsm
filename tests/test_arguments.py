import pytest
from lsm import LSM


def test_argument_checks(subtests, tmp_path):
    with subtests.test("blank context manager"):
        with LSM(str(tmp_path / "test-filled.lsm"), binary=False) as db:
            for i in range(1000):
                db[str(i)] = str(i)

        with LSM(str(tmp_path / "test-filled.lsm")):
            pass

    with subtests.test("autoflush=1048577"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), autoflush=1048577)

    with subtests.test("autoflush=-1"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), autoflush=-1)

    with subtests.test("autocheckpoint=0"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), autocheckpoint=0)

    with subtests.test("autocheckpoint=-1"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), autocheckpoint=-1)

    with subtests.test("block_size=65"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), block_size=65)

    with subtests.test("safety=32"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), safety=32)

    with subtests.test("compress='zip'"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), compress='zip')

    with subtests.test("logger=1"), pytest.raises(ValueError):
        LSM(str(tmp_path / "test.lsm"), logger=1)


