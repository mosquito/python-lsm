from pathlib import Path

from lsm_asyncio import AsyncLSM, AsyncLSMReadOnly


async def test_simple(subtests, tmp_path: Path):
    with subtests.test("fill db"):
        async with AsyncLSM(tmp_path / 'test.ldb', binary=False) as db:
            for i in range(10):
                await db.set('k{}'.format(i), str(i))

            assert await db.get('k0') == '0'

    with subtests.test("read db"):
        async with AsyncLSMReadOnly(tmp_path / 'test.ldb', binary=False) as db:
            for i in range(10):
                assert await db.get('k{}'.format(i)) == str(i)
