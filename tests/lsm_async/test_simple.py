from pathlib import Path

from lsm_asyncio import AsyncLSM


async def test_simple(tmp_path: Path):
    async with AsyncLSM(tmp_path / 'test.ldb') as db:
        for i in range(10):
            await db.set('k{}'.format(i), str(i))

        assert await db['k0'] == '0'
