import asyncio
import hashlib
import logging
import sys
from pathlib import Path

import aiomisc
from aiomisc.service import MemoryTracer
from lsm import LSM


@aiomisc.threaded
def seq_reader(fname: Path):
    hasher = hashlib.md5()

    logging.info("Start reading %s", fname)
    with LSM(fname, binary=True, readonly=True) as db:
        for key, value in db.items():
            hasher.update(key)
            hasher.update(value)

    logging.info("DIGEST: %s", hasher.hexdigest())
    logging.info("Reading done for %s", fname)


async def main():
    await asyncio.gather(
        *[seq_reader(Path(sys.argv[1])) for _ in range(2)]
    )


aiomisc.run(
    main(),
    MemoryTracer(interval=2, ),
    pool_size=8,
    log_format="rich_tb",
    log_level="info",
)
