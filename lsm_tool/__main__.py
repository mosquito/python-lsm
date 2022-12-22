import sys
import time
from argparse import ArgumentParser
from pathlib import Path

from lsm import LSM, SAFETY_OFF


def main():
    parser = ArgumentParser(
        description=(
            "This tool is useful for various manipulations on LSM databases. "
            "It can apply one of page compression algorithms "
            "(--compress=lz4, --compress=zstd) or copies the database without "
            "compression (--compress=none). You can also merge one or more "
            "databases into one file if the destination file exists, or "
            "simply copy the database element by element. You can also use "
            "it to check the integrity of a database."
        )
    )
    parser.add_argument(
        "source", type=Path, help="Source LSM database file path"
    )
    parser.add_argument(
        "dest", type=Path, help="Destination LSM database file path"
    )
    parser.add_argument(
        "-c", "--compress", choices=["none", "zstd", "lz4"],
        help="Page compression algorithm"
    )
    parser.add_argument(
        "-L", "--compress-level", choices=list(range(1, 10)), type=int,
        default=6, help="Page compression level"
    )
    parser.add_argument(
        "-R", "--replace", action="store_true",
        help="Replace existent database file, "
             "otherwise merge keys and values from the source"
    )
    parser.add_argument(
        "--dest-page-size", choices=list(1024 << i for i in range(7)),
        help="Destination file page_size"
    )

    arguments = parser.parse_args()

    if arguments.replace and arguments.dest.exists():
        sys.stderr.write(
            f"Removing {arguments.dest} because replace flag passed\n"
        )
        arguments.dest.unlink()

    dest = LSM(
        path=arguments.dest,
        compress=arguments.compress,
        compress_level=arguments.compress_level,
        multiple_processes=False,
        automerge=8,
        use_log=False,
        safety=SAFETY_OFF
    )

    prompt = f"\rCopying {arguments.source} -> {arguments.dest}: "

    def log_progress(idx):
        sys.stderr.write(prompt)
        sys.stderr.write(f"{idx:>10}")
        sys.stderr.flush()

    with LSM(arguments.source, readonly=True) as src, dest:
        idx = 0
        log_progress(idx)
        started_at = time.monotonic()

        for idx, (key, value) in enumerate(src.items()):
            dest[key] = value
            if idx % 1024 == 0:
                log_progress(idx)

        log_progress(idx)
        finished_at = time.monotonic()

        sys.stdout.write("\n")
        sys.stdout.write(
            f"Copied {idx} items in {finished_at - started_at} seconds\n"
        )


if __name__ == '__main__':
    main()
