import json
import logging
import os
import struct
import tempfile
from argparse import Action, ArgumentParser
from glob import glob
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from pathlib import Path
from random import shuffle
from threading import RLock, local
from typing import Union

from mimesis import Address, Business, Datetime, Person
from rich.logging import RichHandler
from rich.progress import (
    BarColumn, Progress, ProgressColumn, Task, TaskID, TextColumn,
    TimeRemainingColumn,
)
from rich.text import Text

import lsm


class SpeedColumn(ProgressColumn):
    """Renders human readable transfer speed."""

    def render(self, task: Task) -> Text:
        """Show data transfer speed."""
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        return Text(f"{int(speed)}it/s", style="progress.data.speed")


progress = Progress(
    TextColumn("[bold red]{task.description} [bold blue]{task.completed}/{task.total}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "[bold blue] ETA:",
    TimeRemainingColumn(),
    SpeedColumn(),
    auto_refresh=True,
)

progress.start()
log = logging.getLogger("rich")


class AppendConstAction(Action):
    def __init__(
        self, option_strings, dest, const=None, default=None,
        type=None, choices=None, required=False,
        help=None, metavar=None,
    ):
        assert const, "const= required"
        super().__init__(
            option_strings, dest, const=const, nargs=0,
            default=default, type=type, choices=choices,
            required=required, help=help, metavar=metavar,
        )

    def __call__(self, parser, namespace, value, option_string=None):
        if getattr(namespace, self.dest) is None:
            setattr(namespace, self.dest, list())

        lst = getattr(namespace, self.dest)
        lst.append(self.const)


parser = ArgumentParser()
parser.add_argument("-n", "--count", default=100000, type=int)
parser.add_argument("--pool-size", type=int, default=cpu_count())
parser.add_argument(
    "--clear",
    help="Keep existent database before writing",
    action="store_true",
)
parser.add_argument(
    "--path",
    default=os.path.join(tempfile.gettempdir(), "lsm-compressed"),
)

parser.add_argument("--run-sequentially", action="store_true")

group = parser.add_argument_group("cases")
group.add_argument(
    "--case-all",
    dest="cases",
    const="all",
    action=AppendConstAction,
)
group.add_argument(
    "--case-lz4",
    dest="cases",
    const="lz4",
    action=AppendConstAction,
)
group.add_argument(
    "--case-zstd",
    dest="cases",
    const="zstd",
    action=AppendConstAction,
)
group.add_argument(
    "--case-raw",
    dest="cases",
    const="raw",
    action=AppendConstAction,
)

group = parser.add_argument_group("benchmarks")
group.add_argument(
    "--bench-all",
    dest="benchmarks",
    const="all",
    action=AppendConstAction,
)
group.add_argument(
    "--bench-insert",
    dest="benchmarks",
    const="insert",
    action=AppendConstAction,
)
group.add_argument(
    "--bench-select-seq",
    dest="benchmarks",
    const="select-seq",
    action=AppendConstAction,
)
group.add_argument(
    "--bench-select-rnd",
    dest="benchmarks",
    const="select-rnd",
    action=AppendConstAction,
)

group.add_argument(
    "--bench-copy-seq",
    dest="benchmarks",
    const="copy-seq",
    action=AppendConstAction,
)


person_generator = Person("en")
address_generator = Address("en")
business_generator = Business("en")
datetime_generator = Datetime("en")


class Cases:
    @classmethod
    def lz4(cls, path):
        return [
            path + ".lsm.lz4",
            dict(multiple_processes=False, compress="lz4"),
        ]

    @classmethod
    def zstd(cls, path):
        return [
            path + ".lsm.zst",
            dict(multiple_processes=False, compress="zstd"),
        ]

    @classmethod
    def raw(cls, path):
        return [
            path + ".lsm",
            dict(multiple_processes=False),
        ]


def get_key(idx) -> Union[bytes, str]:
    return struct.pack("I", idx)


def get_value(idx) -> Union[bytes, str]:
    return json.dumps({
        "id": idx,
        "person": {
            "full_name": person_generator.full_name(),
            "email": person_generator.email(
                domains=[
                    "gmail.com",
                    "hotmail.com",
                    "yandex.ru",
                    "mail.ru",
                ],
            ),
            "phone": person_generator.telephone(mask="+7(9##)-###-####"),
            "avatar": person_generator.avatar(),
            "language": person_generator.language(),
        },
        "gps": {
            "lat": address_generator.latitude(),
            "lon": address_generator.longitude(),
        },
        "address": {
            "city": address_generator.city(),
            "country": address_generator.country_code(),
            "address": address_generator.address(),
            "zip": address_generator.zip_code(),
            "region": address_generator.region(),
        },
        "business": {
            "company": business_generator.company(),
            "type": business_generator.company_type(),
            "copyright": business_generator.copyright(),
            "currency": business_generator.currency_symbol(),
        },
        "registration": {
            "join_date": datetime_generator.datetime().isoformat(),
        },
    }).encode()


DATA_HEADER = struct.Struct("!I")


def gen_data(path, n, task_id: TaskID):
    with open(path, "a+b") as fp:
        fp.seek(0)
        head = fp.read(DATA_HEADER.size)

        if len(head) == DATA_HEADER.size and DATA_HEADER.unpack(head)[0] == n:
            log.info("Using previously generated file. Skipping")
            return

        fp.truncate(0)
        fp.flush()

        fp.write(b"\x00" * DATA_HEADER.size)

        progress.start_task(task_id)
        track = progress.track(
            range(n), task_id=task_id, total=n,
        )

        for i in track:
            value = get_value(i)
            fp.write(DATA_HEADER.pack(len(value)))
            fp.write(value)

        fp.seek(0)
        os.pwrite(fp.fileno(), DATA_HEADER.pack(n), 0)
        fp.flush()
        progress.stop_task(task_id)


def fill_db(path, *, pool_size, data_file, **kwargs):
    with ThreadPool(pool_size) as pool, \
         lsm.LSM(path, **kwargs) as db, \
         open(data_file, "rb") as fp:

        n = DATA_HEADER.unpack(fp.read(DATA_HEADER.size))[0]
        read_lock = RLock()

        task_id = progress.add_task(
            description=(
                f"Fill DB "
                f"[bold green]compress={kwargs.get('compress', 'none')}"
            ), total=n,
        )

        count = 0

        def insert(i):
            nonlocal count

            with read_lock:
                line = fp.read(
                    DATA_HEADER.unpack(fp.read(DATA_HEADER.size))[0],
                )

            db[get_key(i)] = line
            count += 1
            progress.update(task_id, completed=count)

        for _ in pool.imap_unordered(insert, range(n)):
            pass

        db.work(complete=True)


def select_thread_pool(
    path, *, keys_iter, keys_total, pool_size, **kwargs
):
    tls = local()
    db_pool = set()

    log.info("Opening: %s with %r", path, kwargs)
    with ThreadPool(pool_size) as pool:
        task_id = progress.add_task(
            description=(
                f"[bold blue]Select all keys sequentially "
                f"[bold green]compress={kwargs.get('compress', 'none')}"
            ), total=keys_total,
        )

        count = 0

        def select(k):
            nonlocal count
            if not hasattr(tls, "db"):
                tls.db = lsm.LSM(path, readonly=True, **kwargs)
                tls.db.open()
                db_pool.add(tls.db)

            _ = tls.db[get_key(k)]
            count += 1
            progress.update(task_id=task_id, completed=count)
            return 0

        for _ in pool.imap_unordered(select, keys_iter):
            pass

        for conn in db_pool:
            conn.close()


def copy_seq(path, **kwargs):
    with tempfile.TemporaryDirectory() as dest:
        dest = lsm.LSM(os.path.join(dest, "lsm-copy"), **kwargs)
        src = lsm.LSM(path, readonly=True, **kwargs)

        with src, dest:
            total_keys = len(src.keys())

            task_id = progress.add_task(
                description=f"Copy [bold green]{kwargs.get('compress', 'none')}",
            )
            track = progress.track(
                src.items(), task_id=task_id, total=total_keys,
            )

            for key, value in track:
                dest[key] = value


def run_parallel(func, cases):
    with ThreadPool(len(cases)) as pool:
        for _ in pool.imap_unordered(func, cases):
            pass


def run_sequentially(func, cases):
    for case in cases:
        func(case)


def main():
    arguments = parser.parse_args()

    run_insert = False
    run_select_seq = False
    run_select_rnd = False
    run_copy_seq = False

    if not arguments.benchmarks:
        run_insert = True
        run_select_seq = True
        run_select_rnd = True
        run_copy_seq = True
    else:
        if "insert" in arguments.benchmarks:
            run_insert = True
        if "select-seq" in arguments.benchmarks:
            run_select_seq = True
        if "select-rnd" in arguments.benchmarks:
            run_select_rnd = True
        if "copy-seq" in arguments.benchmarks:
            run_copy_seq = True

        if "all" in arguments.benchmarks:
            run_insert = True
            run_select_seq = True
            run_select_rnd = True
            run_copy_seq = True

    if not arguments.cases or "all" in arguments.cases:
        cases = [
            Cases.zstd(arguments.path),
            Cases.lz4(arguments.path),
            Cases.raw(arguments.path),
        ]
    else:
        cases = []
        if "zstd" in arguments.cases:
            cases.append(Cases.zstd(arguments.path))
        if "lz4" in arguments.cases:
            cases.append(Cases.lz4(arguments.path))
        if "raw" in arguments.cases:
            cases.append(Cases.raw(arguments.path))

    if arguments.run_sequentially:
        run = run_sequentially
    else:
        run = run_parallel

    if arguments.clear:
        for file_name in glob(arguments.path + ".*"):
            log.info("Removing: %r", file_name)
            os.remove(file_name)

    data_path = Path(arguments.path).parent / "data.json"

    def fill_job(item):
        path, kwargs = item
        return fill_db(
            path,
            pool_size=arguments.pool_size,
            data_file=data_path,
            **kwargs
        )

    def select_job(item):
        path, kwargs = item
        return select_thread_pool(
            path,
            pool_size=arguments.pool_size,
            keys_iter=range(arguments.count),
            keys_total=arguments.count,
            **kwargs
        )

    def select_random_job(item):

        path, kwargs = item
        keys = list(range(arguments.count))
        shuffle(keys)

        return select_thread_pool(
            path,
            pool_size=arguments.pool_size,
            keys_iter=iter(keys),
            keys_total=arguments.count,
            **kwargs
        )

    def copy_seq_job(item):
        path, kwargs = item
        return copy_seq(path, **kwargs)

    with progress:
        if run_insert:
            gen_data(
                data_path, arguments.count, progress.add_task(
                    description="Generate data",
                ),
            )

            run(fill_job, cases)

        if run_select_seq:
            log.info("Select all keys sequentially")
            run(select_job, cases)

        if run_select_rnd:
            log.info("Select all keys random")
            run(select_random_job, cases)

        if run_copy_seq:
            log.info("Copy database")
            run(copy_seq_job, cases)


if __name__ == "__main__":
    logging.basicConfig(
        level="NOTSET",
        format="%(message)s",
        datefmt="[%X]",
    )
    logging.getLogger().handlers.clear()
    logging.getLogger().handlers.append(
        RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True
        )
    )

    main()
