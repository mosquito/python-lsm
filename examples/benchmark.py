import json
import os
import struct
import tempfile
from argparse import ArgumentParser, Action
from glob import glob
from multiprocessing import cpu_count
from random import shuffle
from threading import local
from typing import Union

import lsm
from multiprocessing.pool import ThreadPool

from tqdm import tqdm
from mimesis import Person, Address, Business, Datetime


class AppendConstAction(Action):
    def __init__(self, option_strings, dest, const=None, default=None,
                 type=None, choices=None, required=False,
                 help=None, metavar=None):
        assert const, "const= required"
        super().__init__(option_strings, dest, const=const, nargs=0,
                         default=default, type=type, choices=choices,
                         required=required, help=help, metavar=metavar)

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


person_generator = Person('en')
address_generator = Address('en')
business_generator = Business('en')
datetime_generator = Datetime('en')


class Cases:
    @classmethod
    def lz4(cls, path):
        return [
            path + ".lsm.lz4",
            dict(multiple_processes=False, compress="lz4")
        ]

    @classmethod
    def zstd(cls, path):
        return [
            path + ".lsm.zst",
            dict(multiple_processes=False, compress="zstd")
        ]

    @classmethod
    def raw(cls, path):
        return [
            path + ".lsm",
            dict(multiple_processes=False)
        ]


def get_key(idx) -> Union[bytes, str]:
    return struct.pack("I", idx)


def get_value(idx) -> Union[bytes, str]:
    return json.dumps({
        "id": idx,
        "person": {
            "full_name": person_generator.full_name(),
            "email": person_generator.email(domains=[
                'gmail.com',
                'hotmail.com',
                'yandex.ru',
                'mail.ru',
            ]),
            "phone": person_generator.telephone(mask='+7(9##)-###-####'),
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
        }
    }).encode()


def fill_db(path, *, n, pool_size, **kwargs):
    print("Opening:", path, "with", kwargs)
    with ThreadPool(pool_size) as pool, lsm.LSM(path, **kwargs) as db:

        def insert(i):
            db[get_key(i)] = get_value(i)

        for _ in tqdm(
            pool.imap_unordered(insert, range(n)),
            desc=f"insert {kwargs.get('compress', 'none')}", total=n
        ):
            pass


def select_thread_pool(path, *, keys_iter, keys_total, pool_size, **kwargs):
    tls = local()
    db_pool = set()

    print("Opening:", path, "with", kwargs)
    with ThreadPool(pool_size) as pool:
        def select(k):
            if not hasattr(tls, 'db'):
                tls.db = lsm.LSM(path, readonly=True, **kwargs)
                tls.db.open()
                db_pool.add(tls.db)

            _ = tls.db[get_key(k)]
            return 0

        for _ in tqdm(
            pool.imap_unordered(select, keys_iter),
            desc=f"select {kwargs.get('compress', 'none')}",
            total=keys_total
        ):
            pass

        for conn in db_pool:
            conn.close()


def copy_seq(path, **kwargs):
    print("Opening:", path, "with", kwargs)

    with tempfile.TemporaryDirectory() as dest:
        dest = lsm.LSM(os.path.join(dest, "lsm-copy"), **kwargs)
        src = lsm.LSM(path, readonly=True, **kwargs)

        with src, dest:
            for key, value in tqdm(src.items(), total=len(src.keys())):
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
            print("Removing: ", file_name)
            os.remove(file_name)

    def fill_job(item):
        path, kwargs = item
        return fill_db(
            path,
            pool_size=arguments.pool_size,
            n=arguments.count,
            **kwargs
        )

    if run_insert:
        print("Filling DB")
        run(fill_job, cases)

    def select_job(item):
        path, kwargs = item
        return select_thread_pool(
            path,
            pool_size=arguments.pool_size,
            keys_iter=range(arguments.count),
            keys_total=arguments.count,
            **kwargs
        )

    if run_select_seq:
        print("Select all keys sequentially")
        run(select_job, cases)

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

    if run_select_rnd:
        print("Select all keys random")
        run(select_random_job, cases)

    def copy_seq_job(item):
        path, kwargs = item
        return copy_seq(path, **kwargs)

    if run_copy_seq:
        print("Copy database")
        run(copy_seq_job, cases)


if __name__ == '__main__':
    main()
