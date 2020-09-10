import json
import os
import struct
import tempfile
from argparse import ArgumentParser
from glob import glob
from multiprocessing import cpu_count
from threading import local
from typing import Union

import lsm
from multiprocessing.pool import ThreadPool

from tqdm import tqdm
from mimesis import Person, Address, Business, Datetime


parser = ArgumentParser()
parser.add_argument("-n", "--count", default=100000, type=int)
parser.add_argument("--pool-size", type=int, default=cpu_count())
parser.add_argument(
    "--keep",
    help="Keep existent database before writing",
    action="store_true",
)
parser.add_argument(
    "--path",
    default=os.path.join(tempfile.gettempdir(), "lsm-compressed"),
)


person_generator = Person('en')
address_generator = Address('en')
business_generator = Business('en')
datetime_generator = Datetime('en')


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
    })


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


def select_from_db(path, *, n, pool_size, **kwargs):
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
            pool.imap_unordered(select, range(n)),
            desc=f"select {kwargs.get('compress', 'none')}", total=n
        ):
            pass

        for conn in db_pool:
            conn.close()


def main():
    arguments = parser.parse_args()

    cases = [
        [
            arguments.path + ".lsm.zst",
            dict(multiple_processes=False, compress="zstd")
        ],
        [
            arguments.path + ".lsm.lz4",
            dict(multiple_processes=False, compress="lz4")
        ],
        [
            arguments.path + ".lsm",
            dict(multiple_processes=False)
        ],
    ]

    with ThreadPool(len(cases)) as POOL:
        print("Filling DB")

        if not arguments.keep:
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

        for _ in POOL.imap_unordered(fill_job, cases):
            pass

        print("Selecting")

        def select_job(item):
            path, kwargs = item
            return select_from_db(
                path,
                pool_size=arguments.pool_size,
                n=arguments.count,
                **kwargs
            )

        for _ in POOL.imap_unordered(select_job, cases):
            pass


if __name__ == '__main__':
    main()
