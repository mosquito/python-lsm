lsm
===

Fast Python bindings for [SQLite's LSM key/value store](http://www.sqlite.org/src4/doc/trunk/www/lsmusr.wiki>).
The LSM storage engine was initially written as part of the experimental
SQLite4 rewrite (now abandoned). More recently, the LSM source code was moved
into the SQLite3 [source tree](https://www.sqlite.org/cgi/src/dir?ci=e148cdad35520e66&name=ext/lsm1)
and has seen some improvements and fixes. This project uses the LSM code from
the SQLite3 source tree.

Features:

* Embedded zero-conf database.
* Keys support in-order traversal using cursors.
* Transactional (including nested transactions).
* Single writer/multiple reader MVCC based transactional concurrency model.
* On-disk database stored in a single file.
* Data is durable in the face of application or power failure.
* Thread-safe.
* Releases GIL for read and write operations
  (each connection has own mutex)
* Page compression (lz4 or zstd)
* Zero dependency static library
* Python 3.x.

Limitations:

The source for Python lsm is
[hosted on GitHub](https://github.com/mosquito/python-lsm).

If you encounter any bugs in the library, please
[open an issue](https://github.com/mosquito/python-lsm/issues/new),
including a description of the bug and any related traceback.

## Quick-start

Below is a sample interactive console session designed to show some of the
basic features and functionality of the ``lsm`` Python library.

To begin, instantiate a `LSM` object, specifying a path to a database file.

<!--  name: test_example_db -->
```python
from lsm import LSM
db = LSM('test.ldb')
assert db.open()
```

More pythonic variant is using context manager:

<!--  name: test_example_db_context_manager -->
```python
from lsm import LSM
with LSM("test.ldb") as db:
    assert db.info()
```

Not opened database will raise a RuntimeError:

<!--  name: test_example_db -->
```python
import pytest
from lsm import LSM

db = LSM('test.ldb')

with pytest.raises(RuntimeError):
    db.info()
```

### Binary/string mode

You should select mode for opening the database with ``binary: bool = True``
argument.

For example when you want to store strings just pass ``binary=False``:

<!--  name: test_binary_mode -->
```python
from lsm import LSM
with LSM("test_0.ldb", binary=False) as db:
    # must be str for keys and values
    db['foo'] = 'bar'
    assert db['foo'] == "bar"
```

Otherwise, you must pass keys and values ad ``bytes`` (default behaviour):

<!--  name: test_string_mode -->
```python
from lsm import LSM

with LSM("test.ldb") as db:
    db[b'foo'] = b'bar'
    assert db[b'foo'] == b'bar'
```

### Key/Value Features

``lsm`` is a key/value store, and has a dictionary-like API:

<!--  name: test_getitem -->
```python
from lsm import LSM
with LSM("test.ldb", binary=False) as db:
    db['foo'] = 'bar'
    assert db['foo'] == 'bar'
```

Database apply changes as soon as possible:

<!--  name: test_get_del_item -->
```python
import pytest
from lsm import LSM

with LSM("test.ldb", binary=False) as db:
    for i in range(4):
         db[f'k{i}'] = str(i)

    assert 'k3' in db
    assert 'k4' not in db
    del db['k3']

    with pytest.raises(KeyError):
        print(db['k3'])
```

By default, when you attempt to look up a key, ``lsm`` will search for an
exact match. You can also search for the closest key, if the specific key you
are searching for does not exist:

<!--  name: test_get_del_item_seek_mode -->
```python
import pytest
from lsm import LSM, SEEK_LE, SEEK_GE, SEEK_LEFAST


with LSM("test.ldb", binary=False) as db:
    for i in range(4):
        db[f'k{i}'] = str(i)

    # Here we will match "k1".
    assert db['k1xx', SEEK_LE] == '1'

    # Here we will match "k1" but do not fetch a value
    # In this case the value will always be ``True`` or there will
    # be an exception if the key is not found
    assert db['k1xx', SEEK_LEFAST] is True

    with pytest.raises(KeyError):
        print(db['000', SEEK_LEFAST])

    # Here we will match "k2".
    assert db['k1xx', SEEK_GE] == "2"
```

`LSM` supports other common dictionary methods such as:

* `keys()`
* `values()`
* `items()`
* `update()`


### Slices and Iteration

The database can be iterated through directly, or sliced. When you are slicing
the database the start and end keys need not exist -- ``lsm`` will find the
closest key (details can be found in the [LSM.fetch_range()](https://lsm-db.readthedocs.io/en/latest/api.html#lsm.LSM.fetch_range)
documentation).

<!--
    name: test_slices;
-->
```python
from lsm import LSM

with LSM("test_slices.ldb", binary=False) as db:

    # clean database
    for key in db.keys():
        del db[key]

    db['foo'] = 'bar'

    for i in range(3):
        db[f'k{i}'] = str(i)

    # Can easily iterate over the database items
    assert (
        sorted(item for item in db.items()) == [
            ('foo', 'bar'), ('k0', '0'), ('k1', '1'), ('k2', '2')
        ]
    )

    # However, you will not read the entire database into memory, as special
    # iterator objects are used.
    assert str(db['k0':'k99']).startswith("<lsm_slice object at")

    # But you can cast it to the list for example
    assert list(db['k0':'k99']) == [('k0', '0'), ('k1', '1'), ('k2', '2')]
```

You can use open-ended slices. If the lower- or upper-bound is outside the
range of keys an empty list is returned.


<!--
    name: test_slices;
    case: open_ended_slices
-->
```python
with LSM("test_slices.ldb", binary=False, readonly=True) as db:
    assert list(db['k0':]) == [('k0', '0'), ('k1', '1'), ('k2', '2')]
    assert list(db[:'k1']) == [('foo', 'bar'), ('k0', '0'), ('k1', '1')]
    assert list(db[:'aaa']) == []
```

To retrieve keys in reverse order or stepping over more than one item,
simply use a third slice argument as usual.
Negative step value means reverse order, but first and second arguments
must be ordinarily ordered.

<!--
    name: test_slices;
    case: reverse_slices
-->
```python
with LSM("test_slices.ldb", binary=False, readonly=True) as db:
    assert list(db['k0':'k99':2]) == [('k0', '0'), ('k2', '2')]
    assert list(db['k0'::-1]) == [('k2', '2'), ('k1', '1'), ('k0', '0')]
    assert list(db['k0'::-2]) == [('k2', '2'), ('k0', '0')]
    assert list(db['k0'::3]) == [('k0', '0')]
```

You can also **delete** slices of keys, but note that delete **will not**
include the keys themselves:

<!--
    name: test_slices;
    case: del_slice
-->
```python
with LSM("test_slices.ldb", binary=False) as db:
    del db['k0':'k99']

    # Note that 'k0' still exists.
    assert list(db.items()) == [('foo', 'bar'), ('k0', '0')]
```

### Cursors

While slicing may cover most use-cases, for finer-grained control you can use
cursors for traversing records.

<!--
    name: test_cursors;
    case: iterate_over_one_item
-->
```python
from lsm import LSM, SEEK_GE, SEEK_LE

with LSM("test_cursors.ldb", binary=False) as db:
    del db["a":"z"]

    db["spam"] = "spam"

    with db.cursor() as cursor:
        cursor.seek('spam')
        key, value = cursor.retrieve()
        assert key == 'spam'
        assert value == 'spam'
```

Seeking over cursors:

<!--
    name: test_cursors;
    case: iterate_over_multiple_items
-->
```python
with LSM("test_cursors.ldb", binary=False) as db:
    db.update({'k0': '0', 'k1': '1', 'k2': '2', 'k3': '3', 'foo': 'bar'})

    with db.cursor() as cursor:

        cursor.first()
        key, value = cursor.retrieve()
        assert key == "foo"
        assert value == "bar"

        cursor.last()
        key, value = cursor.retrieve()
        assert key == "spam"
        assert value == "spam"

        cursor.previous()
        key, value = cursor.retrieve()
        assert key == "k3"
        assert value == "3"

```

Finding the first match that is greater than or equal to `'k0'` and move
forward until the key is less than `'k99'`

<!--
    name: test_cursors;
    case: iterate_ge_until_k99
-->
```python
with LSM("test_cursors.ldb", binary=False) as db:
    with db.cursor() as cursor:
        cursor.seek("k0", SEEK_GE)
        results = []

        while cursor.compare("k99") > 0:
            key, value = cursor.retrieve()
            results.append((key, value))
            cursor.next()

    assert results == [('k0', '0'), ('k1', '1'), ('k2', '2'), ('k3', '3')]

```

Finding the last match that is lower than or equal to `'k99'` and move
backward until the key is less than `'k0'`

<!--
    name: test_cursors;
    case: iterate_le_until_k0
-->
```python
with LSM("test_cursors.ldb", binary=False) as db:
    with db.cursor() as cursor:
        cursor.seek("k99", SEEK_LE)
        results = []

        while cursor.compare("k0") >= 0:
            key, value = cursor.retrieve()
            results.append((key, value))
            cursor.previous()

    assert results == [('k3', '3'), ('k2', '2'), ('k1', '1'), ('k0', '0')]
```

It is very important to close a cursor when you are through using it. For this
reason, it is recommended you use the `LSM.cursor()` context-manager, which
ensures the cursor is closed properly.

### Transactions

``lsm`` supports nested transactions. The simplest way to use transactions
is with the `LSM.transaction()` method, which returns a context-manager:

<!-- name: test_transactions -->
```python
from lsm import LSM

with LSM("test_tx.ldb", binary=False) as db:
    del db["a":"z"]
    for i in range(10):
        db[f"k{i}"] = f"{i}"


with LSM("test_tx.ldb", binary=False) as db:
    with db.transaction() as tx1:
        db['k1'] = '1-mod'

        with db.transaction() as tx2:
            db['k2'] = '2-mod'
            tx2.rollback()

    assert db['k1'] == '1-mod'
    assert db['k2'] == '2'
```

You can commit or roll-back transactions part-way through a wrapped block:

<!-- name: test_transactions_2 -->
```python
from lsm import LSM

with LSM("test_tx_2.ldb", binary=False) as db:
    del db["a":"z"]
    for i in range(10):
        db[f"k{i}"] = f"{i}"

with LSM("test_tx_2.ldb", binary=False) as db:
    with db.transaction() as txn:
        db['k1'] = 'outer txn'

        # The write operation is preserved.
        txn.commit()

        db['k1'] = 'outer txn-2'

        with db.transaction() as txn2:
            # This is committed after the block ends.
            db['k1'] = 'inner-txn'

        assert db['k1'] == "inner-txn"

        # Rolls back both the changes from txn2 and the preceding write.
        txn.rollback()

        assert db['k1'] == 'outer txn', db['k1']
```


If you like, you can also explicitly call `LSM.begin()`, `LSM.commit()`, and
`LSM.rollback()`.

<!-- name: test_transactions_db -->
```python
from lsm import LSM

# fill db
with LSM("test_db_tx.ldb", binary=False) as db:
    del db["k":"z"]
    for i in range(10):
        db[f"k{i}"] = f"{i}"


with LSM("test_db_tx.ldb", binary=False) as db:
    # start transaction
    db.begin()
    db['k1'] = '1-mod'

    # nested transaction
    db.begin()
    db['k2'] = '2-mod'
    # rolling back nested transaction
    db.rollback()

    # comitting top-level transaction
    db.commit()

    assert db['k1'] == '1-mod'
    assert db['k2'] == '2'
```

### Thanks to

* [@coleifer](https://github.com/coleifer) - this project was inspired by
[coleifer/python-lsm-db](https://github.com/coleifer/python-lsm-db).
