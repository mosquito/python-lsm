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

```python

>>> from lsm import LSM
>>> db = LSM('test.ldb')
>>> db.open()
>>> print(db)
<LSM at "/tmp/test.ldb" as 0x10951e450>
```

More pythonic variant is using context manager:

```python
>>> from lsm import LSM
>>> with LSM("/tmp/test.ldb") as db:
...     print(db)
<LSM at "/tmp/test.ldb" as 0x10951e450>
```

### Binary/string mode

You should select mode for opening the database with ``binary: bool = True``
argument.

For example when you want to store strings just pass ``binary=False``:

```python
>>> from lsm import LSM
>>> with LSM("/tmp/test.ldb", binary=False) as db:
...    db['foo'] = 'bar'   # must be str for keys and values
...    print(db['foo'])
bar
```

Otherwise, you must pass keys and values ad ``bytes`` (default behaviour):

```python
>>> from lsm import LSM
>>> with LSM("/tmp/test.ldb") as db:
...    db[b'foo'] = b'bar'   # must be bytes for keys and values
...    print(db[b'foo'])
b'bar'
```

### Key/Value Features

``lsm`` is a key/value store, and has a dictionary-like API:

```python
>>> from lsm import LSM
>>> with LSM("/tmp/test.ldb", binary=False) as db:
...    db['foo'] = 'bar'
...    print(db['foo'])
bar
```

Database apply changes as soon as possible:

```python
>>> from lsm import LSM
>>> db = LSM("/tmp/test.ldb", binary=False)
>>> db.open()
True
>>> for i in range(4):
...     db[f'k{i}'] = str(i)
...
>>> 'k3' in db
True
>>> 'k4' in db
False
>>> del db['k3']
>>> db['k3']
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
KeyError: "Key 'k3' was not found"
```

By default when you attempt to look up a key, ``lsm`` will search for an
exact match. You can also search for the closest key, if the specific key you
are searching for does not exist:

```python

>>> from lsm import, LSM, SEEK_LE, SEEK_GE
>>> db = LSM("/tmp/test.ldb", binary=False)
>>> db.open()
True
>>> db['k1xx', SEEK_LE]  # Here we will match "k1".
'1'
>>> db['k1xx', SEEK_GE]  # Here we will match "k2".
'2'
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

```python

>>> [item for item in db.items()]
[('foo', 'bar'), ('k0', '0'), ('k1', '1'), ('k2', '2')]

>>> db['k0':'k99']
<lsm_slice object at 0x10d4f3500>

>>> list(db['k0':'k99'])
[('k0', '0'), ('k1', '1'), ('k2', '2')]
```

You can use open-ended slices. If the lower- or upper-bound is outside the
range of keys an empty list is returned.

```python

>>> list(db['k0':])
[('k0', '0'), ('k1', '1'), ('k2', '2')]

>>> list(db[:'k1'])
[('foo', 'bar'), ('k0', '0'), ('k1', '1')]

>>> list(db[:'aaa'])
[]
```

To retrieve keys in reverse order or stepping over more then one item,
simply use a third slice argument as usual.
Negative step value means reverse order, but first and second arguments
must be ordinary ordered.

```python

>>> list(db['k0':'k99':2])
[('k0', '0'), ('k2', '2')]

>>> list(db['k0'::-1])
[('k2', '2'), ('k1', '1'), ('k0', '0')]

>>> list(db['k0'::-2])
[('k2', '2'), ('k0', '0')]


>>> list(db['k0'::3])
[('k0', '0')]
```

You can also **delete** slices of keys, but note that the delete **will not**
include the keys themselves:

```python

>>> del db['k0':'k99']

>>> list(db)  # Note that 'k0' still exists.
[('foo', 'bar'), ('k0', '0')]
```

### Cursors

While slicing may cover most use-cases, for finer-grained control you can use
cursors for traversing records.

```python

>>> with db.cursor() as cursor:
...     for key, value in cursor:
...         print(key, '=>', value)
...
foo => bar
k0 => 0

>>> db.update({'k1': '1', 'k2': '2', 'k3': '3', 'foo': 'bar'})

>>> with db.cursor() as cursor:
...     cursor.first()
...     print(cursor.key())
...     cursor.last()
...     print(cursor.key())
...     cursor.previous()
...     print(cursor.key())
...
foo
k3
k2

>>> with db.cursor() as cursor:
...     cursor.seek('k0', SEEK_GE)
...     print(list(cursor.fetch_until('k99')))
...
[('k0', '0'), ('k1', '1'), ('k2', '2'), ('k3', '3')]
```

It is very important to close a cursor when you are through using it. For this
reason, it is recommended you use the `LSM.cursor()` context-manager, which
ensures the cursor is closed properly.

### Transactions

``lsm`` supports nested transactions. The simplest way to use transactions
is with the `LSM.transaction()` method, which doubles as a context-manager or
decorator.

```python

>>> with db.transaction() as txn:
...     db['k1'] = '1-mod'
...     with db.transaction() as txn2:
...         db['k2'] = '2-mod'
...         txn2.rollback()
...
True
>>> print(db['k1'], db['k2'])
1-mod 2
```

You can commit or roll-back transactions part-way through a wrapped block:

```python

>>> with db.transaction() as txn:
...    db['k1'] = 'outer txn'
...    txn.commit()  # The write is preserved.
...
...    db['k1'] = 'outer txn-2'
...    with db.transaction() as txn2:
...        db['k1'] = 'inner-txn'  # This is commited after the block ends.
...    print(db['k1']  # Prints "inner-txn".)
...    txn.rollback()  # Rolls back both the changes from txn2 and the preceding write.
...    print(db['k1'])
...
1              <- Return value from call to commit().
inner-txn      <- Printed after end of txn2.
True           <- Return value of call to rollback().
outer txn      <- Printed after rollback.
```

If you like, you can also explicitly call `LSM.begin()`, `LSM.commit()`, and
`LSM.rollback()`.

```python

>>> db.begin()
>>> db['foo'] = 'baze'
>>> print(db['foo'])
baze
>>> db.rollback()
True
>>> print(db['foo'])
bar
```

### Thanks to

* [@coleifer](https://github.com/coleifer) - this project was inspired by
[coleifer/python-lsm-db](https://github.com/coleifer/python-lsm-db).
