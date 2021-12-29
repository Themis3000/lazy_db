=====
Usage
=====

To use lazy_db in a project::

    from lazy_db import LazyDb

    db = LazyDb("test.lazy")
    db.write("key", "value")
    print(db.read("key"))  # prints "value"
    db.close()

In place of "key" and integer may also be used, as well as in place of "value" an integer, dictionary, list of integers, or a bytes object may be used.

Only the initiation of the database, as the write and read methods should ever be used for normal use. All other methods included in the LazyDb class should only be used within that class.
