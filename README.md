# lazy_db

[![badge](https://img.shields.io/pypi/v/lazy-database.svg)](https://pypi.python.org/pypi/lazy-database)
[![badge](https://readthedocs.org/projects/lazy-db/badge/?version=latest)](https://lazy-db.readthedocs.io/en/latest/?version=latest)

* Free software: MIT license
* Documentation: https://lazy-db.readthedocs.io.

A lazily loaded key:value db intended for use with large datasets that are too big to be loaded into memory. The database supports integers, strings, lists of integers, bytes, and dictionaries. This database is meant to strike a good balance of retrieval/insertion speed and memory usage. This database best fits a scenario where each key has a lot of data stored under it. Scenarios where values are under 100 bytes in size this database is not very well suited for.

Install with `pip install lazy-database`

Example usage:

```python
from lazy_db import LazyDb

db = LazyDb("test.lazy")
db.write("test_value", "value")
print(db.read("test_value"))  # prints "value"
db.close()
```

## How it works

#### File layout

All text in database files are encoded in utf-8 format. Each database has a json string at the start of the file that denotes the database's settings, who's end is marked with a NUL byte (00 in hex)

Each database entry is appended at the end of the file and is laid out in this format:

Name           | Size (bytes)     | Purpose
---------------|------------------|-------------
NUL            | 1                | Marks the start of the entry. When the initial headers index, the starting byte of each entry is checked for this NUL byte to be sure the database hasn't been corrupted. This is the beginning to what is considered the "header" for the entry (NUL bytes carry a hex value of 0x00)
Key type       | 1                | Marks if the key is an integer or a string.
Key            | any              | The key for the database entry.
NUL            | 1                | Marks the end of the key. This is necessary since string keys don't have a set size.
Content length | content_int_size | An integer (little endian) depicting the length of the content (including the content type). Defaults to 4 bytes long. This is the end to what is considered the "header" for the entry
Content type   | 1                | Marks if the content is a string, int, int list, dict, or bytes.
Content        | Content length   | Stores the content

#### Content type labels

Name     | Hex type value | Type description
---------|----------------|-------------
String   | 0x01           | A utf-8 string
Int      | 0x02           | An integer
Dict     | 0x03           | A dictionary (internally stored as a utf-8 json string)
Int list | 0x04           | A list of integers. Max integer size is defined by int_list_size (default: 4 bytes)
Bytes    | 0x05           | A bytes object

#### The algorithm

When loading a database, all entry headers are scanned for their key value and lengths. This allows for values to be retrieved very quickly without having to load the content of every entry, at the cost of having to store the key and content length in memory though. This approach makes the database best for cases where your database will be storing a lot of data in each key that you can't afford to store in memory, however you can afford to store the name values and lengths of each element in memory.
