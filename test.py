from lazy_db import LazyDb

try:
    db = LazyDb("test.lazy")
    # db.write(145, 2938475)
    print(db.read(145))
finally:
    db.close()
