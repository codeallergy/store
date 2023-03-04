# store interface

Transactional Data Store interface for several embedded fast key-value storage engines:

Supported back-ends:
* badger (including encryption)
* boltdb
* bbolt
* pebbledb (like rocksdb)

All back-ends represented in separate repositories to keep flexibility of dependencies in your application.

The goal of this repository is to have a single interface for known embedded fast storage engines supported in one place with minimal differences in APIs.
This approach will simplify selection and migration between embedded fast storage engines for multi-purpose applications.
