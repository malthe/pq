Changes
=======

In next release ...

Features:

- Queues now yield task objects which provide metadata and allows
  reading and writing task data.

Bugs:

- The same connection pool can now be used with different queues.

- The transaction manager now correctly returns connections to the
  pool.

Wishlist:

- Support for SQLAlchemy.


1.0 (2013-11-20)
----------------

- Initial public release.
