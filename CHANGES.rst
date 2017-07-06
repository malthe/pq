Changes
=======

1.6 (unreleased)
----------------

- Fix compatibility with `NamedTupleCursor`.


1.5 (2017-04-18)
----------------

- Fixed Python 2 compatibility.


1.4 (2016-03-25)
----------------

- Added worker class and handler helper decorator.
  [jeanphix]


1.3 (2015-05-11)
----------------

- Python 3 compatibility.
  [migurski]

- Fix time zone issue.


1.2 (2014-10-21)
----------------

Improvements:

- Fixed concurrency issue where a large number of locks would be held
  as a queue grows in size.

- Fixed a database connection resource issue.


1.1 (2014-02-27)
----------------

Features:

- A queue is now also a context manager, providing transactional
  semantics.

- A queues now returns task objects which provide metadata and allows
  reading and writing task data.

Improvements:

- The same connection pool can now be used with different queues.

Bugs:

- The `Literal` string wrapper did not work correctly with `psycopg2`.

- The transaction manager now correctly returns connections to the
  pool.


1.0 (2013-11-20)
----------------

- Initial public release.
