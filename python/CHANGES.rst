Changes
=======

In next release ...

1.9.1 (2023-04-04)
------------------

- Add support for PostgreSQL 14 [kalekseev].

  See https://www.postgresql.org/docs/14/release-14.html#id-1.11.6.11.4.

- Add support for using a custom schema (issue #35).

1.9.0 (2020-09-29)
------------------

- The task executor now receives `job_id` as the first argument.

1.8.2 (2020-08-14)
------------------

- Added support for queue names longer than 63 characters.

  A database migration (dropping and recreating the `pq_notify`
  trigger) is required if using names longer than this limit. If not
  using, then no migration is required.

- Return connections to the pool if an exception is raised while it is retrieved

1.8.1 (2019-07-30)
------------------

- Added overridable `encode` and `decode` methods which are
  responsible for turning task data into `JSON` and vice-versa.

1.8.0 (2019-07-03)
------------------

- Change policy on task priority. Tasks with a null-value for
  `expected_at` are now processed after items that have a value set.

1.7.0 (2019-04-07)
------------------

- Use `SKIP LOCKED` instead of advisory lock mechanism (PostgreSQL 9.5+).

1.6.1 (2018-11-14)
------------------

- Fix queue class factory pattern.

1.6 (2018-11-12)
----------------

- Fix compatibility with `NamedTupleCursor`.

- Fix duplicate column name issue.

- Add option to specify own queue class.


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
