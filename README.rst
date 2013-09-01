The library expects a connection implementation which is compatible
with the ``psycopg2`` library, e.g. `psycopg2cffi
<https://pypi.python.org/pypi/psycopg2cffi>`_.

The library is similar to Ryan Smith's `queue_classic
<https://github.com/ryandotsmith/queue_classic>`_ library written in
Ruby, but uses `advisory locks
<http://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS>`_
for concurrency control 

Setup:

::

    from psycopg2 import connect
    from pq import Manager

    conn = connect("dbname=example user=postgres")

    # In the following we pass an already established connection, but
    # we can also provide a connection pool using the ``pool``
    # keyword.
    manager = Manager(conn, table="queue")

    # Create tables and configure trigger.
    manager.install()

    # Define queue. Note that queues are implicitly defined by the
    # items that are put into it. That is, an empty queue exists only
    # in Python.
    queue = manager["apples"]

Usage:

::

    # Insert JSON-compatible objects:
    queue.put({'foo': 'bar'})

    # Retrieve using either blocking or non-blocking call (the default
    # call is blocking; for a non-blocking call, pass a false value).
    item = queue.get()

    # Iterate through queue and pull items out. This iterator blocks
    # until a timeout after which it returns a null item.
    for item in queue:
        if item is None:
            break

The ``Queue`` object returned by the manager is thread-safe. The
implementation uses the event notification system built into
PostgreSQL to block on item retrieval without polling.
