The library expects a connection implementation which is compatible
with the ``psycopg2`` library, e.g. `psycopg2cffi
<https://pypi.python.org/pypi/psycopg2cffi>`_.

The implementation is similar to Ryan Smith's `queue_classic
<https://github.com/ryandotsmith/queue_classic>`_ library written in
Ruby, but uses `advisory locks
<http://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS>`_
for concurrency control.


Getting started
===============

All functionality is encapsulated in a single class ``Manager``.

     ``class Manager(conn=None, pool=None, table="queue", debug=False)``

Example usage:

::

    from psycopg2 import connect
    from pq import Manager

    conn = connect("dbname=example user=postgres")
    manager = Manager(conn)

To prepare the database, you can call the ``install()`` method. It
creates the queue table and adds an insert trigger.

::

    # Create tables and configure trigger.
    manager.install()


Queues
======

The manager exposes queues as a dictionary interface:

::

    queue = manager["apples"]

Use the ``put(data)`` method to insert items into the queue. It takes a
JSON-compatible object, e.g. a ``dict``.

::

    queue.put({'type': 'Cox'})
    queue.put({'type': 'Arthur Turner'})
    queue.put({'type': 'Golden Delicious'})

Items are pulled out of the queue using ``get(block=True)``. The
default behavior is to block until an item is available.

::

    apple = queue.get()

Alternatively, there's an iterator interface available.

::

    for apple in queue:
        if apple is None:
            break

        apple.eat()

The iterator blocks if no item is available. Importantly, there is a
default timeout of one second, after which the iterator yields a value
of ``None``.


Thread-safety
=============

All objects are thread-safe as long as a connection pool is provided
where each thread receives its own database connection.
