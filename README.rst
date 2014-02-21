The library provides a simple interface where queues are created
on-demand and items added using the ``put()`` and ``get()``
methods. In addition, queues are iterable, pulling items out as they
become available.

In addition, the ``put()`` method provides scheduling and
prioritization options. An item can be scheduled for work at a
particular time, and its work priority defined in terms of an expected
time where the item must be worked.

The library expects a connection implementation which is compatible
with the ``psycopg2`` library, e.g. `psycopg2cffi
<https://pypi.python.org/pypi/psycopg2cffi>`_.

The basic queue implementation is similar to Ryan Smith's
`queue_classic <https://github.com/ryandotsmith/queue_classic>`_
library written in Ruby, but uses `advisory locks
<http://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS>`_
for concurrency control.

In terms of performance, the implementation clock in at about 1,000
operations per second. Using the `PyPy <http://pypy.org/>`_
interpreter, this scales linearly with the number of cores available.


Getting started
===============

All functionality is encapsulated in a single class ``PQ``.

     ``class PQ(conn=None, pool=None, table="queue", debug=False)``

Example usage:

::

    from psycopg2 import connect
    from pq import PQ

    conn = connect("dbname=example user=postgres")
    pq = PQ(conn)

For multi-threaded operation, use a connection pool such as
``psycopg2.pool.ThreadedConnectionPool``.

You probably want to make sure your database is created with the
``utf-8`` encoding.

To create and configure the queue table, call the ``create()`` method.

::

    pq.create()

The table name defaults to ``"queue"``. To use a different name, pass
it as a string value as the ``table`` argument for the ```PQ`` class
(illustrated above).


Queues
======

The ``pq`` object exposes queues through Python's dictionary
interface:

::

    queue = pq["apples"]

The ``queue`` object provides ``get`` and ``put`` methods as explained
below, and in addition, it also works as a context manager where it
manages a transaction:

::

    with queue as cursor:
        ...

The statements inside the context manager are either committed as a
transaction or rejected, atomically. This is useful when a queue is
used to manage tasks because it allows you to retrieve a task from the
queue, perform a task and write a result, with transactional
semantics.

Methods
=======

Use the ``put(data)`` method to insert an item into the queue. It
takes a JSON-compatible object such as a Python dictionary:

::

    queue.put({'kind': 'Cox'})
    queue.put({'kind': 'Arthur Turner'})
    queue.put({'kind': 'Golden Delicious'})

Items are pulled out of the queue using ``get(block=True)``. The
default behavior is to block until an item is available with a default
timeout of one second after which a value of ``None`` is returned.

::

    def eat(kind):
        print "umm, %s apples taste good." % kind

    task = queue.get()
    eat(**task.data)

The ``task`` object provides additional metadata in addition to the
``data`` attribute as illustrated by the string representation:

    >>> task
    <pq.Task id=77709 size=1 enqueued_at="2014-02-21T16:22:06Z" schedule_at=None>

The ``get`` operation is also available through iteration:

::

    for task in queue:
        if task is None:
            break

        eat(**task.data)

The iterator blocks if no item is available. Again, there is a default
timeout of one second, after which the iterator yields a value of
``None``.

An application can then choose to break out of the loop, or wait again
for an item to be ready.

::

    for task in queue:
        if task is not None:
            eat(**task.data)

        # This is an infinite loop!


Scheduling
==========

Items can be scheduled such that they're not pulled until a later
time:

::

    queue.put({'kind': 'Cox'}, "5m")

In this example, the item is ready for work five minutes later. The
method also accepts ``datetime`` and ``timedelta`` objects.


Priority
========

If some items are more important than others, a time expectation can
be expressed:

::

    queue.put({'kind': 'Cox'}, expected_at="5m")

This tells the queue processor to give priority to this item over an
item expected at a later time, and conversely, to prefer an item with
an earlier expected time.

The scheduling and priority options can be combined:

::

    queue.put({'kind': 'Cox'}, "1h", "2h")

This item won't be pulled out until after one hour, and even then,
it's only processed subject to it's priority of two hours.


Pickles
=======

If a queue name is provided as ``<name>/pickle``
(e.g. ``"jobs/pickle"``), items are automatically pickled and
unpickled using Python's built-in ``cPickle`` module:

::

    queue = pq["apples/pickle"]

    class Apple(object):
        def __init__(self, kind):
           self.kind = kind

    queue.put(Apple("Cox"))

The old pickle protocol ``0`` is used to ensure the pickled data is
encoded as ``ascii`` which should be compatible with any database
encoding.


Thread-safety
=============

All objects are thread-safe as long as a connection pool is provided
where each thread receives its own database connection.
