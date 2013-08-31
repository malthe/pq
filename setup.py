"""\
PQ is a simple transaction queue for PostgreSQL.

The library expects a connection implementation which is compatible
with the ``psycopg2`` library, e.g. `psycopg2cffi
<https://pypi.python.org/pypi/psycopg2cffi>`_.

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

"""

import sys
import os
from setuptools import setup, find_packages

descriptions = __doc__.split('.\n', 1)

setup(
    name='pq',
    version='1.0-dev',
    url='https://github.com/malthe/pq/',
    license='BSD',
    author='Malthe Borch',
    author_email='mborch@gmail.com',
    description=descriptions[0],
    long_description=descriptions[1],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        #'Development Status :: 1 - Planning',
        #'Development Status :: 2 - Pre-Alpha',
        'Development Status :: 3 - Alpha',
        #'Development Status :: 4 - Beta',
        #'Development Status :: 5 - Production/Stable',
        #'Development Status :: 6 - Mature',
        #'Development Status :: 7 - Inactive',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
    ],
    test_suite="pq.tests",
)
