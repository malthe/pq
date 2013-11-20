import os
import re
import cPickle as pickle

from datetime import datetime, timedelta
from select import select
from threading import current_thread
from logging import getLogger
from weakref import WeakValueDictionary

from json import dumps

from .utils import Literal, prepared, transaction


_re_timedelta = re.compile(r'(\d+)([smhd])')
_timedelta_table = dict(s='seconds', m='minutes', h='hours', d='days')


def _read_sql(name, path=os.path.dirname(__file__)):
    return open(os.path.join(path, '%s.sql' % name), 'r').read()


def _convert_time_spec(spec):
    if spec is None:
        return

    if isinstance(spec, basestring):
        m = _re_timedelta.match(spec)
        if m is None:
            raise ValueError(spec)

        g = m.groups()
        val = int(g[0])
        key = _timedelta_table.get(g[1])
        spec = timedelta(**{key: val})

    if isinstance(spec, timedelta):
        spec = datetime.utcnow() + spec

    return spec


class PQ(object):
    """Convenient queue manager."""

    table = 'queue'

    def __init__(self, *args, **kwargs):
        self.params = args, kwargs
        self.queues = WeakValueDictionary()

    def __getitem__(self, name):
        try:
            return self.queues[name]
        except KeyError:
            return self.queues.setdefault(
                name, Queue(name, *self.params[0], **self.params[1])
            )

    def close(self):
        self[''].close()

    def create(self):
        q = self['']
        conn = q._conn()
        sql = _read_sql('create')
        with transaction(conn) as cursor:
            cursor.execute(sql, {'name': Literal(q.table)})


class NotYet(Exception):
    """Raised when an item is not ready for work."""


class QueueIterator(object):
    """Returns a queue iterator.

    If the `timeout` attribute is set, then the iterator uses the
    smallest value of the time until next scheduled item is available
    and this setting.
    """

    timeout = None

    def __init__(self, queue):
        self.queue = queue

    def __iter__(self):
        return self

    def next(self):
        return self.queue.get(timeout=self.timeout)


class Queue(object):
    """Simple thread-safe transactional queue."""

    # This timeout is used during iteration. If the timeout elapses
    # and no item was pulled from the queue, the iteration loop
    # returns ``None``.
    timeout = 1

    # This setting uses the default cursor factory.
    cursor_factory = None

    logger = getLogger('pq')

    converters = {
        'pickle': (pickle.dumps, lambda data: pickle.loads(str(data))),
    }

    dumps = loads = staticmethod(lambda data: data)

    def __init__(self, name, conn=None, pool=None, table='queue', **kwargs):
        self.conn = conn
        self.pool = pool

        if '/' in name:
            name, key = name.rsplit('/', 1)
            self.dumps, self.loads = self.converters[key]

        self.name = name
        self.table = Literal(table)

        # Set additional options.
        self.__dict__.update(kwargs)

    def __iter__(self):
        return QueueIterator(self)

    def __len__(self):
        with self._transaction() as cursor:
            return self._count(cursor)

    def close(self):
        """Close the queue connection."""

        if self.conn is not None:
            self.conn.close()
        else:
            self.pool.closeall()

    def get(self, block=True, timeout=None):
        """Pull item from queue."""

        timeout = timeout or self.timeout

        while True:
            try:
                with self._transaction() as cursor:
                    data, ts = self._pull_item(cursor, block)
            except NotYet as e:
                data, ts = None, e.args[0]

            if data is not None:
                return self.loads(data)

            if not block:
                return

            if ts is None:
                ts = timeout
            else:
                ts = min(ts, timeout)

            if not self._select(ts):
                block = False

    def put(self, data, schedule_at=None, expected_at=None):
        """Put item into queue.

        If `schedule_at` is provided, the item is not dequeued until
        the provided time.

        The argument may be specified as one of the following:

        - a `datetime` object
        - a `timedelta` object
        - a string on the form '%d(s|m|h|d)`.

        In the last form, the unit indicated is seconds, minutes,
        hours and days, respectively.
        """

        schedule_at = _convert_time_spec(schedule_at)
        expected_at = _convert_time_spec(expected_at)

        with self._transaction() as cursor:
            return self._put_item(
                cursor, dumps(self.dumps(data)), schedule_at, expected_at
            )

    def clear(self):
        with self._transaction() as cursor:
            cursor.execute(
                "DELETE FROM %s WHERE q_name = %s",
                (self.table, self.name),
            )

    def _conn(self):
        if self.pool:
            ident = (current_thread().ident, self.name, self.table)
            return self.pool.getconn(ident)
        return self.conn

    def _listen(self, cursor):
        cursor.execute("LISTEN %s", (Literal(self.name), ))

    @prepared
    def _put_item(self, cursor):
        """Puts a single item into the queue.

            INSERT INTO %(table)s (q_name, data, schedule_at, expected_at)
            VALUES (%(name)s, $1, $2, $3) RETURNING id

        This method expects a string argument which is the item data
        and the scheduling timestamp.
        """

        return cursor.fetchone()[0]

    @prepared
    def _pull_item(self, cursor, blocking):
        """Return a single item from the queue.

        Priority is given to items that are ready for work and
        requested earliest in terms of their expected time.

        This method uses the following query:

            WITH item AS (
               SELECT id, data, schedule_at at time zone 'utc'
               FROM %(table)s
               WHERE q_name = %(name)s
                 AND dequeued_at IS NULL
                 AND pg_try_advisory_xact_lock(id)
               ORDER BY schedule_at nulls first, expected_at nulls first
               FOR UPDATE LIMIT 1
            )
            UPDATE %(table)s SET dequeued_at = current_timestamp
            WHERE id = (SELECT id FROM item)
            RETURNING data, schedule_at

        If `blocking` is set, the item blocks until an item is ready
        or the timeout has been reached.

        """

        row = cursor.fetchone()
        if row is None:
            if blocking:
                self._listen(cursor)
            return None, None

        schedule_at = row[1]
        if schedule_at is not None:
            d = schedule_at.replace(tzinfo=None) - datetime.utcnow()
            s = d.total_seconds()
            if s >= 0:
                self.logger.debug("Next item ready in %d seconds.", s)
                raise NotYet(s)

        return row

    @prepared
    def _count(self, cursor):
        """Return number of items in queue.

            SELECT COUNT(*) FROM %(table)s
            WHERE q_name = %(name)s AND dequeued_at IS NULL

        """

        return cursor.fetchone()[0]

    def _select(self, timeout):
        r, w, x = select([self._conn()], [], [], timeout)
        has_data = bool(r or w or x)
        if not has_data:
            self.logger.debug("timeout (%.3f seconds)." % timeout)
        return has_data

    def _transaction(self):
        return transaction(self._conn(), cursor_factory=self.cursor_factory)
