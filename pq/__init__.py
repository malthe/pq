import os

from select import select
from threading import current_thread
from logging import getLogger
from weakref import WeakValueDictionary

from json import dumps

from .utils import Literal, LoggingCursor, transaction


def _read_sql(name, path=os.path.dirname(__file__)):
    return open(os.path.join(path, '%s.sql' % name), 'r').read()


class Manager(object):
    """Convenient queue manager."""

    table = 'queue'

    def __init__(self, *args, **kwargs):
        self.table = kwargs.get('table')
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
        conn = self[None].close()

    def install(self, table='queue'):
        conn = self[None]._conn()
        sql = _read_sql('create')
        with transaction(conn) as cursor:
            cursor.execute(sql, {'name': Literal(table)})


class Queue(object):
    """Simple thread-safe transactional queue."""

    # This timeout is used during iteration. If the timeout elapses
    # and no item was pulled from the queue, the iteration loop
    # returns ``None``.
    timeout = 1

    # This setting uses the default cursor factory.
    cursor_factory = None

    logger = getLogger('pq')

    def __init__(self, name, conn=None, pool=None, table='queue', debug=False):
        self.conn = conn
        self.pool = pool
        self.name = name
        self.table = Literal(table)

        if debug:
            self.cursor_factory = LoggingCursor

    def __iter__(self):
        while True:
            with self._transaction() as cursor:
                data = self._pull_row(cursor, True)

            if data is None and self._select(self.timeout):
                continue

            yield data

    def __len__(self):
        with self._transaction() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM %s WHERE q_name = %s AND dequeued IS NULL",
                (self.table, self.name)
            )

            return cursor.fetchone()[0]

    def close(self):
        if self.conn is not None:
            self.conn.close()
        else:
            self.pool.closeall()

    def get(self, block=True):
        while True:
            with self._transaction() as cursor:
                data = self._pull_row(cursor, block)

            if data is not None or not block:
                return data

            self._select(self.timeout)

        return data

    def put(self, data):
        with self._transaction() as cursor:
            cursor.execute(
                "INSERT INTO %s (q_name, data) VALUES (%s, %s) RETURNING id",
                (self.table, self.name, dumps(data))
            )

            return cursor.fetchone()[0]

    def clear(self):
        with self._transaction() as cursor:
            cursor.execute(
                "DELETE FROM %s WHERE q_name = %s",
                (self.table, self.name),
            )

    def _conn(self):
        if self.pool:
            ident = current_thread().ident
            return self.pool.getconn(ident)
        return self.conn

    def _cursor(self):
        return self._conn().cursor(cursor_factory=self.cursor_factory)

    def _listen(self, cursor):
        cursor.execute("LISTEN %s", (Literal(self.name), ))

    def _pull_row(self, cursor, blocking):
        cursor.execute(
            "SELECT id, data FROM %s WHERE q_name = %s "
            "AND dequeued IS NULL "
            "AND pg_try_advisory_xact_lock(id) FOR UPDATE LIMIT 1;",
            (self.table, self.name)
        )

        row = cursor.fetchone()
        if row is None:
            if blocking:
                self._listen(cursor)
            return

        cursor.execute(
            "UPDATE %s SET dequeued = current_timestamp WHERE id = %s",
            (self.table, row[0])
        )

        return row[1]

    def _select(self, timeout):
        r, w, x = select([self._conn()], [], [], timeout)
        has_data = bool(r or w or x)
        if not has_data:
            self.logger.debug("timeout (%.3f seconds)." % timeout)
        return has_data

    def _transaction(self):
        return transaction(self._conn(), cursor_factory=self.cursor_factory)
