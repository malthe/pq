from logging import getLogger
from psycopg2cffi.extensions import cursor
from contextlib import contextmanager


@contextmanager
def transaction(conn, **kwargs):
    """Context manager.
    Executes block inside DB transaction. Returns cursor.
    At the end of the block, the connection is returned to pool.

    >>> with transaction() as cursor:
    ...     rows = cursor.execute(...).fetchall()
    ...     process(rows)
    ...     cursor.execute(...)
    """
    cursor = conn.cursor(**kwargs)
    try:
        cursor.execute("begin")
        try:
            yield cursor
        except Exception:
            cursor.execute("rollback")
            raise
        else:
            cursor.execute("commit")
    finally:
        cursor.close()


class Literal(str):
    """String wrapper to make a query parameter literal."""

    def __conform__(self, quote):
        return self

    @classmethod
    def mro(cls):
        return (object, )

    def getquoted(self):
        return str(self)


class LoggingCursor(cursor):
    logger = getLogger('sql')

    def execute(self, sql, args=None):
        self.logger.info(self.mogrify(sql, args))

        try:
            cursor.execute(self, sql, args)
        except Exception, exc:
            self.logger.error("%s: %s" % (exc.__class__.__name__, exc))
            raise
