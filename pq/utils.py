from contextlib import contextmanager
from functools import wraps
from textwrap import dedent
from logging import getLogger

logger = getLogger("pq")


def prepared(f):
    """Decorator that prepares an SQL statement.

    The statement must be present in the function's docstring,
    indented with four spaces, i.e.

        SELECT * FROM %(name)s WHERE value > $1

    The formatting variables are pulled from the function bind's
    instance dictionary.

    The placeholders (with dollar sign) are positional arguments to
    the decorated function. These are automatically applied.
    """

    d = dedent(f.__doc__.split('\n', 1)[1])
    name = f.__name__
    query = "\n".join(
        line for line in d.split('\n')
        if line.startswith("    ")
    ) or d
    query = "PREPARE %(statement_name)s AS\n" + query
    arg_count = query.count("$")

    if arg_count:
        statement = "EXECUTE %s (%s)" % (
            name, ", ".join("%s" for i in range(arg_count)))
    else:
        statement = "EXECUTE %s" % name

    @wraps(f)
    def wrapper(self, cursor, *args):
        conn = cursor.connection
        key = "_prepared_%s" % name

        if not hasattr(conn, key):
            setattr(conn, key, None)
            d = self.__dict__.copy()
            d['statement_name'] = Literal(name)
            cursor.execute(query, d)

        params = args[:arg_count]
        cursor.execute(statement, params or None)
        return f(self, cursor, *args[arg_count:])

    return wrapper


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
        try:
            yield cursor
        finally:
            cursor.close()
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()

    for notice in cursor.connection.notices:
        logger.warn(notice)


class Literal(str):
    """String wrapper to make a query parameter literal."""

    def __conform__(self, quote):
        return self

    @classmethod
    def mro(cls):
        return (object, )

    def getquoted(self):
        return str(self)
