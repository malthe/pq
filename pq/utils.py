import re

from contextlib import contextmanager
from functools import wraps
from textwrap import dedent
from logging import getLogger

logger = getLogger("pq")

_re_format = re.compile(r'%\(([a-z]+)\)[a-z]')


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
    query = "\n".join(
        line for line in d.split('\n')
        if line.startswith("    ")
    ) or d

    arg_count = query.count("$")
    if arg_count:
        fargs = "(" + ", ".join(["%s"] * arg_count) + ")"
    else:
        fargs = None

    fname = f.__name__
    for m in _re_format.finditer(query):
        fname += "_%%(%s)s" % m.group(1)

    @wraps(f)
    def wrapper(self, cursor, *args):
        conn = cursor.connection
        name = fname % self.__dict__
        key = "_prepared_%s" % name

        if not hasattr(conn, key):
            setattr(conn, key, None)
            d = self.__dict__.copy()
            cursor.execute("PREPARE %s AS\n%s" % (name, query), d)

        params = args[:arg_count]
        statement = "EXECUTE " + name
        if fargs is not None:
            statement += " " + fargs
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
