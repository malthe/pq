# -*- coding: utf-8 -*-
from functools import wraps


handler_registry = dict()


def handler(queue, *task_args, **task_kwargs):
    def decorator(f):
        f._path = "%s.%s" % (f.__module__, f.__name__)

        handler_registry[f._path] = f

        @wraps(f)
        def wrapper(*args, **kwargs):
            queue.put(
                dict(
                    function=f._path,
                    args=args,
                    kwargs=kwargs,
                ),
                *task_args,
                **task_kwargs
            )

        return wrapper

    return decorator


def perform(task):
    data = task.data
    return handler_registry[data['function']](*data['args'], **data['kwargs'])
