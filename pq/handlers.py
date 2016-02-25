# -*- coding: utf-8 -*-
from logging import getLogger
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


class Worker(object):
    """Worker that performed available tasks within a given queue.
    """
    logger = getLogger('pq.handlers')

    def __init__(self, queue, performer=perform):
        self.queue = queue
        self.performer = perform

    def work(self, burst=False):
        """Starts processing tasks."""
        queue = self.queue

        self.logger.info('Starting new worker for queue `%s`' % queue.name)

        for task in queue:
            if task is None:
                if burst:
                    return

                continue

            try:
                self.performer(task)

            except Exception as e:
                self.logger.warning("Failed to perform task %r :" % task)
                self.logger.exception(e)
