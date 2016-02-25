# -*- coding: utf-8 -*-
from logging import getLogger
from functools import wraps
from . import Queue as BaseQueue


def task(queue, *job_args, **job_kwargs):
    def decorator(f):
        f._path = "%s.%s" % (f.__module__, f.__name__)

        queue.handler_registry[f._path] = f

        @wraps(f)
        def wrapper(*args, **kwargs):
            queue.put(
                dict(
                    function=f._path,
                    args=args,
                    kwargs=kwargs,
                ),
                *job_args,
                **job_kwargs
            )

        return wrapper

    return decorator


class Queue(BaseQueue):
    handler_registry = dict()

    task = task


def perform(queue, job):
    data = job.data
    return (
        queue.handler_registry
        [data['function']]
        (*data['args'], **data['kwargs'])
    )


class Worker(object):
    """Worker that performed available jobs within a given queue.
    """
    logger = getLogger('pq.handlers')

    def __init__(self, queue, performer=perform):
        self.queue = queue
        self.performer = perform

    def work(self, burst=False):
        """Starts processing jobs."""
        queue = self.queue

        self.logger.info('Starting new worker for queue `%s`' % queue.name)

        for job in queue:
            if job is None:
                if burst:
                    return

                continue

            try:
                self.performer(queue, job)

            except Exception as e:
                self.logger.warning("Failed to perform job %r :" % job)
                self.logger.exception(e)
