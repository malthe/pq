# -*- coding: utf-8 -*-
from logging import getLogger
from functools import wraps
from . import (
    PQ as BasePQ,
    Queue as BaseQueue,
)


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
    logger = getLogger('pq.tasks')

    def perform(queue, job):
        data = job.data
        return (
            queue.handler_registry
            [data['function']]
            (*data['args'], **data['kwargs'])
        )

    task = task

    def work(self, burst=False):
        """Starts processing jobs."""
        self.logger.info('`%s` starting to perform jobs' % self.name)

        for job in self:
            if job is None:
                if burst:
                    return

                continue

            try:
                self.perform(job)

            except Exception as e:
                self.logger.warning("Failed to perform job %r :" % job)
                self.logger.exception(e)


class PQ(BasePQ):
    queue_class = Queue
