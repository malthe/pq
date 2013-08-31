from time import time, sleep
from unittest import TestCase
from logging import getLogger, StreamHandler, DEBUG
from threading import Event, Thread, current_thread
from psycopg2cffi.pool import ThreadedConnectionPool

# Set up logging such that we can quickly enable logging for a
# particular queue instance (via the `logging` flag).
getLogger('pq').setLevel(DEBUG)
getLogger('pq').addHandler(StreamHandler())


class QueueTest(TestCase):
    def setUp(self):
        pool = ThreadedConnectionPool(
            10, 100, "dbname=pq_test user=postgres"
        )
        self.queues = []
        from pq import Manager
        self.manager = Manager(pool=pool, table="queue")
        self.manager.install()

    def tearDown(self):
        for queue in self.queues:
            queue.clear()

    def make_one(self, name):
        queue = self.manager[name]
        self.queues.append(queue)
        queue.clear()
        return queue

    def test_put_and_get(self):
        queue = self.make_one("test")
        queue.put({'foo': 'bar'})
        self.assertEqual(queue.get(), {'foo': 'bar'})

    def test_get_not_empty(self):
        queue = self.make_one("test")
        queue.put({'foo': 'bar'})
        t = time()
        self.assertEqual(queue.get(), {'foo': 'bar'})
        self.assertGreater(0.1, time() - t)

    def test_get_empty_thread_puts(self):
        queue = self.make_one("test")

        def target():
            sleep(0.2)
            queue.put({'foo': 'bar'})

        thread = Thread(target=target)
        thread.start()
        t = time()
        self.assertEqual(queue.get(), {'foo': 'bar'})
        self.assertGreater(time() - t, 0.1)
        thread.join()

    def test_get_empty_non_blocking_thread_puts(self):
        queue = self.make_one("test")

        def target():
            sleep(0.1)
            queue.put({'foo': 'bar'})

        thread = Thread(target=target)
        thread.start()
        t = time()
        self.assertEqual(queue.get(False), None, time() - t)
        sleep(0.2)
        self.assertEqual(queue.get(False), {'foo': 'bar'})
        thread.join()

    def test_iter(self):
        queue = self.make_one("test")
        data = {'foo': 'bar'}
        queue.put(data)

        def target():
            for i in xrange(5):
                sleep(0.2)
                queue.put(data)

        thread = Thread(target=target)
        thread.start()

        dt = []
        iterator = enumerate(queue)
        while True:
            t = time()
            i, received = next(iterator)
            self.assertEqual(data, received, (i, time() - t))
            dt.append(time() - t)
            if i == 5:
                break

        thread.join()

        self.assertGreater(0.1, dt[0])
        self.assertGreater(dt[1], 0.1)

    def test_length(self):
        queue = self.make_one("test")
        queue.put({'foo': 'bar'})
        self.assertEqual(len(queue), 1)
        queue.put({'foo': 'bar'})
        self.assertEqual(len(queue), 2)
        queue.get()
        self.assertEqual(len(queue), 1)

    def test_producer_consumer_threaded(self):
        queue = self.make_one("test")
        data = {'foo': 'bar'}
        event = Event()

        def producer():
            ident = current_thread().ident
            for i in xrange(100):
                sleep(0.01)
                d = dict(data)
                d['id'] = ident, i
                queue.put(d)

        consumed = []

        def consumer():
            while True:
                for d in queue:
                    consumed.append(
                        tuple(d['id']) if d is not None else None
                    )
                    if event.is_set():
                        return

        producers = [Thread(target=producer) for i in xrange(5)]
        consumers = [Thread(target=consumer) for i in xrange(5)]

        for t in producers:
            t.start()

        for t in consumers:
            t.start()

        for t in producers:
            t.join()

        event.set()

        for t in consumers:
            t.join()

        self.assertEqual(len(consumed), len(set(consumed)))
