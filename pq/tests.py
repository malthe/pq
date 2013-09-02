import sys

from itertools import chain
from time import time, sleep
from unittest import TestCase
from logging import getLogger, StreamHandler, INFO
from threading import Event, Thread, current_thread
from psycopg2cffi.pool import ThreadedConnectionPool
from psycopg2cffi import ProgrammingError

# Set up logging such that we can quickly enable logging for a
# particular queue instance (via the `logging` flag).
getLogger('pq').setLevel(INFO)
getLogger('pq').addHandler(StreamHandler())


class QueueTest(TestCase):
    @classmethod
    def setUpClass(cls):
        pool = ThreadedConnectionPool(
            10, 100, "dbname=pq_test user=postgres"
        )
        from pq import Manager
        cls.manager = Manager(pool=pool, table="queue")

        try:
            cls.manager.install()
        except ProgrammingError:
            pass

    @classmethod
    def tearDownClass(cls):
        cls.manager.close()

    def setUp(self):
        self.queues = []

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

    def test_benchmark(self, items=4000, c=4):
        queue = self.make_one("benchmark")

        event = Event()

        def consumer(consumed):
            event.wait()
            for item in queue:
                if item is None:
                    break
                consumed.append(item)

        def producer():
            event.wait()
            i = 0
            while True:
                queue.put({})
                i += 1
                if i > items / c:
                    break

        cs = [[] for i in xrange(c * 2)]

        threads = (
            [Thread(target=producer) for i in xrange(c)],
            [Thread(target=consumer, args=(cs[i], )) for i in xrange(c * 2)],
        )

        for thread in chain(*threads):
            thread.start()

        event.set()
        t = time()

        for thread in chain(*threads):
            thread.join()

        elapsed = time() - t
        consumed = sum(map(len, cs))

        sys.stderr.write("complete.\n>>> stats: %s ... " % ("; ".join(
            "%s: %s" % item for item in (
                ("consumed", consumed),
                ("remaining", len(queue)),
                ("throughput", "%.1f items / sec" % (consumed / elapsed)),
            ))))

    def test_producer_consumer_threaded(self, c=5):
        queue = self.make_one("test")

        def producer():
            ident = current_thread().ident
            for i in xrange(100):
                queue.put((ident, i))

        consumed = []

        def consumer():
            while True:
                for d in queue:
                    if d is None:
                        return

                    consumed.append(tuple(d))

        producers = [Thread(target=producer) for i in xrange(c * 2)]
        consumers = [Thread(target=consumer) for i in xrange(c)]

        for t in producers:
            t.start()

        for t in consumers:
            t.start()

        for t in producers:
            t.join()

        for t in consumers:
            t.join()

        self.assertEqual(len(consumed), len(set(consumed)))
