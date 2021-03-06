#!/usr/bin/env python
# -*- coding: utf-8 -*-

# a thread-based and simpler version of multiprocessing.Pool
# ref: https://docs.python.org/2/library/multiprocessing.html#module-multiprocessing.pool

from threading import Thread
from Queue import Queue
from mrbus.util import debug

class _Worker(Thread):

    daemon = True

    def __init__(self, task_que, result_que):
        Thread.__init__(self)
        self._task_que = task_que
        self._result_que = result_que

    def run(self):

        while True:

            no, func, args, argd = self._task_que.get()

            try:
                retval = func(*(args or ()), **(argd or {}))
            except BaseException as e:
                retval = e
                debug('{}: {} from {} {} {}'.format(
                    e.__class__.__name__,
                    e,
                    func,
                    args,
                    argd
                ))

            self._task_que.task_done()

            if no is not None:
                self._result_que.put((no, retval))

class Pool(object):

    # NOTE: not thread-safe

    def __init__(self, n=3):

        self._task_que = Queue()
        self._result_que = Queue()
        self._next_no = 0

        for i in range(n):
            worker = _Worker(self._task_que, self._result_que)
            worker.start()

    def _get_next_no(self):
        no = self._next_no
        self._next_no += 1
        return no

    def join(self):
        self._task_que.join()

    def apply_async(self, func, args=None, kwds=None):

        # here is no multiprocessing.pool.AsyncResult
        self._task_que.put((
            None,
            func,
            args,
            kwds
        ))

    def map(self, func, iterable):

        # XXX: it seems nobody uses it

        # dispatch tasks

        for item in iterable:
            self._task_que.put((
                self._get_next_no(),
                func,
                (item, ),
                None
            ))

        self.join()

        # collect the results

        no_result_pairs = []
        while not self._result_que.empty():
            no, retval = self._result_que.get_nowait()
            if isinstance(retval, BaseException):
                raise retval
            no_result_pairs.append((no, retval))

        no_result_pairs.sort()

        return [result for _, result in no_result_pairs]

if __name__ == '__main__':

    from time import sleep

    def do_task(n):
        sleep(0.1)
        return n

    def make_exc(n):
        [][0]

    pool = Pool()

    print pool.map(make_exc, range(9))

    import sys; sys.exit()

    for i in range(9):
        pool.apply_async(do_task, (i, ))

    pool.join()

    import sys; sys.exit()

    print pool.map(do_task, range(9))
