# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#

from __future__ import division
from __future__ import unicode_literals

from mo_logs import Log
from mo_threads import Lock


class Counter(object):
    """
    Use this class to count things (like threads) using `with` clause

    my_counter = Counter()

    with my_counter:
        # my_counter is incremented in this context
    """

    def __init__(self):
        self.locker = Lock()
        self.value = 0

    def __call__(self, num):
        """
        Sometimes you want your context to track more than one item

        my_counter = Counter()

        with my_counter(20):
            # my_counter is incremented by 20 in this context

        :param num:
        :return:
        """
        return ManyCounter(self, num)

    def __enter__(self):
        with self.locker:
            self.value += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.locker:
            self.value -= 1


class ManyCounter(object):
    """
    Not meant for external use
    """

    def __init__(self, parent, increment):
        self.parent = parent
        self.increment = increment

    def __enter__(self):
        with self.parent.locker:
            self.parent.value += self.increment
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.parent.locker:
            self.parent.value -= self.increment


class Semaphore(object):
    """
    Limit number of threads using this context manager
    """

    def __init__(self, max):
        """
        :param max: Maximum number of concurrent threads, all others will block
        """
        self.lock = Lock()
        self.max = max
        self.remaining = max

    def __call__(self, timeout):
        """
        my_limiter = Semaphore(3)

        with my_limiter(10):
            # Only three concurent threads allowed in this block
            # Other threads will wait up to 10sec before timeout

        :param timeout: Seconds to wait
        :return:  context manager for `with` clause
        """
        return SemaphoreContext(self, timeout)


class SemaphoreContext(object):
    """
    Not meant for external use
    """

    def __init__(self, parent, timeout):
        self.parent = parent
        self.timeout = timeout

    def __enter__(self):
        with self.parent.lock:
            while not self.timeout:
                if self.parent.remaining:
                    self.parent.remaining -= 1
                    return self
                self.parent.lock.wait(self.timeout)
        Log.error("Timeout")

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.parent.lock:
            self.parent.remaining += 1
