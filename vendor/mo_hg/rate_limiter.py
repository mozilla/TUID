# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

from flask import Response

from mo_dots import coalesce
from mo_future import text_type, xrange
from mo_json import json2value, value2json
from mo_logs import Log
from mo_threads import Lock, Signal, Queue, Thread, Till
from mo_times import Date, SECOND, MINUTE
from pyLibrary.env import http
from pyLibrary.sql.sqlite import Sqlite, quote_value, quote_list

CONCURRENCY = 1
AMORTIZATION_PERIOD = SECOND
HG_REQUEST_PER_SECOND = 10
CACHE_RETENTION = 10 * MINUTE


class RateLimiter(object):
    """
    ...AND CACHE
    """

    def __init__(self, rate=None, amortization_period=None, db=None):
        self.amortization_period = coalesce(amortization_period.seconds, AMORTIZATION_PERIOD.seconds)
        self.rate = coalesce(rate, HG_REQUEST_PER_SECOND)
        self.cache_locker = Lock()
        self.cache = {}  # MAP FROM url TO (ready, headers, response, timestamp) PAIR
        self.workers = []
        self.todo = Queue("hg relay todo")
        self.requests = Queue("hg relay requests", max=self.rate * self.amortization_period)
        self.db = Sqlite(db)

        self.threads = [
            Thread.run("hg relay worker" + text_type(i), self._worker)
            for i in range(CONCURRENCY)
        ]
        self.limiter = Thread.run("hg relay limiter", self._rate_limiter)
        self.cleaner = Thread.run("hg relay cleaner", self._cache_cleaner)

    def _rate_limiter(self, please_stop):
        max_requests = self.requests.max
        recent_requests = []

        while not please_stop:
            now = Date.now()
            too_old = now - self.amortization_period

            recent_requests = [t for t in recent_requests if t > too_old]

            num_recent = len(recent_requests)
            if num_recent >= max_requests:
                space_free_at = recent_requests[0] + self.amortization_period
                (please_stop | Till(till=space_free_at.unix)).wait()
                continue
            for _ in xrange(num_recent, max_requests):
                request = self.todo.pop()
                recent_requests.append(Date.now())
                self.requests.add(request)

    def _cache_cleaner(self, please_stop):
        while not please_stop:
            now = Date.now()
            too_old = now-CACHE_RETENTION

            remove = set()
            with self.cache_locker:
                for url, (ready, headers, response, timestamp) in self.cache:
                    if timestamp < too_old:
                        remove.add(url)
                for r in remove:
                    del self.cache[r]
            (please_stop | Till(seconds=CACHE_RETENTION.seconds / 2)).wait()

    def no_cache(self, url):
        """
        :return: False if `url` is not to be cached
        """
        if url.endswith("/tip"):
            return True
        return False

    def request(self, method, url, headers):
        now = Date.now()
        ready = Signal(url)

        # TEST CACHE
        with self.cache_locker:
            pair = self.cache.get(url)
            if pair is None:
                self.cache[url] = (ready, None, None, now)

        if pair is not None:
            # REQUEST IS IN THE QUEUE ALREADY, WAIT
            ready, headers, response, then = pair
            if response is None:
                ready.wait()
                with self.cache_locker:
                    ready, headers, response, timestamp = self.cache.get(url)
            with self.db.transaction as t:
                t.execute("UPDATE cache SET timestamp=" + quote_value(now) + " WHERE url=" + quote_value(url))
            return Response(
                response,
                status=200,
                headers=json2value(headers)
            )

        # TEST DB
        db_response = self.db.query("SELECT headers, response FROM cache WHERE url=" + quote_value(url))
        if db_response:
            headers, response = db_response[0]
            with self.db.transaction as t:
                t.execute("UPDATE cache SET timestamp=" + quote_value(now) + " WHERE url=" + quote_value(url))
            with self.cache_locker:
                self.cache[url] = (ready, headers, response, now)
            ready.go()

            return Response(
                response,
                status=200,
                headers=json2value(headers)
            )

        # MAKE A NETWORK REQUEST
        self.todo.add((ready, method, url, headers, now))
        ready.wait()
        with self.cache_locker:
            ready, headers, response, timestamp = self.cache[url]
        return Response(
            response,
            status=200,
            headers=json2value(headers)
        )

    def _worker(self, todo, please_stop):
        while not please_stop:
            pair = self.requests.pop(till=please_stop)
            if please_stop:
                break
            ready, method, url, headers, timestamp = pair

            try:
                response = http.request(method, url, headers)
                with self.db.transaction as t:
                    t.execute("INSERT INTO cache (url, headers, response, timestamp) VALUES" + quote_list((url, headers, response, timestamp)))
                with self.cache_locker:
                    self.cache[url] = (ready, response.content, value2json(response.headers), timestamp)
            except Exception as e:
                Log.warning("problem with request to {{url}}", url=url, cause=e)
                with self.cache_locker:
                    ready, headers, response = self.cache[url]
                    del self.cache[url]
            finally:
                ready.go()
