# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_logs import Log
from pyLibrary.sql.sqlite import quote_value, Sqlite

DEBUG = False
TRACE = True


class Sql:
    def __init__(self, config):
        self.db = Sqlite(config)

    def execute(self, sql, params=None):
        Log.error("Use a transaction")

    def commit(self):
        Log.error("Use a transaction")

    def rollback(self):
        Log.error("Use a transaction")

    def get(self, sql, params=None):
        if params:
            for p in params:
                sql = sql.replace('?', quote_value(p), 1)
        return self.db.query(sql).data

    def get_one(self, sql, params=None):
        return self.get(sql, params)[0]

    def transaction(self):
        return Transaction(self.db.transaction())

    @property
    def pending_transactions(self):
        """
        :return: NUMBER OF TRANSACTIONS IN THE QUEUE
        """
        return len(self.db.available_transactions)


class Transaction():
    def __init__(self, transaction):
        self.transaction = transaction

    def __enter__(self):
        self.transaction.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.transaction.__exit__(exc_type, exc_val, exc_tb)
        self.transaction = None

    def execute(self, sql, params=None):
        if params:
            for p in params:
                sql = sql.replace('?', quote_value(p), 1)
        return self.transaction.execute(sql)

    def get(self, sql, params=None):
        if params:
            for p in params:
                sql = sql.replace('?', quote_value(p), 1)
        return self.transaction.query(sql).data

    def get_one(self, sql, params=None):
        return self.get(sql, params)[0]

    def query(self, query):
        return self.transaction.query(query)

    def commit(self):
        Log.error("do not know how to handle")

    def rollback(self):
        Log.error("do not know how to handle")
