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

import sqlite3

from mo_dots import Data, coalesce
from mo_files import File
from mo_kwargs import override
from mo_logs import Log
from pyLibrary import convert
from pyLibrary.sql.sqlite import quote_value

DEBUG = False
TRACE = True


class Sql:
    def __init__(self, config):
        self.db = Sqlite(config)

    def execute(self, sql, params=None):
        if params:
            for p in params:
                sql = sql.replace('?', quote_value(p), 1)
        return self.db.execute(sql)

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.execute("ROLLBACK")

    def get(self, sql, params=None):
        if params:
            for p in params:
                sql = sql.replace('?', quote_value(p), 1)
        return self.db.query(sql).data

    def get_one(self, sql, params=None):
        return self.get(sql, params)[0]

    def transaction(self):
        return Transaction(self)


class Transaction(object):
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.execute("BEGIN")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_val, Exception):
            self.db.rollback()
        else:
            self.db.commit()

    def execute(self, sql, params=None):
        return self.db.execute(sql, params)

    def get(self, sql, params=None):
        return self.db.get(sql, params)

    def get_one(self, sql, params=None):
        return self.db.getone(sql, params)


class Sqlite(object):
    """
    Allows multi-threaded access
    Loads extension functions (like SQRT)
    """

    @override
    def __init__(self, filename=None):
        self.filename = File(filename).abspath
        self.db = sqlite3.connect(coalesce(self.filename, ':memory:'), check_same_thread=True)
        self.db.isolation_level = None

        if DEBUG:
            Log.note("Sqlite version {{version}}", version=self.query("select sqlite_version()").data[0][0])

    def query(self, command):
        curr = self.db.execute(command)
        data = curr.fetchall()
        if DEBUG and data:
            text = convert.table2csv(list(data))
            Log.note("Result:\n{{data}}", data=text)
        return Data(
            meta={"format": "table"},
            header=[d[0] for d in curr.description] if curr.description else None,
            data=data
        )

    def execute(self, command):
        self.db.execute(command)

    def commit(self):
        return self.db.commit()

    def rollback(self):
        self.db.rollback()

    def close(self):
        self.db.commit()
        self.db.close()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

