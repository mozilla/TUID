# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from pyLibrary.sql.sqlite import Sqlite, quote_value


class Sql:
    def __init__(self,dbname):
        self.db = Sqlite(dbname)

    def execute(self,sql,params=None):
        if params:
            for p in params:
                sql = sql.replace('?', quote_value(p), 1)
        return self.db.execute(sql)

    def commit(self):
        self.db.commit()

    def get(self,sql,params=None):
        if params:
            for p in params:
                sql = sql.replace('?', quote_value(p), 1)
        return self.db.query(sql).data

    def get_one(self,sql,params=None):
        return self.get(sql, params)[0]
