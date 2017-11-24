import sqlite3
class Sql:
    def __init__(self,dbname):
        self.db = sqlite3.connect(dbname)

    def execute(self,sql):
        self.db.execute(sql)

    def commit(self):
        self.db.commit()

    def get(self,sql,params=None):
        if params:
            return self.db.execute(sql,params).fetchall()
        else:
            return self.db.execute(sql).fetchall()

    def get_one(self,sql,params=None):
        if params:
            return self.db.execute(sql,params).fetchone()
        else:
            return self.db.execute(sql).fetchone()

