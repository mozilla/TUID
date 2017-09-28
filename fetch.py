import sqlite3
import json
from urllib.request import urlopen

class Fetch:
    def __init__(self,conn=None):
        f = open('config.json', 'r')
        if conn is None:
            try:
                config = json.load(f)
                self.conn = sqlite3.connect(config['database']['name'])
            except Exception as e:
                print(e)
                print("Failed to connect to DB")
        else:
            self.conn = conn

    def grabTID(self,ID):
        cursor = self.conn.execute("SELECT * from Temporal WHERE TID=? LIMIT 1;",(ID,))
        return cursor.fetchone()

    def grabTIDs(self,file,revision):
        cursor = self.conn.execute("SELECT * from Temporal WHERE file=? and revision=?;",(file,revision,))
        if cursor.arraysize != 0:
            return cursor
        else:
            return self.grabTIDsFromFile(file,revision)



    def addTIDsFromFile(self,file,revision):
        print(('https://hg.mozilla.org/mozilla-central/json-file/' + revision) + file)
        response = urlopen('https://hg.mozilla.org/mozilla-central/json-file/' + revision + file)
        mozobj = json.load(response)
        rev = mozobj['node']
        length = len(mozobj['lines'])
        for i in range(1,length):
            self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE) values (?,?,?);",(rev,file,str(i),))
        self.conn.commit()