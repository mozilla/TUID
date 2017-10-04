import sqlite3
import json
from urllib.request import urlopen

class TIDService:
    def __init__(self,conn=None): #pass in conn for testing purposes
        try:
            f=open('config.json', 'r',encoding='utf8')
        except Exception:
            print("Config file not found")
            exit(-1)
        config = json.load(f)
        if conn is None:
            try:
                self.conn = sqlite3.connect(config['database']['name'])
            except Exception:
                print("Could not connect to database")
                exit(-1)

        else:
            self.conn = conn
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        if cursor.fetchone() is None:
            self.initDB()
        f.close();


    def initDB(self):
        self.conn.execute('''CREATE TABLE Temporal
                 (TID INTEGER PRIMARY KEY     AUTOINCREMENT,
                 REVISION CHAR(40)		  NOT NULL,
                 FILE TEXT,
        		 LINE INT,
        		 UNIQUE(REVISION,FILE,LINE));''')
        self.conn.execute('''CREATE TABLE Changesets
        		 (
        		 REVISION CHAR(40) PRIMARY KEY,
        		 LENGTH INTEGER
        		 );
        ''')
        print("Table created successfully");

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