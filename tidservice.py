import sqlite3
import json
from urllib.request import urlopen



class TIDService:
    __grabTIDQuery = "SELECT * from Temporal WHERE file=? and revision=?;"
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
        		 DATE INTEGER,
        		 UNIQUE(REVISION,FILE,LINE));''')
        print("Table created successfully");

    def grabTID(self,ID):
        cursor = self.conn.execute("SELECT * from Temporal WHERE TID=? LIMIT 1;",(ID,))
        return cursor.fetchone()

    def grabTIDs(self,file,revision):
        cursor = self.conn.execute(self.__grabTIDQuery,(file,revision,))
        first = cursor.fetchone()
        if  cursor.fetchone() is not None:
            return cursor
        else:
            self.makeTIDsFromWeb(file,revision)
            return self.conn.execute(self.__grabTIDQuery,(file,revision,))




    def makeTIDsFromWeb(self,file,revision):
        print(('https://hg.mozilla.org/mozilla-central/json-file/' + revision) + file)
        response = urlopen('https://hg.mozilla.org/mozilla-central/json-file/' + revision + file)
        mozobj = json.load(response)
        rev = mozobj['node']
        date = mozobj['date'][0]
        length = len(mozobj['lines'])
        for i in range(1,length):
            self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,DATE) values (?,?,?,?);",(rev,file,str(i),date,))
        self.conn.commit()