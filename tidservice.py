import sqlite3
import json
from urllib.request import urlopen
import re



class TIDService:
    __grabTIDQuery = "SELECT * from Temporal WHERE file=? and revision=?;"
    def __init__(self,conn=None): #pass in conn for testing purposes
        f=open('config.json', 'r',encoding='utf8')
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
        		 OPERATOR CHAR(1),
        		 UNIQUE(REVISION,FILE,LINE));''')
        self.conn.execute('''CREATE TABLE Changeset
        (cid CHAR(40) PRIMARY KEY,
        LENGTH INTEGER          NOT NULL,
        DATE INTEGER            NOT NULL
        );
        ''')
        print("Table created successfully");

    def grabTID(self,ID):
        cursor = self.conn.execute("SELECT * from Temporal WHERE TID=? LIMIT 1;",(ID,))
        return cursor.fetchone()

    def grabTIDs(self,file,revision):
        cursor = self.conn.execute(self.__grabTIDQuery,(file,revision,))
        list = cursor.fetchall()
        if  len(list)>0:
            return list
        else:
            self._makeTIDsFromRevision(file, revision)
            cursor = self.conn.execute(self.__grabTIDQuery,(file,revision,))
            return cursor.fetchall()

    def _makeTIDsFromRevision(self, file, revision):
        print(('https://hg.mozilla.org/mozilla-central/json-file/' + revision) + file)
        response = urlopen('https://hg.mozilla.org/mozilla-central/json-file/' + revision + file)
        mozobj = json.load(response)
        rev = mozobj['node']
        date = mozobj['date'][0]
        length = len(mozobj['lines'])
        for i in range(1,length+1):
            self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,DATE,OPERATOR) values (?,?,?,?,?);",(rev,file,str(i),date,'+',))
        self.conn.execute("INSERT into Changeset (CID,LENGTH,DATE) values (?,?,?);",(rev,length,date,))
        self.conn.commit()

    def _makeTIDsFromChangeset(self, file, cid):
        url = 'https://hg.mozilla.org/mozilla-central/json-diff/' + cid + file
        print(url)
        response = urlopen(url)
        mozobj = json.load(response)
        curline = -1
        for line in mozobj['diff'][0]['lines']:
            if curline>0:
                if line['t']=='@':
                    m=re.search('(?<=\+)\d+',line['l'])
                    curline=m.group(0)
                if line['t']=='+':
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,DATE,OPERATOR) values (?,?,?,?,?);",(cid,file,curline,0,'+',))

                if line['t']=='-':
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,DATE,OPERATOR) values (?,?,?,?,?);",(cid, file, curline, 0, '+',))
            curline += 1
        self.conn.commit()




