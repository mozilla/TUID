import sqlite3
import json
from urllib.request import urlopen
import re



class TIDService:
    _grabTIDQuery = "SELECT * from Temporal WHERE file=? and substr(revision,0,12)=substr(?,0,12);"
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
        #Operator is 1 to add a line, negative to delete specified lines
        self.conn.execute('''CREATE TABLE Temporal
                 (TID INTEGER PRIMARY KEY     AUTOINCREMENT,
                 REVISION CHAR(12)		  NOT NULL,
                 FILE TEXT,
        		 LINE INT,
        		 OPERATOR INTEGER,
        		 UNIQUE(REVISION,FILE,LINE,OPERATOR));''')
        self.conn.execute('''CREATE TABLE Changeset
        (cid CHAR(12) PRIMARY KEY,
        LENGTH INTEGER          NOT NULL,
        DATE INTEGER            NOT NULL
        );
        ''')
        self.conn.execute('''CREATE TABLE Revision
        (REV CHAR(12),
        FILE TEXT,
        DATE INTEGER,
        PRIMARY KEY(REV,FILE)
        );
        ''')
        print("Table created successfully");

    #def _getDate(self,file,rev):
    #    cursor = self.conn.execute("select rev,date from revision UNION select cid,date from changeset;")

    def _getRevBeforeDate(self,file,date):
        cursor = self.conn.execute("select * from revision where date<=?",(date,)) #TODO make it grab the max
        return cursor.fetchall();

    def _changesetsBetween(self,file,newcs,oldcs):
        url = "https://hg.mozilla.org/mozilla-central/json-log/"+newcs+file+"?revcount=50"; #adjust this number to optimize
        print(url)
        response = urlopen(url)
        mozobj = json.load(response)
        mozobj = mozobj['entries']
        length = len(mozobj)
        changesets = []
        found = False
        for i in range(length-1,-1,-1):
            if not found and mozobj[i]['node'][:12]==oldcs[:12]:
                found=True
            if found:
                changesets.append(mozobj[i]['node'][:12])
        if not found:
            return None
        return changesets

    def _addChangesetToRev(self,revision,cset):
        for set in cset:
            if set[4]==1:
                revision.insert(set[3],set) #Inserting and deleting will probably be slow
            if set[4]<0:
                del revision[set[3]:set[3]+abs(set[4])]
        return revision

    def grabTID(self,ID):
        cursor = self.conn.execute("SELECT * from Temporal WHERE TID=? LIMIT 1;",(ID,))
        return cursor.fetchone()

    #def _addCSetsToRevision(self,file,startrev,endrev):
     #


    def _grabRevision(self,file,revision):  #Possibly useless
        cursor = self.conn.execute(self._grabTIDQuery, (file, revision,))
        list = cursor.fetchall()
        if  len(list)>0:
            return list
        else:
            self._makeTIDsFromRevision(file, revision)
            cursor = self.conn.execute(self._grabTIDQuery, (file, revision,))
            return cursor.fetchall()

    def _makeTIDsFromRevision(self,file,revision):
        print(('https://hg.mozilla.org/mozilla-central/json-file/' + revision) + file)
        response = urlopen('https://hg.mozilla.org/mozilla-central/json-file/' + revision + file)
        mozobj = json.load(response)
        rev = mozobj['node']
        date = mozobj['date'][0]
        length = len(mozobj['lines'])
        for i in range(1,length+1):
            self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (substr(?,0,12),?,?,?);",(rev,file,str(i),'1',))
        self.conn.execute("INSERT into REVISION (REV,FILE,DATE) values (substr(?,0,12),?,?);",(rev,file,date,))
        self.conn.commit()

    def _makeTIDsFromChangeset(self, file, cid):
        url = 'https://hg.mozilla.org/mozilla-central/json-diff/' + cid + file
        print(url)
        response = urlopen(url)
        mozobj = json.load(response)
        minuscount = 0
        curline = -1    #skip the first two lines
        for line in mozobj['diff'][0]['lines']:
            if curline>0:
                if line['t']=='-':
                    minuscount-=1
                elif minuscount<0:
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (substr(?,0,12),?,?,?);",(cid, file, curline, minuscount,))
                    minuscount=0
                if line['t']=='@':
                    m=re.search('(?<=\+)\d+',line['l'])
                    curline=int(m.group(0))-1
                    minuscount=0
                if line['t']=='+':
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (substr(?,0,12),?,?,?);",(cid,file,curline,1,))
                    curline += 1
                if line['t']=='':
                    curline+=1
            else:
                curline+=1

        self.conn.commit()




