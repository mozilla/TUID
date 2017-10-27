import sqlite3
import json
import requests
import re

class TIDService:
    _grabTIDQuery = "SELECT * from Temporal WHERE file=? and substr(revision,0,13)=substr(?,0,13);"
    _grabChangesetQuery = "select cid from changeset where file=? and substr(cid,0,13)=substr(?,0,13)"
    def __init__(self,conn=None): #pass in conn for testing purposes
        f=open('config.json', 'r',encoding='utf8')
        self.config = json.load(f)
        if conn is None:
            try:
                self.conn = sqlite3.connect(self.config['database']['name'])
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
        #Changeset and Revision are for telling which TIDs are from a Revision and which are from a Changeset
        #Also for date information and stuff
        self.conn.execute('''CREATE TABLE Changeset
        (cid CHAR(12) PRIMARY KEY,
        FILE TEXT               NOT NULL,
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
        print("Table created successfully")

    def _getDate(self,file,rev): #TODO make it fetch from Database
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-file/' + rev + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        return mozobj['date'][0]


    def grabTIDs(self,file,revision):
        date = self._getDate(file,revision)
        rev = self._getRevBeforeDate(file,date)
        if len(rev) == 0:
            self._makeTIDsFromRevision(file,revision)
            cursor = self.conn.execute(self._grabTIDQuery,(file,revision,))
            fetch = cursor.fetchall()
            return fetch
        result = self._applyChangesetsToRev(file,revision,rev[0][0])
        if result == None:
            return rev
        return result


    def _getRevBeforeDate(self,file,date):
        cursor = self.conn.execute("select * from revision where date<=? and file=?",(date,file,)) #TODO make it grab the max
        return cursor.fetchall()

    def _applyChangesetsToRev(self,file,newrev,oldrev):
        rev = self._grabRevision(file,oldrev)
        if oldrev == newrev:
            return rev
        changesets = self._changesetsBetween(file,newrev,oldrev)
        if changesets == None:
            return None
        if changesets == []:
            return None
        for cs in changesets:
            csTIDs = self._grabChangeset(file,cs)
            rev = self._addChangesetToRev(rev,csTIDs)
        return rev

    def _changesetsBetween(self,file,newcs,oldcs): #only works with one branch
        changesets = []
        currentChangeset = newcs
        while currentChangeset != oldcs:
            changesets.append(currentChangeset)
            url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + currentChangeset + file
            print(url)
            response = requests.get(url)
            mozobj = json.loads(response.text)
            currentChangeset = mozobj['parents'][0][:12]
        changesets.append(newcs)
        changesets.pop(0)
        changesets.reverse()
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


    def _grabRevision(self,file,revision): #probably useless
        cursor = self.conn.execute(self._grabTIDQuery, (file, revision,))
        list = cursor.fetchall()
        if  len(list)>0:
            return list
        else:
            self._makeTIDsFromRevision(file, revision)
            cursor = self.conn.execute(self._grabTIDQuery, (file, revision,))
            return cursor.fetchall()

    def _grabChangeset(self,file,cid):
        cursor = self.conn.execute(self._grabChangesetQuery,(file,cid,))
        cids = cursor.fetchall()
        if len(cids)==0:
            self._makeTIDsFromChangeset(file,cid)
        cursor=self.conn.execute(self._grabTIDQuery,(file,cid,))
        return cursor.fetchall()


    def _makeTIDsFromRevision(self,file,revision):
        cursor = self.conn.execute(self._grabTIDQuery,(file,revision,))
        el = cursor.fetchone()
        if el != None:
            return
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-file/' + revision + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        rev = mozobj['node']
        date = mozobj['date'][0]
        length = len(mozobj['lines'])
        for i in range(1,length+1):
            self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (substr(?,0,13),?,?,?);",(rev,file,str(i),'1',))
        self.conn.execute("INSERT into REVISION (REV,FILE,DATE) values (substr(?,0,13),?,?);",(rev,file,date,))
        self.conn.commit()

    def _makeTIDsFromChangeset(self,file,cid):
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + cid + file
        print(url)
        response = requests.get(url)
        self._makeTIDsFromDiff(response.text)
        

    def _makeTIDsFromDiff(self, diff):
        mozobj = json.loads(diff)
        if mozobj['diff']==[]:
            return None
        cid = mozobj['node'][:12]
        file = "/"+mozobj['path']
        minuscount = 0
        curline = -1    #skip the first two lines
        length = len(mozobj['diff'][0]['lines'])
        date = mozobj['date'][0]
        self.conn.execute("INSERT into CHANGESET (CID,FILE,LENGTH,DATE) values (substr(?,0,13),?,?,?)",(cid,file,length,date,))
        for line in mozobj['diff'][0]['lines']:
            if curline>0:
                if line['t']=='-':
                    minuscount-=1
                elif minuscount<0:
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (substr(?,0,13),?,?,?);",(cid, file, curline, minuscount,))
                    minuscount=0
                if line['t']=='@':
                    m=re.search('(?<=\+)\d+',line['l'])
                    curline=int(m.group(0))-2
                    minuscount=0
                if line['t']=='+':
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (substr(?,0,13),?,?,?);",(cid,file,curline,1,))
                    curline += 1
                if line['t']=='':
                    curline+=1
            else:
                curline+=1

        self.conn.commit()




