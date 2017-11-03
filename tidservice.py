import sqlite3
import json
import requests
import re

class TIDService:
    _grabTIDQuery = "SELECT * from Temporal WHERE file=? and substr(revision,0,13)=substr(?,0,13);"
    _grabChangesetQuery = "select cid from changeset where file=? and substr(cid,0,13)=substr(?,0,13)"

    def __init__(self,conn=None): #pass in conn for testing purposes
        try:
            with open('config.json', 'r') as f:
                self.config = json.load(f, encoding='utf8')
            if conn is None:
                self.conn = sqlite3.connect(self.config['database']['name'])
            else:
                self.conn = conn
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            if cursor.fetchone() is None:
                self.init_db()
        except Exception as e:
            raise Exception("can not setup service") from e

    def init_db(self):
        # Operator is 1 to add a line, negative to delete specified lines
        self.conn.execute('''CREATE TABLE Temporal
                (TID INTEGER PRIMARY KEY     AUTOINCREMENT,
                REVISION CHAR(12)		  NOT NULL,
                FILE TEXT,
                LINE INT,
                OPERATOR INTEGER,
                UNIQUE(REVISION,FILE,LINE,OPERATOR));''')
        # Changeset and Revision are for telling which TIDs are from a Revision and which are from a Changeset
        # Also for date information and stuff
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

    def _get_date(self, file, rev): #TODO make it fetch from Database
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-file/' + rev + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        return mozobj['date'][0]


    # def grab_tids(self,file,revision):
    #     date = self._get_date(file,revision)
    #     rev = self._get_rev_before_date(file,date)
    #     if len(rev) == 0:
    #         self._make_tids_from_revision(file,revision)
    #         cursor = self.conn.execute(self._grabTIDQuery,(file,revision,))
    #         fetch = cursor.fetchall()
    #         return fetch
    #     result = self._apply_changesets_to_rev(file,revision,rev[0][0])
    #     if result == None:
    #         return rev
    #     return result

    def grab_tids(self, file, revision):
        return self._grab_revision(file, revision)

    def _get_rev_before_date(self, file, date):
        cursor = self.conn.execute("select * from revision where date<=? and file=?",(date,file,))
        # TODO make it grab the max
        return cursor.fetchall()

    def _apply_changesets_to_rev(self, file, newrev, oldrev):
        rev = self._grab_revision(file, oldrev)
        if oldrev == newrev:
            return rev
        changesets = self._changesets_between(file, newrev, oldrev)
        if changesets is None:
            return None
        if changesets is []:
            return None
        for cs in changesets:
            cs_tids = self._grab_changeset(file, cs)
            rev = self._add_changeset_to_rev(rev, cs_tids)
        return rev

    def _changesets_between(self, file, newcs, oldcs): #only works with one branch
        changesets = []
        current_changeset = newcs
        while current_changeset != oldcs:
            changesets.append(current_changeset)
            url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + current_changeset + file
            print(url)
            response = requests.get(url)
            mozobj = json.loads(response.text)
            current_changeset = mozobj['parents'][0][:12]
        changesets.append(newcs)
        changesets.pop(0)
        changesets.reverse()
        return changesets

    @staticmethod
    def _add_changeset_to_rev(self, revision, cset):
        for set in cset:
            if set[4]==1:
                revision.insert(set[3],set) # Inserting and deleting will probably be slow
            if set[4]<0:
                del revision[set[3]:set[3]+abs(set[4])]
        return revision

    def grab_tid(self, ID):
        cursor = self.conn.execute("SELECT * from Temporal WHERE TID=? LIMIT 1;",(ID,))
        return cursor.fetchone()

    def _grab_revision(self, file, revision): # probably useless
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-annotate/' + revision + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        tid_list = []
        for el in mozobj['annotate']:
            try:
                self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (?,?,?,?);",(el['node'][:12], file, el['targetline'], '1',))
            except sqlite3.IntegrityError:
                print("Already exists")
            cursor = self.conn.execute("select * from Temporal where REVISION=? AND FILE=? AND LINE=?",(el['node'][:12],file,el['targetline'],))
            res = cursor.fetchone()
            tid_list.append(res)
        self.conn.commit()
        return tid_list

    def _grab_changeset(self, file, cid):
        cursor = self.conn.execute(self._grabChangesetQuery,(file,cid,))
        cids = cursor.fetchall()
        if len(cids)==0:
            self._make_tids_from_changeset(file, cid)
        cursor=self.conn.execute(self._grabTIDQuery,(file,cid,))
        return cursor.fetchall()

    def _make_tids_from_revision(self, file, revision):
        cursor = self.conn.execute(self._grabTIDQuery,(file,revision,))
        el = cursor.fetchone()
        if el is not None:
            return
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-file/' + revision + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        rev = mozobj['node']
        date = mozobj['date'][0]
        length = len(mozobj['lines'])
        for i in range(1,length+1):
            self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values "
                              "(substr(?,0,13),?,?,?);",(rev,file,str(i),'1',))
        self.conn.execute("INSERT into REVISION (REV,FILE,DATE) values (substr(?,0,13),?,?);",(rev,file,date,))
        self.conn.commit()

    def _make_tids_from_changeset(self, file, cid):
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + cid + file
        print(url)
        response = requests.get(url)
        self._make_tids_from_diff(response.text)

    def _make_tids_from_diff(self, diff):
        mozobj = json.loads(diff)
        if mozobj['diff'] is []:
            return None
        cid = mozobj['node'][:12]
        file = "/"+mozobj['path']
        minus_count = 0
        current_line = -1    # skip the first two lines
        length = len(mozobj['diff'][0]['lines'])
        date = mozobj['date'][0]
        self.conn.execute("INSERT into CHANGESET (CID,FILE,LENGTH,DATE) values "
                          "(substr(?,0,13),?,?,?)",(cid,file,length,date,))
        for line in mozobj['diff'][0]['lines']:
            if current_line>0:
                if line['t'] == '-':
                    minus_count -= 1
                elif minus_count<0:
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values "
                                      "(substr(?,0,13),?,?,?);", (cid, file, current_line, minus_count,))
                    minus_count=0
                if line['t'] == '@':
                    m=re.search('(?<=\+)\d+',line['l'])
                    current_line= int(m.group(0)) - 2
                    minus_count=0
                if line['t'] == '+':
                    self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values "
                                      "(substr(?,0,13),?,?,?);", (cid, file, current_line, 1,))
                    current_line += 1
                if line['t'] == '':
                    current_line += 1
            else:
                current_line += 1

        self.conn.commit()




