import sqlite3
import json
import requests
import re


class TIDService:
    _grabTIDQuery = "SELECT * from Temporal WHERE file=? and substr(revision,0,13)=substr(?,0,13);"
    _grabChangesetQuery = "select * from changeset where file=? and substr(cid,0,13)=substr(?,0,13)"

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
        self.conn.execute('''
                CREATE TABLE Temporal
                (TID INTEGER PRIMARY KEY     AUTOINCREMENT,
                REVISION CHAR(12)		  NOT NULL,
                FILE TEXT,
                LINE INT,
                OPERATOR INTEGER,
                UNIQUE(REVISION,FILE,LINE,OPERATOR));
                ''')
        # Changeset and Revision are for telling which TIDs are from a Revision and which are from a Changeset
        # Also for date information and stuff
        self.conn.execute('''
        CREATE TABLE Changeset
        (cid CHAR(12) PRIMARY KEY,
        FILE TEXT               NOT NULL,
        LENGTH INTEGER          NOT NULL,
        DATE INTEGER            NOT NULL,
        CHILD CHAR(12)
        );
        ''')
        self.conn.execute('''
        CREATE TABLE Revision
        (REV CHAR(12),
        FILE TEXT,
        DATE INTEGER,
        CHILD CHAR(12),
        PRIMARY KEY(REV,FILE)
        );
        ''')
        print("Table created successfully")

    def _get_date(self, file, rev): #TODO make it fetch from Database
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-file/' + rev + file
        print(url)
        response = requests.get(url)
        if response.status_code == 404:
            raise Exception("Cannot find date")
        mozobj = json.loads(response.text)
        return mozobj['date'][0]


    def grab_tids(self,file,revision):
        date = self._get_date(file,revision)
        # TODO make it grab the max
        cursor = self.conn.execute("select * from revision where date<=? and file=?", (date, file,))
        old_rev = cursor.fetchall()
        if old_rev == [] or old_rev[0][0] == revision:
            return self._grab_revision(file,revision)

        old_rev_id = old_rev[0][0]
        current_changeset = old_rev[0][3] # Grab child
        current_date = old_rev[0][2]
        old_rev = self._grab_revision(file,old_rev_id)
        cs_list = []
        while True:
            cursor = self.conn.execute(self._grabChangesetQuery, (file, current_changeset,))
            change_set = cursor.fetchall()
            if change_set == []:
                url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + current_changeset + file
                print(url)
                response = requests.get(url)
                mozobj = json.loads(response.text)
                self._make_tids_from_diff(mozobj)
                cursor = self.conn.execute(self._grabTIDQuery, (file, current_changeset))
                cs_list = cursor.fetchall()
                current_changeset = mozobj['children']
                if current_changeset != []:
                    current_changeset = current_changeset[0][:12]
                current_date = mozobj['date'][0]
            else:
                cursor = self.conn.execute(self._grabTIDQuery,(file,current_changeset))
                cs_list = cursor.fetchall()
                current_changeset = change_set[0][4]
                current_date = change_set[0][3]
            if current_date > date:
                break
            old_rev = self._add_changeset_to_rev(self,old_rev,cs_list)
        return old_rev


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

    def _grab_revision(self, file, revision): # TODO cache in DB
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-annotate/' + revision + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        date = mozobj['date'][0]
        child = mozobj['children']
        if child != []:
            child = child[0][:12]
        else:
            child = None
        tid_list = []
        for el in mozobj['annotate']:
            try:
                self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (?,?,?,?);",(el['node'][:12], file, el['targetline'], '1',))
            except sqlite3.IntegrityError:
                pass
            cursor = self.conn.execute("select * from Temporal where REVISION=? AND FILE=? AND LINE=?",(el['node'][:12],file,el['targetline'],))
            res = cursor.fetchone()
            tid_list.append(res)
        try:
            self.conn.execute("INSERT into REVISION (REV,FILE,DATE,CHILD) values (substr(?,0,13),?,?,?);", (revision, file, date,child,))
        except sqlite3.IntegrityError:
            pass
        self.conn.commit()
        return tid_list

    def _grab_changeset(self, file, cid):
        cursor = self.conn.execute(self._grabChangesetQuery,(file,cid,))
        cids = cursor.fetchall()
        if len(cids)==0:
            self._make_tids_from_changeset(file, cid)
        cursor=self.conn.execute(self._grabTIDQuery,(file,cid,))
        return cursor.fetchall()


    def _make_tids_from_changeset(self, file, cid):
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + cid + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        self._make_tids_from_diff(mozobj)

    def _make_tids_from_diff(self, diff):
        mozobj = diff
        if mozobj['diff'] is []:
            return None
        cid = mozobj['node'][:12]
        file = "/"+mozobj['path']
        minus_count = 0
        current_line = -1    # skip the first two lines
        length = len(mozobj['diff'][0]['lines'])
        date = mozobj['date'][0]
        child = mozobj['children']
        if child != []:
            child = child[0][:12]
        else:
            child = None
        self.conn.execute("INSERT into CHANGESET (CID,FILE,LENGTH,DATE,CHILD) values "
                          "(substr(?,0,13),?,?,?,?)",(cid,file,length,date,child,))
        for line in mozobj['diff'][0]['lines']:
            if current_line>0:
                if line['t'] == '-':
                    minus_count -= 1
                elif minus_count<0:
                    try:
                        self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values "
                                          "(substr(?,0,13),?,?,?);", (cid, file, current_line, minus_count,))
                    except sqlite3.IntegrityError:
                        print("Already exists")
                    minus_count=0
                if line['t'] == '@':
                    m=re.search('(?<=\+)\d+',line['l'])
                    current_line= int(m.group(0)) - 2
                    minus_count=0
                if line['t'] == '+':
                    try:
                        self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values "
                                          "(substr(?,0,13),?,?,?);", (cid, file, current_line, 1,))
                    except sqlite3.IntegrityError:
                        print("Already exists")
                    current_line += 1
                if line['t'] == '':
                    current_line += 1
            else:
                current_line += 1

        self.conn.commit()




