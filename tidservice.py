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
            if not conn:
                self.conn = sqlite3.connect(self.config['database']['name'])
            else:
                self.conn = conn
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            if not cursor.fetchone():
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
        TID INTEGER,
        LINE INTEGER,
        PRIMARY KEY(REV,FILE,LINE)
        );
        ''')
        print("Table created successfully")

    def grab_tids(self,file,revision):
        # Grabs date
        cursor = self.conn.execute("select date from (select cid,file,date from changeset union "
                                   "select rev,file,date from revision) where cid=? and file=?;",(revision,file,))
        date_list = cursor.fetchall()
        if date_list:
            date = date_list[0][0]
        else:
            url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-file/' + revision + file
            print(url)
            response = requests.get(url)
            if response.status_code == 404:
                return ()
            mozobj = json.loads(response.text)
            date = mozobj['date'][0]
        # End Grab Date

        # TODO make it grab the max
        cursor = self.conn.execute("select * from revision where date<=? and file=?", (date, file,))
        old_rev = cursor.fetchall()
        if not old_rev or old_rev[0][0] == revision:
            return self._grab_revision(file,revision)
        old_rev_id = old_rev[0][0]
        current_changeset = old_rev[0][3] # Grab child
        current_date = old_rev[0][2]
        old_rev = self._grab_revision(file,old_rev_id)
        cs_list = []
        while True:
            cursor = self.conn.execute(self._grabChangesetQuery, (file, current_changeset,))
            change_set = cursor.fetchall()
            if not current_changeset:
                return old_rev
            if not change_set:
                url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + current_changeset + file
                print(url)
                response = requests.get(url)
                mozobj = json.loads(response.text)
                self._make_tids_from_diff(mozobj)
                cursor = self.conn.execute(self._grabTIDQuery, (file, current_changeset))
                cs_list = cursor.fetchall()
                current_changeset = mozobj['children']
                if current_changeset:
                    current_changeset = current_changeset[0][:12]
                current_date = mozobj['date'][0]
            else:
                cursor = self.conn.execute(self._grabTIDQuery, (file,current_changeset))
                cs_list = cursor.fetchall()
                current_changeset = change_set[0][4]
                current_date = change_set[0][3]
            if current_date > date:
                break
            old_rev = self._add_changeset_to_rev(self,old_rev,cs_list)
        return old_rev

    @staticmethod
    def _add_changeset_to_rev(self, revision, cset): # Single use
        for set in cset:
            if set[4]==1:
                revision.insert(set[3],set) # Inserting and deleting will probably be slow
            if set[4]<0:
                del revision[set[3]:set[3]+abs(set[4])]
        return revision


    def _grab_revision(self, file, revision):
        cursor = self.conn.execute("select t.tid,t.revision,t.file,t.line,t.operator from temporal t, revision r where "
                                   "t.tid=r.tid and r.file=? and r.rev=? order by r.line;", (file, revision[:12],))
        res = cursor.fetchall()
        if res:
            return res
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-annotate/' + revision + file
        print(url)
        response = requests.get(url)
        mozobj = json.loads(response.text)
        date = mozobj['date'][0]
        child = mozobj['children']
        if child:
            child = child[0][:12]
        else:
            child = None
        count = 1
        for el in mozobj['annotate']:
            try:
                self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values (?,?,?,?);",
                                  (el['node'][:12], file, el['targetline'], '1',))
            except sqlite3.IntegrityError:
                pass
            cursor = self.conn.execute("select * from Temporal where REVISION=? AND FILE=? AND LINE=?",(el['node'][:12],file,el['targetline'],))
            res2 = cursor.fetchone()
            try:
                self.conn.execute("INSERT into REVISION (REV,FILE,DATE,CHILD,TID,LINE) values (substr(?,0,13),?,?,?,?,?);",
                                  (revision, file, date, child,res2[0],count,))
            except sqlite3.IntegrityError:
                pass
            count+=1

        self.conn.commit()
        cursor = self.conn.execute("select t.tid,t.revision,t.file,t.line,t.operator from temporal t, revision r where "
                                   "t.tid=r.tid and r.file=? and r.rev=? order by r.line;", (file, revision[:12],))
        return cursor.fetchall()

    def _make_tids_from_diff(self, diff): # Single use
        mozobj = diff
        if not mozobj['diff']:
            return None
        cid = mozobj['node'][:12]
        file = "/"+mozobj['path']
        minus_count = 0
        current_line = -1    # skip the first two lines
        length = len(mozobj['diff'][0]['lines'])
        date = mozobj['date'][0]
        child = mozobj['children']
        if child:
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