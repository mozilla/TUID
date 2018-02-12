# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.


import json
import re
import sql
import sqlite3
from log import Log
from web import Web

GRAB_TID_QUERY = "SELECT * from Temporal WHERE file=? and substr(revision,0,13)=substr(?,0,13);"
GRAB_CHANGESET_QUERY = "select * from changeset where file=? and substr(cid,0,13)=substr(?,0,13)"

class TIDService:
    def __init__(self,conn=None): #pass in conn for testing purposes
        try:
            with open('config.json', 'r') as f:
                self.config = json.load(f, encoding='utf8')
            if not conn:
                self.conn = sql.Sql(self.config['database']['name'])
            else:
                self.conn = conn
            if not self.conn.get_one("SELECT name FROM sqlite_master WHERE type='table';"):
                self.init_db()
        except Exception as e:
            raise Exception("can not setup service") from e

    def init_db(self):
        # Operator is 1 to add a line, negative to delete specified lines
        self.conn.execute('''
        CREATE TABLE Temporal (
            TID INTEGER PRIMARY KEY     AUTOINCREMENT,
            REVISION CHAR(12)		  NOT NULL,
            FILE TEXT,
            LINE INT,
            OPERATOR INTEGER,
            UNIQUE(REVISION,FILE,LINE,OPERATOR)
        );''')
        # Changeset and Revision are for telling which TIDs are from a Revision and which are from a Changeset
        # Also for date information and stuff
        self.conn.execute('''
        CREATE TABLE Changeset (
            CID CHAR(12),
            FILE TEXT,
            LENGTH INTEGER          NOT NULL,
            DATE INTEGER            NOT NULL,
            CHILD CHAR(12),
            PRIMARY KEY(CID,FILE)
        );''')
        self.conn.execute('''
        CREATE TABLE Revision (
            REV CHAR(12),
            FILE TEXT,
            DATE INTEGER,
            CHILD CHAR(12),
            TID INTEGER,
            LINE INTEGER,
            PRIMARY KEY(REV,FILE,LINE)
        );''')

        self.conn.execute('''
        CREATE TABLE DATES (
            CID CHAR(12),
            FILE TEXT,
            DATE INTEGER,
            PRIMARY KEY(CID,FILE)
        );''')

        Log.note("Table created successfully")

    def grab_tids_from_files(self,dir,files,revision):
        result = []
        total = len(files)
        count = 0
        for file in files:
            count+=1
            Log.note(file+" "+str(count/total)+"%")
            result.append((file,self.grab_tids(dir+file,revision)))
        return result

    def grab_tids(self,file,revision):
        # Grabs date
        date_list = self.conn.get("select date from (select cid,file,date from changeset union "
                                   "select rev,file,date from revision union "
                                  "select cid,file,date from dates) where cid=? and file=?;",(revision,file,))
        if date_list:
            date = date_list[0][0]
        else:
            url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-file/' + revision + file
            Log.note(url)
            response = Web.get_string(url)
            if response.status_code == 404:
                return ()
            mozobj = json.loads(response.text)
            date = mozobj['date'][0]
            cid = mozobj['node'][:12]
            file = "/" + mozobj['path']
            date = mozobj['date'][0]
            self.conn.execute("INSERT INTO DATES (CID,FILE,DATE) VALUES (?,?,?)",(cid,file,date,))
        # End Grab Date

        # TODO make it grab the max
        old_revision = self.conn.get("select REV,DATE,CHILD from revision where date<=? and file=?", (date, file,))
        if not old_revision or old_revision[0][0] == revision:
            return self._grab_revision(file,revision)
        old_rev_id = old_revision[0][0]
        current_changeset = old_revision[0][2] # Grab child
        current_date = old_revision[0][1]
        old_rev = self._grab_revision(file,old_rev_id)
        cs_list = []
        while True:
            if current_changeset == []:
                return old_rev
            change_set = self.conn.get(GRAB_CHANGESET_QUERY, (file, current_changeset,))
            if not current_changeset:
                return old_rev
            if not change_set:
                url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-diff/' + current_changeset + file
                Log.note(url)
                mozobj = Web.get(url)
                self._make_tids_from_diff(mozobj)
                cs_list = self.conn.get(GRAB_TID_QUERY, (file, current_changeset))
                current_changeset = mozobj['children']
                if current_changeset:
                    current_changeset = current_changeset[0][:12]
                current_date = mozobj['date'][0]
            else:
                cs_list = self.conn.get(GRAB_TID_QUERY, (file,current_changeset))
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
        res = self.conn.get("select t.tid,t.revision,t.file,t.line,t.operator from temporal t, revision r where "
                                   "t.tid=r.tid and r.file=? and r.rev=? order by r.line;", (file, revision[:12],))
        if res:
            return res
        url = 'https://hg.mozilla.org/'+self.config['hg']['branch']+'/json-annotate/' + revision + file
        Log.note(url)
        mozobj = Web.get(url)
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
            tid_result = self.conn.get_one("select TID from Temporal where REVISION=? AND FILE=? AND LINE=?",(el['node'][:12],file,el['targetline'],))[0]
            try:
                self.conn.execute("INSERT into REVISION (REV,FILE,DATE,CHILD,TID,LINE) values (substr(?,0,13),?,?,?,?,?);",
                                  (revision, file, date, child,tid_result,count,))
            except sqlite3.IntegrityError:
                pass
            count+=1

        self.conn.commit()
        return self.conn.get("select t.tid,t.revision,t.file,t.line,t.operator from temporal t, revision r where "
                                   "t.tid=r.tid and r.file=? and r.rev=? order by r.line;", (file, revision[:12],))

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
        try:
            self.conn.execute("INSERT into CHANGESET (CID,FILE,LENGTH,DATE,CHILD) values "
                              "(substr(?,0,13),?,?,?,?)",(cid,file,length,date,child,))
        except sqlite3.IntegrityError:
            pass
        for line in mozobj['diff'][0]['lines']:
            if current_line>0:
                if line['t'] == '-':
                    minus_count -= 1
                elif minus_count<0:
                    try:
                        self.conn.execute("INSERT into Temporal (REVISION,FILE,LINE,OPERATOR) values "
                                          "(substr(?,0,13),?,?,?);", (cid, file, current_line, minus_count,))
                    except sqlite3.IntegrityError:
                        Log.note("Already exists")
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
                        Log.note("Already exists")
                    current_line += 1
                if line['t'] == '':
                    current_line += 1
            else:
                current_line += 1

        self.conn.commit()
