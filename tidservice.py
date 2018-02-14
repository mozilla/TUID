# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
from collections import deque

from mo_dots import coalesce
from mo_future import text_type
from mo_logs import Log
from mo_times.dates import unicode2Date
from toposort import toposort_flatten

import sql
from pyLibrary.env import http
from pyLibrary.sql import sql_list, sql_iso
from pyLibrary.sql.sqlite import quote_value

DEBUG = True
RETRY = {"times": 3, "sleep": 5}

GET_LINES_QUERY = (
    "SELECT tuid, line" +
    " FROM temporal" +
    " WHERE file=? and revision=?" +
    " ORDER BY line"
)


GET_TUID_QUERY = "SELECT tuid FROM temporal WHERE revision=? and file=? and line=?"


class TIDService:
    def __init__(self, conn=None):  # pass in conn for testing purposes
        try:
            with open('config.json', 'r') as f:
                self.config = json.load(f, encoding='utf8')
            if not conn:
                self.conn = sql.Sql(self.config['database']['name'])
            else:
                self.conn = conn
            if not self.conn.get_one("SELECT name FROM sqlite_master WHERE type='table';"):
                self.init_db()

            self.next_tuid = coalesce(self.conn.get_one("SELECT max(tuid)+1 FROM temporal")[0], 1)
        except Exception as e:
            Log.error("can not setup service", cause=e)

    def init_db(self):
        self.conn.execute('''
        CREATE TABLE temporal (
            tuid     INTEGER,
            revision CHAR(12) NOT NULL,
            file     TEXT,
            line     INTEGER
        );''')

        self.conn.execute("CREATE UNIQUE INDEX temporal_rev_file ON temporal(revision, file, line)")

        # Changeset to hold all high-level changeset info
        self.conn.execute('''
        CREATE TABLE Changeset (
            cid CHAR(12),
            date INTEGER,
            parent CHAR(12),
            PRIMARY KEY(cid, parent)
        );''')

        Log.note("Table created successfully")

    def tuid(self):
        """
        :return: next tuid
        """
        try:
            return self.next_tuid
        finally:
            self.next_tuid += 1

    def get_tids_from_files(self, dir, files, revision):
        result = []
        total = len(files)
        for count, file in enumerate(files):
            Log.note("{{file}} {{percent|percent(decimal=0)}}", file=file, percent=count / total)
            result.append((file, self.get_tids(dir + file, revision)))
        return result

    def get_tids(self, file, revision):
        revision = revision[:12]
        file = file.lstrip('/')

        output = self._get_lines(file, revision)
        if output is not None:
            return output

        # if file is unknown, then use blame
        has_file = self.conn.get_one("select 1 from temporal where file=? limit 1", (file,))
        if not has_file:
            self._update_blame(revision, file)
            desc = self._get_single_changeset(revision)
            self.conn.execute(
                "INSERT INTO changeset (cid, date, parent)" +
                " VALUES " + sql_list(
                    sql_iso(sql_list([quote_value(desc.node[:12]), quote_value(unicode2Date(desc.date).unix), quote_value(p[:12])]))
                    for p in desc.parents
                )
            )
            self.conn.commit()
            return self._get_lines(file, revision)

        self._update_changesets(revision)
        return self._get_lines(file, revision)

    def _get_lines(self, file, revision):
        output = self.conn.get(GET_LINES_QUERY, (file, revision))
        if output:
            if len(output) == 1 and output[0] == (0, 0):
                return []  # file does not exist
            return output
        else:
            return None

    def _update_changesets(self, revision):
        # find all missing changesets up to `revision`
        if len(revision) != 12 or revision.lower() != revision:
            Log.error("expecting 12 char lowercase revision")
        cid = revision

        acc = {}
        todo = deque([cid])
        while todo:
            cid = todo.popleft()[:12]
            if cid in acc:
                continue
            has_changeset = self.conn.get_one("select 1 from changeset where cid = ?", (cid,))
            if has_changeset:
                break
            desc = self._get_single_changeset(cid)
            acc[desc.node[:12]] = desc
            todo.extend(desc.parents)
            todo.extend(desc.children)

        ordering = list(reversed(toposort_flatten({k: set(c[:12] for c in d.children) for k, d in acc.items()})))

        for rev in ordering:
            if rev not in acc:
                continue
            desc = acc[rev]
            cid = desc.node[:12]

            for f in set(desc.files):
                self._update_blame(cid, f)

            # copy all unchanged files from parents
            self.conn.execute(
                "INSERT INTO temporal (tuid, revision, file, line)" +
                " SELECT tuid, " + quote_value(cid) + ", file, line" +
                " FROM temporal" +
                " WHERE revision IN " + sql_iso(sql_list(p[:12] for p in desc.parent)) + " AND " +
                " file NOT IN " + sql_iso(sql_list(quote_value(f.lstrip('/')) for f in desc.files)) +
                " GROUP BY tuid, file, line"  # EXPECTED TO FAIL IF TWO PARENTS ARE DIFFERENT
            )

            self.conn.execute(
                "INSERT INTO changeset (cid, date, parent)" +
                " VALUES " + sql_list(
                    sql_iso(sql_list([quote_value(cid), quote_value(unicode2Date(desc.date).unix), quote_value(p[:12])]))
                    for p in desc.parents
                )
            )

    def _get_single_changeset(self, cid):
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + "/json-info?node=" + cid
        response = http.get_json(url, retry=RETRY)
        if len(response) > 1:
            Log.error("not expected")
        desc = list(response.values())[0]
        if DEBUG:
            Log.note("HG: {{url}} (date={{date}})", url=url, date=desc.date)
        return desc

    def _update_blame(self, revision, file):
        if len(revision) != 12 or revision.lower() != revision:
            Log.error("expecting 12 char lowercase revision")
        if file.lstrip() != file:
            Log.error(" '/' at start of file is not allowed")

        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-annotate/' + revision + "/" + file
        if DEBUG:
            Log.note("HG: {{url}}", url=url)
        mozobj = http.get_json(url, retry=RETRY)
        if isinstance(mozobj, (text_type, str)):
            # does not exist; add dummy record
            try:
                self.conn.execute(
                    "INSERT INTO temporal (tuid, revision, file, line) VALUES (?, ?, ?, ?)",
                    (0, revision, file, 0)
                )
                self.conn.commit()
                return
            except Exception as e:
                Log.error("not expected", cause=e)

        acc = {}
        for el in mozobj['annotate']:
            first_rev = el['node'][:12]
            key = (first_rev, file.lstrip('/'), el['targetline'])
            tuid = self.conn.get_one(GET_TUID_QUERY, key)
            if not tuid:
                tuid = self.tuid()
                acc[key] = tuid
            else:
                tuid = tuid[0]

            key = (revision, file.lstrip('/'), el['lineno'])
            existing_tuid = self.conn.get_one(GET_TUID_QUERY, key)
            if existing_tuid:
                if existing_tuid[0] != tuid:
                    Log.error("not expected")
            else:
                existing_tuid = acc.get(key)
                if existing_tuid is not None and existing_tuid != tuid:
                    Log.error("not expected")
                acc[key] = tuid

        if acc:
            self.conn.execute(
                "INSERT INTO temporal (tuid, revision, file, line) VALUES " +
                sql_list(sql_iso(sql_list(quote_value(v) for v in (t,)+k)) for k, t in acc.items())
            )
        self.conn.commit()


