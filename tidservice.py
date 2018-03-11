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
import os
import subprocess
import whatthepatch

from mo_dots import coalesce
from mo_future import text_type
from mo_logs import Log

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
    def __init__(self, conn=None,
                 local_hg_source="""C:/mozilla-source/mozilla-central/""",
                 hg_for_building="""C:/mozilla-build/python/Scripts/hg.exe"""):  # pass in conn for testing purposes
        self.local_hg_source = local_hg_source
        self.hg_for_building = hg_for_building

        try:
            with open('config.json', 'r') as f:
                self.config = json.load(f, encoding='utf8')
            if not conn:
                self.conn = sql.Sql(self.config['database']['name'])
            else:
                self.conn = conn
            created = False
            if not self.conn.get_one("SELECT name FROM sqlite_master WHERE type='table';"):
                self.init_db()
                created = True

            self.next_tuid = coalesce(self.conn.get_one("SELECT max(tuid)+1 FROM temporal")[0], 1)
            #if created:
            #    self.build_test_db()
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
        Log.note("Table created successfully")

    def insert_dummy(self, rev, file_name):
        if not self.conn.get_one("select 1 from temporal where file=? and revision =? and line=?", (file_name, rev, 0)):
            self.conn.execute(
                "INSERT INTO temporal (tuid, revision, file, line) VALUES (?, ?, ?, ?)",
                (-1, rev[:12], file_name, 0)
            )
            self.conn.commit()
        return [(-1,0)]

    def tuid(self):
        """
        :return: next tuid
        """
        try:
            return self.next_tuid
        finally:
            self.next_tuid += 1

    # Gets the TUIDs for the files modified by a revision.
    def get_tids_from_revision(self, revision):
        result = []
        URL_TO_FILES = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-info/' + revision
        try:
            mozobject = http.get_json(url=URL_TO_FILES, retry=RETRY)
        except Exception as e:
            Log.note("Unexpected error trying to get file list for revision " + revision, cause=e)
            return None

        files = mozobject[revision]['files']
        total = len(files)

        for count, file in enumerate(files):
            Log.note("{{file}} {{percent|percent(decimal=0)}}", file=file, percent=count / total)
            tmp_res = self.get_tids(file, revision)
            if tmp_res:
                result.append((file, tmp_res))
            else:
                Log.note("Error occured for file " + file + " in revision " + revision)
                result.append([(-1,0)])
        return result


    # Gets the TUIDs for a set of files, at a given revision.
    def get_tids_from_files(self, files, revision):
        result = []
        total = len(files)

        for count, file in enumerate(files):
            Log.note("{{file}} {{percent|percent(decimal=0)}}", file=file, percent=count / total)
            tmp_res = self.get_tids(file, revision)
            if tmp_res:
                result.append((file, tmp_res))
            else:
                Log.note("Error occured for file " + file + " in revision " + revision)
                result.append([(-1, 0)])
        return result


    # Inserts new lines from all changesets (this is all that is required).
    def _update_file_changesets(self, annotated_lines):
        for cset, anline in annotated_lines.items():
            self._update_file_changeset(anline['abspath'], cset)


    # Inserts diff information for the given file at the given revision.
    def _update_file_changeset(self, file, cset):
        # Get the diff
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-diff/' + cset + '/' + file
        if DEBUG:
            Log.note("HG: {{url}}", url=url)

        # Ensure we get the diff before continuing
        try:
            diff_object = http.get_json(url, retry=RETRY)
        except Exception as e:
            Log.note("Unexpected error while trying to get diff for: " + url, cause=e)
            Log.note("Inserting dummy revision...")
            self.insert_dummy(cset, file)
            return

        # Convert diff to text for whatthepatch, and parse the diff
        # TODO: Deal with csets that have changed names.
        parsed_diff = whatthepatch.parse_patch(
                         ''.join([line['l'] for line in diff_object['diff'][0]['lines']])
                      )
        # Generator manipulation for easier access
        tmp = [x for x in parsed_diff]
        changes = tmp[0][1]

        # Add all added lines into the DB.
        for line in changes:
            if line[0] == None and line[1] != None: # Signifies added line
                self.conn.execute(
                    "INSERT INTO temporal (tuid, revision, file, line)" +
                    " VALUES (?, ?, ?, ?)", (quote_value(self.tuid()), quote_value(cset), quote_value(file), quote_value(line[1]))
                )
        self.conn.commit()


    # Returns (TUID, line) tuples for a given file at a given revision.
    #
    # Uses json-annotate to find all lines in this revision, then it updates
    # the database with any missing revisions for the file changes listed
    # in annotate. Then, we use the information from annotate coupled with the
    # diff information that was inserted into the DB to return TUIDs. This way
    # we don't have to deal with child, parents, dates, etc..
    def get_tids(self, file, revision):
        revision = revision[:12]
        file = file.lstrip('/')

        # Get annotated file (cannot get around using this).
        # Unfortunately, this also means we always have to
        # deal with a small network delay.
        url = 'https://hg.mozilla.org/' + self.config['hg']['branch'] + '/json-annotate/' + revision + "/" + file
        if DEBUG:
            Log.note("HG: {{url}}", url=url)

        # If we can't get the annotated file, return dummy record.
        try:
            annotated_object = http.get_json(url, retry=RETRY)
            if isinstance(annotated_object, (text_type, str)):
                raise Exception("Annotated object does not exist.")
        except Exception as e:
            Log.note("Error while obtaining annotated file for file " + file + " in revision " + revision, error=e)
            Log.note("Inserting dummy entry...")
            dummy_entry = self.insert_dummy(revision, file)
            return dummy_entry

        # Gather all missing csets and the
        # corresponding lines.
        annotated_lines = {}
        line_origins = []
        for node in annotated_object['annotate']:
            cset_len12 = node['node'][:12]

            # If the cset is not in the database, process it
            #
            # Use the 'abspath' field to determine the current filename in
            # case it has changed.
            has_cset = self.conn.get_one("select 1 from temporal where file=? and revision=? limit 1",
                                         params=(quote_value(node['abspath']), quote_value(cset_len12)))
            if not has_cset and cset_len12 not in annotated_lines:
                annotated_lines[cset_len12] = node

            # Used to gather TUIDs later
            line_origins.append((quote_value(node['abspath']), quote_value(cset_len12), quote_value(int(node['targetline']))))

        # Update DB with any revisions found in annotated
        # object that are not in the DB.
        if len(annotated_lines) > 0:
            self._update_file_changesets(annotated_lines)

        # Get the TUIDs for each line (can probably be optimized with a join)
        tuids = []
        for line_num in range(1, len(line_origins)+1):
            try:
                tuid_tmp = self.conn.get_one("select tuid from temporal where file=? and revision=? and line=?",
                                             line_origins[line_num-1])

                # Return dummy line if we can't find the TUID for this entry
                # (likely because of an error from insertion).
                if tuid_tmp:
                    tuids.append((tuid_tmp[0], line_num))
                else:
                    tuids.append((-1, 0))
            except Exception as e:
                Log.note("Unexpected error searching for " + line_num, cause=e)

        return tuids


    # Previously used to build test_db. Needs to be rewritten for new system.
    # TODO: Rewrite for new system.
    def build_test_db(self, files_to_add=1000):
        # Get all file names under dom for testing.
        if not os.path.exists(self.local_hg_source):
            raise Exception("Can't find local hg source for file information.")

        try:
            import adr.recipes.all_code_coverage_files as adr_cc
        except:
            raise Exception("Active-data-recipes needs to be installed.")
        rev = '9f87ddff4b02'
        results = adr_cc.run(['--path', 'dom/', '--rev', '9f87ddff4b02'])

        print(results[1])
        print(results[1][0])
        print('here: ' + str(results[1][0]))
        results = list(set([results[i][0] for i in range(1,len(results))]))    # Get rid of header and duplicates

        # Update to the correct revision
        cwd = os.getcwd()
        os.chdir(self.local_hg_source)
        try:
            subprocess.check_output([self.hg_for_building, 'pull', 'central'])
            subprocess.check_output([self.hg_for_building, 'update', '-r', rev])
        except Exception as e:
            Log.error("Hg has broken...", cause=e)
        finally:
            os.chdir(cwd)

        URL_TO_REV = 'https://hg.mozilla.org/mozilla-central/json-annotate/' + rev + '/'
        date = None

        # For each covered file, add them into the DB.
        # (We are creating a database here, no need to check if they exists, etc.)
        for file_name in results[:files_to_add]:
            # Get the file info and add the lines into the DB.
            '''
            # Slow method...
            if DEBUG:
                Log.note("Adding file from HG: {{url}}", url=URL_TO_REV + file_name)
            try:
                mozobject = http.get_json(URL_TO_REV + file_name, retry=RETRY)
                if not date:
                    date = mozobject['date'][0]
            except Exception as e:
                Log.note("Unexpected HG call failure during database building...continuing to next file.", error=e)

            # Ensure it exists.
            if isinstance(mozobject, (text_type, str)):
                # File does not exist; add dummy record
                try:
                    self.conn.execute(
                        "INSERT INTO temporal (tuid, revision, file, line) VALUES (?, ?, ?, ?)",
                        (-1, rev, file_name, 0)
                    )
                    self.conn.commit()
                    continue
                except Exception as e:
                    Log.error("not expected", cause=e)
            '''
            # Fast method using a local mozilla-central build. Very fast in comparison to hg
            # until we either use elasticsearch, or something else. Method above takes 27hrs for 50,000 files
            # and this method takes 3 hours for about 0.2 sec per file.

            # If the file does not exist, insert a dummy copy
            local_file = os.path.join(self.local_hg_source, file_name)
            if not os.path.exists(local_file):
                self.insert_dummy(rev, file_name)
                continue

            # Count the lines
            line_count = 0
            for _ in open(local_file):
                line_count += 1

            if line_count == 0:
                self.insert_dummy(rev, file_name)
                continue

            # Now add all lines, creating new TUID's for each of them.
            Log.note('file_name: ' + file_name)
            def sampler(to_print):
                print(to_print)
                return to_print

            inserted = False
            retry_count = 0
            while (not inserted) and retry_count < 5:
                try:
                    self.conn.execute(
                        "INSERT INTO temporal (tuid, revision, file, line) VALUES " + sql_list(
                            sql_iso(sql_list([quote_value(self.tuid()), quote_value(rev), quote_value(file_name), quote_value(el)]))
                            for el in range(1, line_count))
                    )
                    self.conn.commit()
                    inserted = True
                except Exception as e:
                    Log.note("Odd unexpected error...retrying...\nError:\n" + str(e))
                    retry_count += 1

                    if retry_count == 5:
                        Log.note("Not retrying again, failed to insert file " + file_name + " tried inserting the following: ")
                        print([[quote_value(self.tuid()), quote_value(rev), quote_value(file_name), quote_value(el)]
                                for el in range(1, line_count+1)])

        URL_TO_INFO = 'https://hg.mozilla.org/mozilla-central/json-info/'
        mozobject = http.get_json(URL_TO_INFO + rev, retry=RETRY)
        if len(mozobject[rev]['children']) != 1:
            Log.error("Unexpected number of children for revision: Expected 1, Got " + str(len(mozobject[rev]['children'])))
        if not 0 < len(mozobject[rev]['parents']) <= 2:
            Log.error("Unexpected number of parents for revision: Expected 1 or 2, Got " + str(len(mozobject[rev]['children'])))

        # Insert the changed files in this revision.
        self.conn.execute(
            "INSERT INTO fileModifications (cid, date, file)" +
            " VALUES " + sql_list(sql_iso(sql_list([quote_value(rev), quote_value(date), quote_value(filep)])) for filep in mozobject[rev]['files'])
        )
        # Insert the initial changeset.
        # If parent2 exists, keep it.
        if len(mozobject[rev]['parents']) > 1:
            parent2 = mozobject[rev]['parents'][1]
        else:
            parent2 = '-1'
        self.conn.execute(
            "INSERT INTO changeset (cid, child, parent1, parent2) VALUES (?, ?, ?, ?)",
            (quote_value(rev), quote_value(mozobject[rev]['children'][0]), quote_value(mozobject[rev]['parents'][0]), parent2)
        )

        self.conn.commit()
        Log.note("Initialization complete...")
        return


