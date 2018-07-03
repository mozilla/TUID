# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import gc
import time

from jx_python import jx
from mo_dots import Null, coalesce, wrap
from mo_future import text_type
from mo_hg.hg_mozilla_org import HgMozillaOrg
from mo_files.url import URL
from mo_kwargs import override
from mo_logs import Log
from mo_math.randoms import Random
from mo_threads import Till, Thread, Lock
from mo_times.durations import SECOND, DAY
from pyLibrary.env import http
from pyLibrary.meta import cache
from pyLibrary.sql import sql_list, sql_iso
from pyLibrary.sql.sqlite import quote_value
from tuid import sql
from tuid.util import MISSING, TuidMap

# Global constants
DEBUG = False

# Tuid constants
VERIFY_TUIDS = True
RETRY = {"times": 3, "sleep": 5}
SQL_ANN_BATCH_SIZE = 5
SQL_BATCH_SIZE = 500
FILES_TO_PROCESS_THRESH = 5
ENABLE_TRY = False
DAEMON_WAIT_AT_NEWEST = 30 * SECOND # Time to wait at the newest revision before polling again.

# Clogger constants
SQL_CSET_BATCH_SIZE = 500
CSET_TIP_WAIT_TIME = 5 * 60 # seconds
CSET_BACKFILL_WAIT_TIME = 1 * 60 # seconds
CSET_MAINTENANCE_WAIT_TIME = 30 * 60 # seconds
CSET_DELETION_WAIT_TIME = 1 * 60 # seconds
TUID_EXISTENCE_WAIT_TIME = 1 * 60 # seconds
MINIMUM_PERMANENT_CSETS = 1000
MAXIMUM_NONPERMANENT_CSETS = 20000
UPDATE_VERY_OLD_FRONTIERS = False

GET_TUID_QUERY = "SELECT tuid FROM temporal WHERE file=? and revision=? and line=?"
GET_ANNOTATION_QUERY = "SELECT annotation FROM annotations WHERE revision=? and file=?"
GET_LATEST_MODIFICATION = "SELECT revision FROM latestFileMod WHERE file=?"

HG_URL = URL('https://hg.mozilla.org/')


class TUIDService:

    @override
    def __init__(self, database, hg, hg_cache=None, conn=None, kwargs=None, clogger=None):
        try:
            self.config = kwargs

            self.conn = conn if conn else sql.Sql(self.config.database.name)
            self.hg_cache = HgMozillaOrg(kwargs=self.config.hg_cache, use_cache=True) if self.config.hg_cache else Null

            if not self.conn.get_one("SELECT name FROM sqlite_master WHERE type='table';"):
                self.init_db()

            self.clogger = clogger if clogger else Clogger(conn=self.conn, tuid_service=self, kwargs=kwargs)
            self.locker = Lock()
            self.next_tuid = coalesce(self.conn.get_one("SELECT max(tuid)+1 FROM temporal")[0], 1)
        except Exception as e:
            Log.error("can not setup service", cause=e)


    def tuid(self):
        """
        :return: next tuid
        """
        with self.locker:
            try:
                return self.next_tuid
            finally:
                self.next_tuid += 1


    def init_db(self):
        '''
        Creates all the tables, and indexes needed for the service.

        :return: None
        '''
        with self.conn.transaction() as t:
            t.execute('''
            CREATE TABLE temporal (
                tuid     INTEGER,
                revision CHAR(12) NOT NULL,
                file     TEXT,
                line     INTEGER
            );''')

            t.execute('''
            CREATE TABLE annotations (
                revision       CHAR(12) NOT NULL,
                file           TEXT,
                annotation     TEXT,
                PRIMARY KEY(revision, file)
            );''')

            # Used in frontier updating
            t.execute('''
            CREATE TABLE latestFileMod (
                file           TEXT,
                revision       CHAR(12) NOT NULL,
                PRIMARY KEY(file)
            );''')

            # Used by cset worker
            t.execute('''
            CREATE TABLE csetLog (
                revnum         INTEGER,
                revision       CHAR(12) NOT NULL,
                timestamp      INTEGER,
                PRIMARY KEY(revision)
            );''')

            t.execute("CREATE UNIQUE INDEX temporal_rev_file ON temporal(revision, file, line)")
        Log.note("Tables created successfully")


    def _dummy_tuid_exists(self, transaction, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        return None != transaction.get_one("select 1 from temporal where file=? and revision=? and line=?",
                                         (file_name, rev, 0))


    def _dummy_annotate_exists(self, transaction, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        return None != transaction.get_one("select 1 from annotations where file=? and revision=? and annotation=?",
                                         (file_name, rev, ''))


    def insert_tuid_dummy(self, transaction, rev, file_name, commit=True):
        # Inserts a dummy tuid: (-1,rev,file_name,0)
        if not self._dummy_tuid_exists(transaction, file_name, rev):
            transaction.execute(
                "INSERT INTO temporal (tuid, revision, file, line) VALUES (?, ?, ?, ?)",
                (-1, rev[:12], file_name, 0)
            )
            # if commit:
            #     self.conn.commit()
        return MISSING


    def insert_annotate_dummy(self, transaction, rev, file_name, commit=True):
        # Inserts annotation dummy: (rev, file, '')

        if not self._dummy_annotate_exists(transaction, file_name, rev):
            self.insert_annotations(transaction, [(rev[:12], file_name, '')])


    def insert_annotations(self, transaction, data):
        if VERIFY_TUIDS:
            for _, _, tuids_string in data:
                self.destringify_tuids(tuids_string)

        transaction.execute(
            "INSERT INTO annotations (revision, file, annotation) VALUES " +
            sql_list(sql_iso(sql_list(map(quote_value, row))) for row in data)
        )


    def _get_annotation(self, rev, file, transaction=None):
        # Returns an annotation if it exists
        return coalesce(transaction, self.conn).get_one(GET_ANNOTATION_QUERY, (rev, file))[0]


    def _get_one_tuid(self, transaction, cset, path, line):
        # Returns a single TUID if it exists
        return transaction.get_one(
            "select tuid from temporal where revision=? and file=? and line=?",
            (cset, path, int(line))
        )

    def _get_latest_revision(self, file, transaction):
        # Returns the latest revision that we
        # have information on the requested file.
        return coalesce(transaction, self.conn).get_one(GET_LATEST_MODIFICATION, (file,))


    def stringify_tuids(self, tuid_list):
        # Turns the TuidMap list to a string for storage in
        # the annotations table.
        return "\n".join([','.join([str(x.tuid), str(x.line)]) for x in tuid_list])


    def destringify_tuids(self, tuids_string):
        # Builds up TuidMap list from annotation cache entry.
        try:
            lines = tuids_string.splitlines()
            line_origins = []
            for line in lines:
                if not line:
                    continue
                tuid, linenum = line.split(',')
                line_origins.append(
                    TuidMap(int(tuid), int(linenum))
                )
            return line_origins
        except Exception as e:
            Log.error("Invalid entry in tuids list:\n{{list}}", list=tuids_string, cause=e)


    # Gets a diff from a particular revision from https://hg.mozilla.org/
    def _get_hg_diff(self, cset, repo=None):
        def check_merge(description):
            if description.startswith("merge "):
                return True
            elif description.startswith("Merge "):
                return True
            return False

        if repo is None:
            repo = self.config.hg.branch
        tmp = self.hg_cache.get_revision(
            wrap({
                "changeset": {"id": cset},
                "branch": {"name": repo}
            }),
            None, False, True
        )
        output = tmp['changeset']['moves']
        output2 = {}
        output2['diffs'] = output

        merge_description = tmp['changeset']['description']
        output2['merge'] = check_merge(merge_description)
        return output2


    # Gets an annotated file from a particular revision from https://hg.mozilla.org/
    def _get_hg_annotate(self, cset, file, annotated_files, thread_num, repo, please_stop=None):
        url = HG_URL / repo / "json-annotate" / cset / file
        if DEBUG:
            Log.note("HG: {{url}}", url=url)

        # Ensure we get the annotate before continuing
        try:
            annotated_files[thread_num] = http.get_json(url, retry=RETRY)
        except Exception as e:
            annotated_files[thread_num] = []
            Log.warning("Unexpected error while trying to get annotate for {{url}}", url=url, cause=e)
        return


    def get_diffs(self, csets, repo=None):
        # Get all the diffs
        if repo is None:
            repo = self.config.hg.branch

        list_diffs = []
        for cset in csets:
            list_diffs.append({'cset': cset, 'diff': self._get_hg_diff(cset,repo=repo)})
        return list_diffs


    def get_tuids_from_revision(self, revision):
        """
        Gets the TUIDs for the files modified by a revision.

        :param revision: revision to get files from
        :return: list of (file, list(tuids)) tuples
        """
        result = []
        URL_TO_FILES = HG_URL / self.config.hg.branch / 'json-info' / revision
        try:
            mozobject = http.get_json(url=URL_TO_FILES, retry=RETRY)
        except Exception as e:
            Log.warning("Unexpected error trying to get file list for revision {{revision}}", cause=e)
            return None

        files = mozobject[revision]['files']

        results = self.get_tuids(files, revision)
        return results

    @cache(duration=DAY)
    def _check_branch(self, revision, branch):
        '''
        Used to find out if the revision is in the given branch.

        :param revision: Revision to check.
        :param branch: Branch to check revision on.
        :return: True/False - Found it/Didn't find it
        '''

        # Get a changelog
        clog_url = HG_URL / branch / 'json-log' / revision
        try:
            Log.note("Searching through changelog {{url}}", url=clog_url)
            clog_obj = http.get_json(clog_url, retry=RETRY)
            if isinstance(clog_obj, (text_type, str)):
                Log.note(
                    "Revision {{cset}} does not exist in the {{branch}} branch",
                    cset=revision, branch=branch
                )
                return False
        except Exception as e:
            Log.note("Unexpected error getting changset-log for {{url}}: {{error}}", url=clog_url, error=e)
            return False
        return True

    def mthread_testing_get_tuids_from_files(self, files, revision, results, res_position,
                                             going_forward=False, repo=None, please_stop=None):
        """
        Same as `get_tuids_from_files` but for multi-threaded service _result_ testing.
        :param files:
        :param revision:
        :param going_forward:
        :param repo:
        :param please_stop:
        :return:
        """
        Log.note("Thread {{pos}} is running.", pos=res_position)
        results[res_position], _ = self.get_tuids_from_files(files, revision, going_forward=going_forward, repo=repo)
        Log.note("Thread {{pos}} is ending.", pos=res_position)
        return

    def get_tuids_from_files(
            self,
            files,
            revision,
            going_forward=False,
            repo=None,
            use_thread=True,
            max_csets_proc=30
        ):
        """
        Gets the TUIDs for a set of files, at a given revision.
        list(tuids) is an array of tuids, one tuid for each line, in order, and `null` if no tuid assigned

        Uses frontier updating to build and maintain the tuids for
        the given set of files. Use changelog to determine what revisions
        to process and get the files that need to be updated by looking
        at the diffs. If the latestFileMod table is empty, for any file,
        we perform an annotation-based update.

        This function assumes the newest file names are given, if they
        are not, then no TUIDs are returned for that file.

        IMPORTANT:
        If repo is set to None, the service will check if the revision is in
        the correct branch (to prevent catastrophic failures down the line) - this
        results in one extra changeset log call per request.
        If repo is set to something other than None, then we assume that the caller has already
        checked this and is giving a proper branch for the revision.

        :param files: list of files
        :param revision: revision to get files at
        :param repo: Branch to get files from (mozilla-central, or try)
        :param disable_thread: Disables the thread that spawns if the number of files to process exceeds the
                               threshold set by FILES_TO_PROCESS_THRESH.
        :param going_forward: When set to true, the frontiers always get updated to the given revision
                              even if we can't find a file's frontier. Otherwise, if a frontier is too far,
                              the latest revision will not be updated.
        :return: The following tuple which contains:
                    ([list of (file, list(tuids)) tuples], True/False if completed or not)
        """
        completed = True

        if repo is None:
            repo = self.config.hg.branch
            check = self._check_branch(revision, repo)
            if not check:
                # Error was already output by _check_branch
                return [(file, []) for file in files], completed

        if repo in ('try',):
            # We don't need to keep latest file revisions
            # and other related things for this condition.

            # Enable the 'try' repo calls with ENABLE_TRY
            if ENABLE_TRY:
                return self._get_tuids_from_files_try_branch(files, revision), completed
            return [(file, []) for file in files], completed

        result = []
        revision = revision[:12]
        files = [file.lstrip('/') for file in files]
        frontier_update_list = []

        total = len(files)
        latestFileMod_inserts = {}
        new_files = []

        log_existing_files = []
        for count, file in enumerate(files):
            # Go through all requested files and
            # either update their frontier or add
            # them to the DB through an initial annotation.

            if DEBUG:
                Log.note(" {{percent|percent(decimal=0)}}|{{file}}", file=file, percent=count / total)

            with self.conn.transaction() as t:
                latest_rev = self._get_latest_revision(file, transaction=t)
                already_ann = self._get_annotation(revision, file, transaction=t)

            # Check if the file has already been collected at
            # this revision and get the result if so
            if already_ann:
                result.append((file,self.destringify_tuids(already_ann)))
                if going_forward:
                    latestFileMod_inserts[file] = (file, revision)
                log_existing_files.append('exists|' + file)
                continue
            elif already_ann == '':
                result.append((file,[]))
                if going_forward:
                    latestFileMod_inserts[file] = (file, revision)
                log_existing_files.append('removed|' + file)
                continue

            if (latest_rev and latest_rev[0] != revision):
                # File has a frontier, let's update it
                if DEBUG:
                    Log.note("Will update frontier for file {{file}}.", file=file)
                frontier_update_list.append((file, latest_rev[0]))
            elif latest_rev == revision:
                with self.conn.transaction() as t:
                    t.execute("DELETE FROM latestFileMod WHERE file = " + quote_value(file))
                new_files.append(file)
                Log.note(
                    "Missing annotation for existing frontier - readding: " +
                    "{{rev}}|{{file}} ",
                    file=file, rev=revision
                )
            else:
                Log.note(
                    "Frontier update - adding: " +
                    "{{rev}}|{{file}} ",
                    file=file, rev=revision
                )
                new_files.append(file)

        if DEBUG:
            Log.note(
                "Frontier update - already exist in DB: " +
                "{{rev}} || {{file_list}} ",
                file_list=str(log_existing_files), rev=revision
            )
        else:
            Log.note(
                "Frontier update - already exist in DB for {{rev}}: " +
                    "{{count}}/{{total}} | {{percent|percent}}",
                count=str(len(log_existing_files)), total=str(len(files)),
                rev=revision, percent=len(log_existing_files)/len(files)
            )

        if len(latestFileMod_inserts) > 0:
            with self.conn.transaction() as transaction:
                for _, inserts_list in jx.groupby(latestFileMod_inserts.values(), size=SQL_BATCH_SIZE):
                    transaction.execute(
                        "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
                        sql_list(
                            sql_iso(sql_list(map(quote_value, i)))
                            for i in inserts_list
                        )
                    )

        def update_tuids_in_thread(
                new_files,
                frontier_update_list,
                revision,
                please_stop=None
            ):
            try:
                # Processes the new files and files which need their frontier updated
                # outside of the main thread as this can take a long time.
                result = []

                latestFileMod_inserts = {}
                if len(new_files) > 0:
                    # File has never been seen before, get it's initial
                    # annotation to work from in the future.
                    tmp_res = self.get_tuids(new_files, revision, commit=False)
                    if tmp_res:
                        result.extend(tmp_res)
                    else:
                        Log.note("Error occured for files " + str(new_files) + " in revision " + revision)

                    # If this file has not been seen before,
                    # add it to the latest modifications, else
                    # it's already in there so update its past
                    # revisions.
                    for file in new_files:
                        latestFileMod_inserts[file] = (file, revision)

                Log.note("Finished updating frontiers. Updating DB table `latestFileMod`...")
                if len(latestFileMod_inserts) > 0:
                    with self.conn.transaction() as transaction:
                        for _, inserts_list in jx.groupby(latestFileMod_inserts.values(), size=SQL_BATCH_SIZE):
                            transaction.execute(
                                "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
                                sql_list(
                                    sql_iso(sql_list(map(quote_value, i)))
                                    for i in inserts_list
                                )
                            )

                # If we have files that need to have their frontier updated, do that now
                if len(frontier_update_list) > 0:
                    tmp = self._update_file_frontiers(
                        frontier_update_list,
                        revision,
                        going_forward=going_forward,
                        max_csets_proc=max_csets_proc
                    )
                    result.extend(tmp)

                Log.note("Completed work overflow for revision {{cset}}", cset=revision)
                return result
            except Exception as e:
                Log.warning("Thread dead becasue of problem", cause=e)

        threaded = False
        if use_thread:
            # If there are too many files to process, start a thread to do
            # that work and return completed as False.
            if (len(new_files) + len(frontier_update_list) > FILES_TO_PROCESS_THRESH):
                threaded = True

        if threaded:
            completed = False
            Log.note("Incomplete response given")
            Thread.run(
                'get_tuids_from_files (' + Random.base64(9) + ")",
                update_tuids_in_thread, new_files, frontier_update_list, revision
            )
        else:
            result.extend(
                update_tuids_in_thread(new_files, frontier_update_list, revision)
            )

        return result, completed


    def _apply_diff_forwards(self, transaction, annotation, diff, cset, file):
        '''
        Using an annotation ([(tuid,line)] - array
        of TuidMap objects), we change the line numbers to
        reflect a given diff and return them. diff must
        be a diff object returned from get_diff(cset, file).
        Only for going forward in time, not back.

        :param annotation: list of TuidMap objects
        :param diff: unified diff from get_diff
        :param cset: revision to apply diff at
        :param file: name of file diff is applied to
        :return:
        '''
        # Add all added lines into the DB.
        list_to_insert = []
        new_ann = [x for x in annotation]
        new_ann.sort(key=lambda x: x.line)

        def add_one(tl_tuple, lines):
            start = tl_tuple.line
            return lines[:start-1] + [tl_tuple] + [TuidMap(tmap.tuid, int(tmap.line) + 1) for tmap in lines[start-1:]]

        def remove_one(start, lines):
            return lines[:start-1] + [TuidMap(tmap.tuid, int(tmap.line) - 1) for tmap in lines[start:]]

        for f_proc in diff:
            if f_proc['new'].name.lstrip('/') != file:
                continue

            f_diff = f_proc['changes']
            for change in f_diff:
                if change.action == '+':
                    tuid_tmp = self._get_one_tuid(transaction, cset, file, change.line+1)
                    if not tuid_tmp:
                        new_tuid = self.tuid()
                        list_to_insert.append((new_tuid, cset, file, change.line+1))
                    else:
                        new_tuid = tuid_tmp[0]
                    new_ann = add_one(TuidMap(new_tuid, change.line+1), new_ann)
                elif change.action == '-':
                    new_ann = remove_one(change.line+1, new_ann)
            break # Found the file, exit searching

        if len(list_to_insert) > 0:
            count = 0
            for _, inserts_list in jx.groupby(list_to_insert, size=SQL_BATCH_SIZE):
                transaction.execute(
                    "INSERT INTO temporal (tuid, revision, file, line)" +
                    " VALUES " +
                    sql_list(sql_iso(sql_list(map(quote_value, tp))) for tp in inserts_list)
                )

        return new_ann



    def _apply_diff_backwards(self, transaction, annotation, diff, cset, file):
        '''
        Using an annotation ([(tuid,line)] - array
        of TuidMap objects), we change the line numbers to
        reflect a given diff and return them. diff must
        be a diff object returned from get_diff(cset, file).
        Only for going forward in time, not back.

        :param annotation: list of TuidMap objects
        :param diff: unified diff from get_diff
        :param cset: revision to apply diff at
        :param file: name of file diff is applied to
        :return:
        '''
        # Ignore merges, they have duplicate entries.
        if diff['merge']:
            return annotation, file
        if file.lstrip('/') == 'dev/null':
            return [], file

        list_to_insert = []
        new_ann = [x for x in annotation]
        new_ann.sort(key=lambda x: x.line)

        def add_one(tl_tuple, lines):
            start = tl_tuple.line
            return lines[:start-1] + [tl_tuple] + [TuidMap(tmap.tuid, int(tmap.line) + 1) for tmap in lines[start-1:]]

        def remove_one(start, lines):
            return lines[:start-1] + [TuidMap(tmap.tuid, int(tmap.line) - 1) for tmap in lines[start:]]

        for f_proc in diff['diffs']:
            new_fname = f_proc['new'].name.lstrip('/')
            old_fname = f_proc['old'].name.lstrip('/')
            if new_fname != file and old_fname != file:
                continue
            if old_fname != new_fname:
                if new_fname == 'dev/null':
                    return [], file
                # Change the file name so that new tuids
                # are correctly created.
                file = new_fname

            f_diff = f_proc['changes']
            for change in f_diff:
                if change.action == '+':
                    tuid_tmp = self._get_one_tuid(transaction, cset, file, change.line+1)
                    if not tuid_tmp:
                        new_tuid = self.tuid()
                        list_to_insert.append((new_tuid, cset, file, change.line+1))
                    else:
                        new_tuid = tuid_tmp[0]
                    new_ann = add_one(TuidMap(new_tuid, change.line+1), new_ann)
                elif change.action == '-':
                    new_ann = remove_one(change.line+1, new_ann)
            break # Found the file, exit searching

        if len(list_to_insert) > 0:
            count = 0
            for _, inserts_list in jx.groupby(list_to_insert, size=SQL_BATCH_SIZE):
                transaction.execute(
                    "INSERT INTO temporal (tuid, revision, file, line)" +
                    " VALUES " +
                    sql_list(sql_iso(sql_list(map(quote_value, tp))) for tp in inserts_list)
                )

        return new_ann, file


    def _get_tuids_from_files_try_branch(self, files, revision):
        '''
        Gets files from a try revision. It abuses the idea that try pushes
        will come from various, but stable points (if people make many
        pushes on that revision). Furthermore, updates are generally done
        to a revision that should eventually have tuids already in the DB
        (i.e. overtime as people update to revisions that have a tuid annotation).

        :param files: Files to query.
        :param revision: Revision to get them at.
        :return: List of (file, tuids) tuples.
        '''

        repo = 'try'
        result = []
        log_existing_files = []
        files_to_update = []

        # Check if the files were already annotated.
        for file in files:
            with self.conn.transaction() as t:
                already_ann = self._get_annotation(revision, file, transaction=t)
            if already_ann:
                result.append((file, self.destringify_tuids(already_ann)))
                log_existing_files.append('exists|' + file)
                continue
            elif already_ann[0] == '':
                result.append((file, []))
                log_existing_files.append('removed|' + file)
                continue
            else:
                files_to_update.append(file)

        if len(log_existing_files) > 0:
            Log.note(
                "Try revision run - existing entries: {{count}}/{{total}} | {{percent}}",
                count=str(len(log_existing_files)),
                total=str(len(files)),
                percent=str(100*(len(log_existing_files)/len(files)))
            )

        if len(files_to_update) <= 0:
            Log.note("Found all files for try revision request: {{cset}}", cset=revision)
            return result

        # There are files to process, so let's find all the diffs.
        found_mc_patch = False
        diffs_to_get = [] # Will contain diffs in reverse order of application
        curr_rev = revision
        mc_revision = ''
        while not found_mc_patch:
            jsonrev_url = HG_URL / repo / 'json-rev' / curr_rev
            try:
                Log.note("Searching through changelog {{url}}", url=jsonrev_url)
                clog_obj = http.get_json(jsonrev_url, retry=RETRY)
                if isinstance(clog_obj, (text_type, str)):
                    Log.error(
                        "Revision {{cset}} does not exist in the {{branch}} branch",
                        cset=curr_rev, branch=repo
                    )
                if 'phase' not in clog_obj:
                    Log.warning(
                        "Unexpected error getting changset-log for {{url}}: `phase` entry cannot be found.",
                        url=jsonrev_url
                    )
                    return [(file, []) for file in files]
            except Exception as e:
                Log.warning(
                    "Unexpected error getting changset-log for {{url}}: {{error}}",
                    url=jsonrev_url, error=e
                )
                return [(file, []) for file in files]

            # When `phase` is public, the patch is (assumed to be)
            # in any repo other than try.
            if clog_obj['phase'] == 'public':
                found_mc_patch = True
                mc_revision = curr_rev
                continue
            elif clog_obj['phase'] == 'draft':
                diffs_to_get.append(curr_rev)
            else:
                Log.warning(
                    "Unknown `phase` state `{{state}}` encountered at revision {{cset}}",
                    cset=curr_rev, state=clog_obj['phase']
                )
                return [(file, []) for file in files]
            curr_rev = clog_obj['parents'][0][:12]

        added_files = {}
        removed_files = {}
        files_to_process = {}

        Log.note("Gathering diffs for: {{csets}}", csets=str(diffs_to_get))
        all_diffs = self.get_diffs(diffs_to_get, repo=repo)

        # Build a dict for faster access to the diffs
        parsed_diffs = {entry['cset']: entry['diff'] for entry in all_diffs}
        for csets_diff in all_diffs:
            cset_len12 = csets_diff['cset']
            parsed_diff = csets_diff['diff']['diffs']

            for f_added in parsed_diff:
                # Get new entries for removed files.
                new_name = f_added['new'].name.lstrip('/')
                old_name = f_added['old'].name.lstrip('/')

                # If we don't need this file, skip it
                if new_name not in files_to_update:
                    # If the file was removed, set a
                    # flag and return no tuids later.
                    if new_name == 'dev/null':
                        removed_files[old_name] = True
                    continue

                if old_name == 'dev/null':
                    added_files[new_name] = True
                    continue

                if new_name in files_to_process:
                    files_to_process[new_name].append(cset_len12)
                else:
                    files_to_process[new_name] = [cset_len12]

        # We've found a good patch (a public one), get it
        # for all files and apply the patch's onto it.
        curr_annotations = self.get_tuids(files, mc_revision, commit=False)
        curr_annots_dict = {file: mc_annot for file, mc_annot in curr_annotations}

        anns_to_get = []
        ann_inserts = []
        tmp_results = {}

        with self.conn.transaction() as transaction:
            for file in files_to_update:
                if file not in curr_annots_dict:
                    Log.note(
                        "WARNING: Missing annotation entry in mozilla-central branch revision {{cset}} " +
                        "for {{file}}",
                        file=file, cset=mc_revision
                    )
                    # Try getting it from the try revision
                    anns_to_get.append(file)
                    continue

                if file in added_files:
                    Log.note("Try revision run - added: {{file}}", file=file)
                    anns_to_get.append(file)
                elif file in removed_files:
                    Log.note("Try revision run - removed: {{file}}", file=file)
                    ann_inserts.append((revision, file, ''))
                    tmp_results[file] = []
                elif file in files_to_process:
                    # Reverse the list, we always find the newest diff first
                    Log.note("Try revision run - modified: {{file}}", file=file)
                    csets_to_proc = files_to_process[file][::-1]
                    old_ann = curr_annots_dict[file]

                    # Apply all the diffs
                    tmp_res = old_ann
                    new_fname = file
                    for i in csets_to_proc:
                        tmp_res, new_fname = self._apply_diff(transaction, tmp_res, parsed_diffs[i], i, new_fname)

                    ann_inserts.append((revision, file, self.stringify_tuids(tmp_res)))
                    tmp_results[file] = tmp_res
                else:
                    # Nothing changed with the file, use it's current annotation
                    Log.note("Try revision run - not modified: {{file}}", file=file)
                    ann_inserts.append((revision, file, self.stringify_tuids(curr_annots_dict[file])))
                    tmp_results[file] = curr_annots_dict[file]

            # Insert and check annotations, get all that were
            # added by another thread.
            anns_added_by_other_thread = {}
            if len(ann_inserts) > 0:
                ann_inserts = list(set(ann_inserts))
                for _, tmp_inserts in jx.groupby(ann_inserts, size=SQL_ANN_BATCH_SIZE):
                    # Check if any were added in the mean time by another thread
                    recomputed_inserts = []
                    for rev, filename, tuids in tmp_inserts:
                        tmp_ann = self._get_annotation(rev, filename, transaction=transaction)
                        if not tmp_ann:
                            recomputed_inserts.append((rev, filename, tuids))
                        else:
                            anns_added_by_other_thread[filename] = self.destringify_tuids(tmp_ann)

                    try:
                        self.insert_annotations(transaction, recomputed_inserts)
                    except Exception as e:
                        Log.error("Error inserting into annotations table.", cause=e)

        if len(anns_to_get) > 0:
            result.extend(self.get_tuids(anns_to_get, revision, repo=repo))

        for f in tmp_results:
            tuids = tmp_results[f]
            if f in anns_added_by_other_thread:
                tuids = anns_added_by_other_thread[f]
            result.append((f, tuids))
        return result


    def _update_file_frontiers(
            self,
            frontier_list,
            revision,
            max_csets_proc=30,
            going_forward=False
        ):
        '''
        Update the frontier for all given files, up to the given revision.

        Built for quick continuous _forward_ updating of large sets
        of files of TUIDs. Backward updating should be done through
        get_tuids(files, revision). If we cannot find a frontier, we will
        stop looking after max_csets_proc and update all files at the given
        revision.

        :param frontier_list: list of files to update
        :param revision: revision to update files to
        :param max_csets_proc: maximum number of changeset logs to look through
                               to find past frontiers.
        :param going_forward: If we know the requested revision is in front
                              of the latest revision use this flag. Used when
                              the frontier is too far away. If this is not set and
                              a frontier is too far, the latest revision will not
                              be updated.
        :return: list of (file, list(tuids)) tuples
        '''

        # Get the changelogs and revisions until we find the
        # last one we've seen, and get the modified files in
        # each one.

        # Holds the files modified up to the last frontiers.
        files_to_process = {}

        # Holds all frontiers to find
        remaining_frontiers = {cset for cset in list(set([frontier for _, frontier in frontier_list]))}

        if len(remaining_frontiers) <= 1 and frontier_list[0][1] == revision:
            # If the latest revision is the requested revision,
            # and there is only one frontier requested
            # continue to the tuid querys.
            remaining_frontiers = {}

        # Revision we are searching from
        final_rev = revision

        # If this exceeds max_csets_proc,
        # all frontiers not found are considered lost
        csets_proced = 0

        # Holds info on how to apply the diffs onto each frontier,
        # and all known frontiers.
        diffs_to_frontier = {cset: [] for cset in remaining_frontiers}

        Log.note("Searching for frontier(s): {{frontier}} ", frontier=str(list(remaining_frontiers)))
        Log.note(
            "Running on revision with HG URL: {{url}}",
            url=HG_URL / self.config.hg.branch / 'rev' / revision
        )

        while remaining_frontiers:
            # Get a changelog
            clog_url = HG_URL / self.config.hg.branch / 'json-log' / final_rev
            try:
                Log.note("Searching through changelog {{url}}", url=clog_url)
                clog_obj = http.get_json(clog_url, retry=RETRY)
                if isinstance(clog_obj, (text_type, str)):
                    Log.error(
                        "Revision {{cset}} does not exist in the {{branch}} branch",
                        cset=final_rev, branch=self.config.hg.branch
                    )
            except Exception as e:
                Log.error(
                    "Unexpected error getting changset-log for {{url}}: {{error}}",
                    url=clog_url,
                    error=e
                )

            # For each changeset in the log (except the last one
            # which is duplicated on the next log page requested.
            clog_obj_list = list(clog_obj['changesets'])
            for clog_cset in clog_obj_list[:-1]:
                nodes_cset = clog_cset['node'][:12]

                if remaining_frontiers:
                    if nodes_cset in remaining_frontiers:
                        # Found a frontier, remove it from search list.
                        remaining_frontiers.remove(nodes_cset)

                        if not remaining_frontiers:
                            # Found all frontiers, get out of the loop before
                            # we add the diff to a frontier update list.
                            break

                    # Add this diff to the processing list
                    # for each remaining frontier
                    for cset in diffs_to_frontier:
                        if cset in remaining_frontiers:
                            diffs_to_frontier[cset].append(nodes_cset)

            csets_proced += 1
            if not remaining_frontiers:
                # End searching
                break
            elif csets_proced >= max_csets_proc:
                # In this case, all files need to be updated to this revision to ensure
                # line ordering consistency (between past, and future) when a revision
                # that is in the past is asked for.
                files_to_process = {file: [revision] for file, _ in frontier_list}
                break
            else:
                # Go to the next log page
                last_entry = clog_obj_list[-1]
                final_rev = last_entry['node'][:12]
        remaining_frontiers = []
        if not remaining_frontiers:
            Log.note("Found all frontiers: {{frontiers_list}}", frontiers_list=str(list(diffs_to_frontier.keys())))
        else:
            found_frontiers = [
                frontier for frontier in diffs_to_frontier if frontier not in remaining_frontiers
            ]
            Log.note("Found frontiers: {{found}}", found=str(found_frontiers))
            Log.note("Did not find frontiers: {{not_found}}", not_found=str(list(remaining_frontiers)))

        added_files = {}
        removed_files = {}
        parsed_diffs = {}

        # This list is used to determine what files
        file_to_frontier = {file: frontier for file, frontier in frontier_list}
        if len(remaining_frontiers) != len(diffs_to_frontier.keys()):
            # If there is at least one frontier that was found
            # Only get diffs that are needed (if any frontiers were not found)
            diffs_cache = []
            for cset in diffs_to_frontier:
                if cset not in remaining_frontiers:
                    diffs_cache.extend([rev for revnum, rev in diffs_to_frontier[cset]])

            Log.note("Gathering diffs for: {{csets}}", csets=str(diffs_cache))
            all_diffs = self.get_diffs(diffs_cache)

            # Build a dict for faster access to the diffs,
            # to be used later when applying them.
            parsed_diffs = {diff_entry['cset']: diff_entry['diff'] for diff_entry in all_diffs}

            # In case the file name changes, this will map
            # the requested file to the new file name so
            # diffs can all be gathered.
            filenames_to_seek = {}

            # Parse diffs for files to process and store diffs to
            # apply for each file in files_to_process.
            added_and_removed_counts = {file: 1 for file in file_to_frontier}
            for csets_diff in all_diffs:
                cset_len12 = csets_diff['cset']
                parsed_diff = csets_diff['diff']['diffs']

                for f_added in parsed_diff:
                    # Get new entries for removed files.
                    new_name = f_added['new'].name.lstrip('/')
                    old_name = f_added['old'].name.lstrip('/')

                    # If we don't need this file, skip it
                    if new_name not in file_to_frontier and \
                       new_name not in filenames_to_seek:
                        if old_name not in file_to_frontier and \
                           old_name not in filenames_to_seek:
                            # File not requested
                            continue
                        if new_name == 'dev/null':
                            frontier_filename = old_name
                            while frontier_filename in filenames_to_seek:
                                frontier_filename = filenames_to_seek[frontier_filename]

                            if frontier_filename not in removed_files:
                                removed_files[frontier_filename] = 0
                            removed_files[frontier_filename] = added_and_removed_counts[frontier_filename]
                            added_and_removed_counts[frontier_filename] += 1
                            continue

                    if old_name == 'dev/null':
                        frontier_filename = new_name
                        while frontier_filename in filenames_to_seek:
                            frontier_filename = filenames_to_seek[frontier_filename]

                        if frontier_filename not in added_files:
                            added_files[frontier_filename] = 0
                        added_files[frontier_filename] = added_and_removed_counts[frontier_filename]
                        added_and_removed_counts[frontier_filename] += 1
                        continue
                    if new_name != old_name:
                        # File name was changed, keep the diff anyway
                        # to add any changes it makes.
                        filenames_to_seek[new_name] = old_name

                    # Get the originally requested file name
                    # by following filenames_to_seek entries
                    frontier_filename = new_name
                    while frontier_filename in filenames_to_seek:
                        frontier_filename = filenames_to_seek[frontier_filename]

                    # If we are past the frontier for this file,
                    # or if we are at the frontier skip it.
                    if file_to_frontier[frontier_filename] == '':
                        # Previously found frontier, skip
                        continue

                    # At this point, file is in the database, is
                    # asked to be processed, and we are still
                    # searching for the last frontier.
                    if file_to_frontier[frontier_filename] == cset_len12:
                        file_to_frontier[frontier_filename] = ''
                        # Found the frontier, skip
                        continue

                    if old_name != new_name:
                        Log.note(
                            "{{cset}} changes a requested file's name: {{file}} from {{oldfile}}. ",
                            file=new_name,
                            oldfile=old_name,
                            cset=cset
                        )

                    # Store the diff as it needs to be applied
                    if frontier_filename in files_to_process:
                        files_to_process[frontier_filename].append(cset_len12)
                    else:
                        files_to_process[frontier_filename] = [cset_len12]

        # Process each file that needs it based on the
        # files_to_process list.
        result = []
        ann_inserts = []
        latestFileMod_inserts = {}
        anns_to_get = []
        total = len(frontier_list)
        tmp_results = {}

        with self.conn.transaction() as transaction:
            for count, (file, old_frontier) in enumerate(frontier_list):
                if old_frontier in remaining_frontiers:
                    # If we were still looking for the frontier by the end, get a new
                    # annotation for this file.
                    anns_to_get.append(file)

                    if going_forward:
                        # If we are always going forward, update the frontier
                        latestFileMod_inserts[file] = (file, revision)

                    Log.note(
                        "Frontier update - can't find frontier {{lost_frontier}}: " +
                        "{{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ",
                        count=count,
                        total=total,
                        file=file,
                        rev=revision,
                        percent=count / total,
                        lost_frontier=old_frontier
                    )
                    continue
                elif file in removed_files or file in added_files:
                    if file not in removed_files:
                        removed_files[file] = 0
                    if file not in added_files:
                        added_files[file] = 0

                    if removed_files[file] <= added_files[file]:
                        # For it to still exist it has to be
                        # added last (to give it a larger count)
                        anns_to_get.append(file)
                        Log.note(
                            "Frontier update - adding: " +
                            "{{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ",
                            count=count,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total,
                            lost_frontier=old_frontier
                        )
                    else:
                        Log.note(
                            "Frontier update - deleting: " +
                            "{{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ",
                            count=count,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total,
                            lost_frontier=old_frontier
                        )
                        tmp_results[file] = []
                    if going_forward:
                        # If we are always going forward, update the frontier
                        latestFileMod_inserts[file] = (file, revision)

                    continue

                # If the file was modified, get it's newest
                # annotation and update the file.
                tmp_res = None
                if file in files_to_process:
                    # Process this file using the diffs found
                    tmp_ann = self._get_annotation(old_frontier, file, transaction)
                    if tmp_ann is None or tmp_ann == '' or self.destringify_tuids(tmp_ann) is None:
                        Log.warning(
                            "{{file}} has frontier but can't find old annotation for it in {{rev}}, " +
                            "restarting it's frontier.",
                            rev=old_frontier,
                            file=file
                        )
                        anns_to_get.append(file)
                    else:
                        # File was modified, apply it's diffs
                        # Reverse the diff list, we always find the newest diff first
                        csets_to_proc = files_to_process[file][::-1]
                        tmp_res = self.destringify_tuids(tmp_ann)
                        new_fname = file
                        for i in csets_to_proc:
                            tmp_res, new_fname = self._apply_diff(transaction, tmp_res, parsed_diffs[i], i, new_fname)

                        ann_inserts.append((revision, file, self.stringify_tuids(tmp_res)))
                        Log.note(
                            "Frontier update - modified: {{count}}/{{total}} - {{percent|percent(decimal=0)}} " +
                            "| {{rev}}|{{file}} ",
                            count=count,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total
                        )
                else:
                    old_ann = self._get_annotation(old_frontier, file, transaction)
                    if old_ann is None or (old_ann == '' and file in added_files):
                        # File is new (likely from an error), or re-added - we need to create
                        # a new initial entry for this file.
                        anns_to_get.append(file)
                        Log.note(
                            "Frontier update - readded: {{count}}/{{total}} - {{percent|percent(decimal=0)}} " +
                            "| {{rev}}|{{file}} ",
                            count=count,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total
                        )
                    else:
                        # File was not modified since last
                        # known revision
                        tmp_res = self.destringify_tuids(old_ann) if old_ann != '' else []
                        ann_inserts.append((revision, file, old_ann))
                        Log.note(
                            "Frontier update - not modified: {{count}}/{{total}} - {{percent|percent(decimal=0)}} " +
                            "| {{rev}}|{{file}} ",
                            count=count,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total
                        )

                if tmp_res:
                    tmp_results[file] = tmp_res
                else:
                    Log.note(
                        "Error occured for file {{file}} in revision {{revision}}",
                        file=file,
                        revision=revision
                    )
                    tmp_results[file] = []

                # If we have found all frontiers, update to the
                # latest revision. Otherwise, the requested
                # revision is too far away (can't be sure
                # if it's past). Unless we are told that we are
                # going forward.
                if going_forward or not remaining_frontiers:
                    latest_rev = revision
                else:
                    latest_rev = old_frontier
                latestFileMod_inserts[file] = (file, latest_rev)

            Log.note("Updating DB tables `latestFileMod` and `annotations`...")

            # No need to double-check if latesteFileMods has been updated before,
            # we perform an insert or replace any way.
            if len(latestFileMod_inserts) > 0:
                for _, inserts_list in jx.groupby(latestFileMod_inserts.values(), size=SQL_BATCH_SIZE):
                    transaction.execute(
                        "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
                        sql_list(sql_iso(sql_list(map(quote_value, i))) for i in inserts_list)
                    )

            anns_added_by_other_thread = {}
            if len(ann_inserts) > 0:
                ann_inserts = list(set(ann_inserts))
                for _, tmp_inserts in jx.groupby(ann_inserts, size=SQL_ANN_BATCH_SIZE):
                    # Check if any were added in the mean time by another thread
                    recomputed_inserts = []
                    for rev, filename, string_tuids in tmp_inserts:
                        tmp_ann = self._get_annotation(rev, filename, transaction)
                        if not tmp_ann or tmp_ann == '':
                            recomputed_inserts.append((rev, filename, string_tuids))
                        else:
                            anns_added_by_other_thread[filename] = self.destringify_tuids(tmp_ann)

                    if len(recomputed_inserts) <= 0:
                        continue

                    try:
                        for rev, filename, tuids_ann in recomputed_inserts:
                            tmp_ann = self.destringify_tuids(tuids_ann)
                            for tuid_map in tmp_ann:
                                if tuid_map is None or tuid_map.tuid is None or tuid_map.line is None:
                                    Log.warning(
                                        "None value encountered in annotation insertion in " +
                                        "{{rev}} for {{file}}: {{tuids}}" ,
                                        rev=rev, file=filename, tuids=str(tuid_map)
                                    )
                        self.insert_annotations(transaction, recomputed_inserts)
                    except Exception as e:
                        Log.error(
                            "Error inserting into annotations table: {{inserting}} caused by: {{cause}}",
                            inserting=recomputed_inserts,
                            cause=e
                        )

        if len(anns_to_get) > 0:
            result.extend(self.get_tuids(anns_to_get, revision, commit=False))

        for f in tmp_results:
            tuids = tmp_results[f]
            if f in anns_added_by_other_thread:
                tuids = anns_added_by_other_thread[f]
            result.append((f, tuids))
        return result


    def _update_file_changesets(self, transaction, annotated_lines):
        '''
        Inserts new lines from all changesets in the given annotation.

        :param annotated_lines: Response from annotation request from HGMO
        :return: None
        '''
        quickfill_list = []

        for anline in annotated_lines:
            cset = anline['node'][:12]
            if not self._get_one_tuid(transaction, cset, anline['abspath'], int(anline['targetline'])):
                quickfill_list.append((cset, anline['abspath'], int(anline['targetline'])))
        self._quick_update_file_changeset(transaction, list(set(quickfill_list)))


    def _quick_update_file_changeset(self, transaction, qf_list):
        '''
        Updates temporal table to include any new TUIDs.

        :param qf_list: List to insert
        :return: None
        '''
        for _, tmp_qf_list in jx.groupby(qf_list, size=SQL_BATCH_SIZE):
            transaction.execute(
                "INSERT INTO temporal (tuid, revision, file, line)" +
                " VALUES " +
                sql_list(sql_iso(sql_list(map(quote_value, (self.tuid(), i[0], i[1], i[2])))) for i in tmp_qf_list)
            )

    def get_tuids(self, files, revision, commit=True, chunk=50, repo=None):
        '''
        Wrapper for `_get_tuids` to limit the number of annotation calls to hg
        and separate the calls from DB transactions. Also used to simplify `_get_tuids`.

        :param files:
        :param revision:
        :param commit:
        :param chunk:
        :param repo:
        :return:
        '''
        results = []
        revision = revision[:12]

        # For a single file, there is no need
        # to put it in an array when given.
        if not isinstance(files, list):
            files = [files]
        if repo is None:
            repo = self.config.hg.branch

        for _, new_files in jx.groupby(files, size=chunk):
            for count, file in enumerate(new_files):
                new_files[count] = file.lstrip('/')

            annotations_to_get = []
            for file in new_files:
                with self.conn.transaction() as t:
                    already_ann = self._get_annotation(revision, file, transaction=t)
                if already_ann:
                    results.append((file, self.destringify_tuids(already_ann)))
                elif already_ann == '':
                    results.append((file, []))
                else:
                    annotations_to_get.append(file)

            if not annotations_to_get:
                # No new annotations to get, so get next set
                continue

            # Get all the annotations in parallel and
            # store in annotated_files
            annotated_files = [None] * len(annotations_to_get)
            threads = [
                Thread.run(
                    str(thread_count),
                    self._get_hg_annotate,
                    revision,
                    annotations_to_get[thread_count],
                    annotated_files,
                    thread_count,
                    repo
                )
                for thread_count, _ in enumerate(annotations_to_get)
            ]
            for t in threads:
                t.join()

            # Help for memory, because `chunk` (or a lot of)
            # threads are started at once.
            del threads

            with self.conn.transaction() as transaction:
                results.extend(
                    self._get_tuids(
                        transaction, annotations_to_get, revision, annotated_files, commit=commit, repo=repo
                    )
                )

        # Help for memory
        gc.collect()
        return results

    def _get_tuids(
            self,
            transaction,
            files,
            revision,
            annotated_files,
            commit=True,
            repo=None
        ):
        '''
        Returns (TUID, line) tuples for a given file at a given revision.

        Uses json-annotate to find all lines in this revision, then it updates
        the database with any missing revisions for the file changes listed
        in annotate. Then, we use the information from annotate coupled with the
        diff information that was inserted into the DB to return TUIDs. This way
        we don't have to deal with child, parents, dates, etc..

        :param files: list of files to process
        :param revision: revision at which to get the file
        :param annotated_files: annotations for each file
        :param commit: True to commit new TUIDs else False
        :param repo: The branch to get tuids from
        :return: List of TuidMap objects
        '''
        results = []

        for fcount, annotated_object in enumerate(annotated_files):
            file = files[fcount]

            # If it's not defined at this revision, we need to add it in
            errored = False
            if isinstance(annotated_object, (text_type, str)):
                errored = True
                Log.warning(
                    "{{file}} does not exist in the revision={{cset}} branch={{branch_name}}",
                    branch_name=repo,
                    cset=revision,
                    file=file
                )
            elif annotated_object is None:
                Log.warning(
                    "Unexpected error getting annotation for: {{file}} in the revision={{cset}} branch={{branch_name}}",
                    branch_name=repo,
                    cset=revision,
                    file=file
                )
                errored = True
            elif 'annotate' not in annotated_object:
                Log.warning(
                    "Missing annotate, type got: {{ann_type}}, expecting:dict returned when getting " +
                    "annotation for: {{file}} in the revision {{cset}}",
                    cset=revision, file=file, ann_type=type(annotated_object)
                )
                errored = True

            if errored:
                Log.note("Inserting dummy entry...")
                self.insert_tuid_dummy(transaction, revision, file, commit=commit)
                self.insert_annotate_dummy(transaction, revision, file, commit=commit)
                results.append((file, []))
                continue

            # Gather all missing csets and the
            # corresponding lines.
            annotated_lines = []
            line_origins = []
            existing_tuids = {}
            for node in annotated_object['annotate']:
                cset_len12 = node['node'][:12]

                # If the line added by `cset_len12` is not known
                # add it. Use the 'abspath' field to determine the
                # name of the file it was created in (in case it was
                # changed).
                tuid_tmp = transaction.get_one(GET_TUID_QUERY, (node['abspath'], cset_len12, int(node['targetline'])))
                if (not tuid_tmp):
                    annotated_lines.append(node)
                else:
                    existing_tuids[int(node['lineno'])] = tuid_tmp[0]
                # Used to gather TUIDs later
                line_origins.append((node['abspath'], cset_len12, int(node['targetline'])))

            # Update DB with any revisions found in annotated
            # object that are not in the DB.
            if len(annotated_lines) > 0:
                # If we are using get_tuids within another transaction
                try:
                    self._update_file_changesets(transaction, annotated_lines)
                except Exception as e:
                    # Something broke for this file, ignore it and go to the
                    # next one.
                    Log.note("Failed to insert new tuids", cause=e)
                    continue


            # Get the TUIDs for each line (can probably be optimized with a join)
            tuids = []
            errored = False
            for line_ind, line_origin in enumerate(line_origins):
                line_num = line_ind + 1
                if line_num in existing_tuids:
                    tuids.append(TuidMap(existing_tuids[line_num], line_num))
                    continue
                try:
                    tuid_tmp = transaction.get_one(GET_TUID_QUERY, line_origins[line_ind])

                    # Return dummy line if we can't find the TUID for this entry
                    # (likely because of an error from insertion).
                    if tuid_tmp:
                        tuids.append(TuidMap(tuid_tmp[0], line_num))
                    else:
                        tuids.append(MISSING)
                except Exception as e:
                    Log.note("Unexpected error searching for tuids {{cause}}", cause=e)
                    errored = True
                    break
            if errored:
                # Error searching for tuids, go to next file
                continue

            # Make sure we are not adding the same thing another thread
            # added.
            tmp_ann = self._get_annotation(revision, file, transaction=transaction)
            if tmp_ann:
                results.append((file, self.destringify_tuids(tmp_ann)))
                continue

            self.insert_annotations(
                transaction,
                [(
                    revision,
                    file,
                    self.stringify_tuids(tuids)
                )]
            )
            results.append((file, tuids))

        return results


    def _daemon(self, please_stop, only_coverage_revisions=False):
        '''
        Runs continuously to prefill the temporal and
        annotations table with the coverage revisions*.

        * A coverage revision is a revision which has had
        code coverage run on it.

        :param please_stop: Used to stop the daemon
        :return: None
        '''
        while not please_stop:
            # Get all known files and their latest revisions on the frontier
            files_n_revs = self.conn.get("SELECT file, revision FROM latestFileMod")

            # Split these files into groups of revisions to make it
            # easier to update them. If we group them together, we
            # may end up updating groups that are new back to older
            # revisions.
            revs = {rev: [] for rev in set([file_n_rev[1] for file_n_rev in files_n_revs])}
            for file_n_rev in files_n_revs:
                revs[file_n_rev[1]].append(file_n_rev[0])

            # Go through each frontier and update it
            ran_changesets = False
            coverage_revisions = None
            for frontier in revs:
                if please_stop:
                    return

                files = revs[frontier]

                # Go through changeset logs until we find the last
                # known frontier for this revision group.
                csets = []
                final_rev = ''
                found_last_frontier = False
                Log.note("Searching for frontier: {{frontier}} ", frontier=frontier)
                Log.note("HG URL: {{url}}", url=HG_URL / self.config.hg.branch / 'rev' / frontier)
                while not found_last_frontier:
                    # Get a changelog
                    clog_url = HG_URL / self.config.hg.branch / 'json-log' / final_rev
                    try:
                        clog_obj = http.get_json(clog_url, retry=RETRY)
                    except Exception as e:
                        Log.error("Unexpected error getting changset-log for {{url}}", url=clog_url, error=e)

                    cset = ''
                    still_looking = True
                    # For each changeset/node
                    for clog_cset in clog_obj['changesets']:
                        cset = clog_cset['node'][:12]
                        if cset == frontier:
                            still_looking = False
                            break
                        csets.append(cset)

                    if not still_looking:
                        found_last_frontier = True
                    final_rev = cset

                # No csets found means that we are already
                # at the latest revisions.
                if len(csets) == 0:
                    continue

                # Get all the latest ccov and jsdcov revisions
                if (not coverage_revisions) and only_coverage_revisions:
                    active_data_url = 'http://activedata.allizom.org/query'
                    query_json = {
                        "limit": 1000,
                        "from": "task",
                        "where": {"and": [
                            {"in": {"build.type": ["ccov", "jsdcov"]}},
                            {"gte": {"run.timestamp": {"date": "today-day"}}},
                            {"eq": {"repo.branch.name": self.config.hg.branch}}
                        ]},
                        "select": [
                            {"aggregate": "min", "value": "run.timestamp"},
                            {"aggregate": "count"}
                        ],
                        "groupby": ["repo.changeset.id12"]
                    }
                    coverage_revisions_resp = http.post_json(active_data_url, retry=RETRY, data=query_json)
                    coverage_revisions = [rev_arr[0] for rev_arr in coverage_revisions_resp.data]

                # Reverse changeset list and for each code coverage revision
                # found by going through the list from oldest to newest,
                # update _all known_ file frontiers to that revision.
                csets.reverse()
                prev_cset = frontier
                for cset in csets:
                    if please_stop:
                        return
                    if only_coverage_revisions:
                        if cset not in coverage_revisions:
                            continue
                    if DEBUG:
                        Log.note("Moving frontier {{frontier}} forward to {{cset}}.", frontier=prev_cset, cset=cset)

                    # Update files
                    self.get_tuids_from_files(files, cset, going_forward=True)

                    ran_changesets = True
                    prev_cset = cset

            if not ran_changesets:
                (please_stop | Till(seconds=DAEMON_WAIT_AT_NEWEST.seconds)).wait()
