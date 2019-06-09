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
import copy

from jx_python import jx
from mo_dots import Null, coalesce, wrap, set_default
from mo_future import text_type
from mo_hg.hg_mozilla_org import HgMozillaOrg
from mo_hg.apply import apply_diff, apply_diff_backwards
from mo_files.url import URL
from mo_kwargs import override
from mo_logs import Log
from mo_logs.exceptions import suppress_exception
from mo_math.randoms import Random
from mo_threads import Till, Thread, Lock
from mo_times.durations import SECOND, HOUR, MINUTE, DAY
from pyLibrary.env import http, elasticsearch
from pyLibrary.meta import cache
from pyLibrary.sql import sql_list, sql_iso
from pyLibrary.sql.sqlite import quote_value, quote_list
from tuid import sql
from tuid.statslogger import StatsLogger
from tuid.counter import Counter
from tuid.util import MISSING, TuidMap, TuidLine, AnnotateFile, HG_URL
from mo_json import json2value, value2json

import tuid.clogger

DEBUG = False
ANNOTATE_DEBUG = False
VERIFY_TUIDS = True
RETRY = {"times": 3, "sleep": 5, "http": True}
ANN_WAIT_TIME = 5 * HOUR
MEMORY_LOG_INTERVAL = 15
MAX_CONCURRENT_ANN_REQUESTS = 5
MAX_ANN_REQUESTS_WAIT_TIME = 5 * SECOND
MAX_THREAD_WAIT_TIME = 5 * SECOND
WORK_OVERFLOW_BATCH_SIZE = 250
SQL_ANN_BATCH_SIZE = 5
SQL_BATCH_SIZE = 500
FILES_TO_PROCESS_THRESH = 5
ENABLE_TRY = False
DAEMON_WAIT_AT_NEWEST = (
    30 * SECOND
)  # Time to wait at the newest revision before polling again.

GET_LATEST_MODIFICATION = "SELECT revision FROM latestFileMod WHERE file=?"


class TUIDService:
    @override
    def __init__(
        self,
        database,
        hg,
        hg_cache=None,
        conn=None,
        clogger=None,
        start_workers=True,
        kwargs=None,
    ):
        try:
            self.config = kwargs

            self.conn = conn if conn else sql.Sql(self.config.database.name)
            self.hg_cache = (
                HgMozillaOrg(kwargs=self.config.hg_cache, use_cache=True)
                if self.config.hg_cache
                else Null
            )
            self.hg_url = URL(hg.url)

            self.esconfig = self.config.esservice
            self.es_temporal = elasticsearch.Cluster(kwargs=self.esconfig.temporal)
            self.es_annotations = elasticsearch.Cluster(
                kwargs=self.esconfig.annotations
            )

            if not self.conn.get_one(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ):
                self.init_db()
            else:
                self.init_db(True)

            self.locker = Lock()
            self.request_locker = Lock()
            self.ann_thread_locker = Lock()
            self.service_thread_locker = Lock()
            self.count_locker = Counter()
            self.num_requests = 0
            self.ann_threads_running = 0
            self.service_threads_running = 0
            query = {"size": 0, "aggs": {"value": {"max": {"field": "tuid"}}}}
            self.next_tuid = int(
                coalesce(
                    eval(str(self.temporal.search(query).aggregations.value.value)), 0
                )
                + 1
            )
            self.total_locker = Lock()
            self.temporal_locker = Lock()
            self.total_files_requested = 0
            self.total_tuids_mapped = 0

            self.statsdaemon = StatsLogger()
            self.clogger = (
                clogger
                if clogger
                else tuid.clogger.Clogger(
                    conn=self.conn,
                    tuid_service=self,
                    start_workers=start_workers,
                    kwargs=kwargs,
                )
            )
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

    def init_db(self, temporal_only=False):
        """
        Creates all the tables, and indexes needed for the service.

        :return: None
        """

        temporal = self.esconfig.temporal
        set_default(temporal, {"schema": TEMPORAL_SCHEMA})
        # what would be the _id here
        self.temporal = self.es_temporal.get_or_create_index(kwargs=temporal)
        self.temporal.refresh()

        total = self.temporal.search({"size": 0})
        while not total.hits:
            total = self.temporal.search({"size": 0})
        with suppress_exception:
            self.temporal.add_alias()

        annotations = self.esconfig.annotations
        set_default(annotations, {"schema": ANNOTATIONS_SCHEMA})
        # what would be the _id here
        self.annotations = self.es_annotations.get_or_create_index(kwargs=annotations)
        self.annotations.refresh()

        total = self.annotations.search({"size": 0})
        while not total.hits:
            total = self.annotations.search({"size": 0})
        with suppress_exception:
            self.annotations.add_alias()
        if temporal_only:
            return

        with self.conn.transaction() as t:

            # Used in frontier updating
            t.execute(
                """
            CREATE TABLE latestFileMod (
                file           TEXT,
                revision       CHAR(12) NOT NULL,
                PRIMARY KEY(file)
            );"""
            )

        Log.note("Tables created successfully")

    def _query_result_size(self, terms):
        query = {"size": 0, "query": {"terms": terms}}
        return query

    def _dummy_tuid_exists(self, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        query = {
            "_source": {"includes": ["tuid"]},
            "query": {
                "bool": {
                    "must": [
                        {"term": {"file": file_name}},
                        {"term": {"revision": rev}},
                        {"term": {"line": 0}},
                    ]
                }
            },
            "size": 0,
        }
        temp = self.temporal.search(query).hits.total
        return 0 != temp

    def _dummy_annotate_exists(self, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        query = {
            "_source": {"includes": ["annotation"]},
            "query": {
                "bool": {
                    "must": [{"term": {"file": file_name}}, {"term": {"revision": rev}}]
                }
            },
            "size": 1,
        }
        temp = self.annotations.search(query).hits.total
        return 0 != temp

    def _make_record_temporal(self, tuid, revision, file, line):
        record = {
            "_id": revision + file + str(line),
            "tuid": tuid,
            "revision": revision,
            "file": file,
            "line": line,
        }
        return {"value": record}

    def _make_record_annotations(self, revision, file, annotation):
        record = {
            "_id": revision + file,
            "revision": revision,
            "file": file,
            "annotation": annotation,
        }
        return {"value": record}

    def insert_tuid_dummy(self, rev, file_name):
        # Inserts a dummy tuid: (-1,rev,file_name,0)
        if not self._dummy_tuid_exists(file_name, rev):
            self.temporal.add(self._make_record_temporal(-1, rev[:12], file_name, 0))
            self.temporal.refresh()
            while not self._dummy_tuid_exists(file_name, rev[:12]):
                Till(seconds=0.001).wait()
                self.temporal.refresh()
        return MISSING

    def insert_annotate_dummy(self, rev, file_name):
        # Inserts annotation dummy: (rev, file, '')
        if not self._dummy_annotate_exists(file_name, rev):
            self.insert_annotations([(rev[:12], file_name, "")])

    def insert_annotations(self, data):
        if VERIFY_TUIDS:
            for _, _, tuids_string in data:
                self.destringify_tuids(tuids_string)

        records = []
        ids = []
        for row in data:
            revision, file, annotation = row
            record = self._make_record_annotations(revision, file, annotation)
            records.append(record)
            ids.append(record["value"]["_id"])

        self.annotations.extend(records)
        query = self._query_result_size({"_id": ids})
        self.annotations.refresh()
        while self.annotations.search(query).hits.total != len(ids):
            Till(seconds=0.001).wait()
            self.annotations.refresh()

    def _annotation_record_exists(self, rev, file):
        query = {
            "_source": {"includes": ["revision"]},
            "query": {
                "bool": {
                    "must": [{"term": {"revision": rev}}, {"term": {"file": file}}]
                }
            },
            "size": 1,
        }
        temp = self.annotations.search(query).hits.hits[0]._source.revision
        return temp

    def _get_annotation(self, rev, file):
        query = {
            "_source": {"includes": ["annotation"]},
            "query": {
                "bool": {
                    "must": [{"term": {"revision": rev}}, {"term": {"file": file}}]
                }
            },
            "size": 1,
        }
        temp = self.annotations.search(query).hits.hits[0]._source.annotation
        return temp

    def _get_one_tuid(self, cset, path, line):
        # Returns a single TUID if it exists else None
        query = {
            "_source": {"includes": ["tuid"]},
            "query": {
                "bool": {
                    "must": [
                        {"term": {"file": path}},
                        {"term": {"revision": cset}},
                        {"term": {"line": line}},
                    ]
                }
            },
            "size": 1,
        }
        temp = self.temporal.search(query).hits.hits[0]._source.tuid

        query = {
            "_source": {"includes": ["annotation"]},
            "query": {
                "bool": {
                    "must": [{"term": {"revision": cset}}, {"term": {"file": path}}]
                }
            },
            "size": 1,
        }
        temp = self.annotations.search(query).hits.hits[0]._source.annotation[line-1]
        return temp

    def _get_latest_revision(self, file, transaction):
        # Returns the latest revision that we
        # have information on the requested file.
        return coalesce(transaction, self.conn).get_one(
            GET_LATEST_MODIFICATION, (file,)
        )

    def stringify_tuids(self, tuid_list):
        # Turns the TuidMap list to a sorted list
        tuid_list.sort(key=lambda x: x.line)
        ordered_tuid = [-1] * len(tuid_list)
        #checks any line number is missing
        for tuid, line in tuid_list:
            ordered_tuid[line - 1] = tuid

        return ordered_tuid

    def destringify_tuids(self, tuids_list):
        # Builds up TuidMap list from annotation cache entry.
        try:
            line_origins = [
                TuidMap(tuid, line + 1)
                for line, tuid in enumerate(
                    tuids_list
                )
            ]

            return line_origins
        except Exception as e:
            Log.error(
                "Invalid entry in tuids list:\n{{list}}", list=tuids_list, cause=e
            )

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
            wrap({"changeset": {"id": cset}, "branch": {"name": repo}}),
            None,
            False,
            True,
        )
        output = tmp["changeset"]["moves"]
        output2 = {}
        output2["diffs"] = output

        merge_description = tmp["changeset"]["description"]
        output2["merge"] = check_merge(merge_description)
        return output2

    # Gets an annotated file from a particular revision from https://hg.mozilla.org/
    def _get_hg_annotate(
        self, cset, file, annotated_files, thread_num, repo, please_stop=None
    ):
        with self.ann_thread_locker:
            self.ann_threads_running += 1
        url = str(HG_URL) + "/" + repo + "/json-annotate/" + cset + "/" + file
        if DEBUG:
            Log.note("HG: {{url}}", url=url)

        # Wait until there is room to request
        self.statsdaemon.update_anns_waiting(1)
        num_requests = MAX_CONCURRENT_ANN_REQUESTS
        timeout = Till(seconds=ANN_WAIT_TIME.seconds)
        while num_requests >= MAX_CONCURRENT_ANN_REQUESTS and not timeout:
            with self.request_locker:
                num_requests = self.num_requests
                if num_requests < MAX_CONCURRENT_ANN_REQUESTS:
                    self.num_requests += 1
                    break
            if ANNOTATE_DEBUG:
                Log.note(
                    "Waiting to request annotation at {{rev}} for file: {{file}}",
                    rev=cset,
                    file=file,
                )
            Till(seconds=MAX_ANN_REQUESTS_WAIT_TIME.seconds).wait()
        self.statsdaemon.update_anns_waiting(-1)

        annotated_files[thread_num] = []
        if not timeout:
            try:
                annotated_files[thread_num] = http.get_json(url, retry=RETRY)
            except Exception as e:
                Log.warning(
                    "Unexpected error while trying to get annotate for {{url}}",
                    url=url,
                    cause=e,
                )
            finally:
                with self.request_locker:
                    self.num_requests -= 1
        else:
            Log.warning(
                "Timeout {{timeout}} exceeded waiting for annotation: {{url}}",
                timeout=ANN_WAIT_TIME,
                url=url,
            )
        with self.ann_thread_locker:
            self.ann_threads_running -= 1
        return

    def get_diffs(self, csets, repo=None):
        # Get all the diffs
        if repo is None:
            repo = self.config.hg.branch

        list_diffs = []
        for cset in csets:
            list_diffs.append(
                {"cset": cset, "diff": self._get_hg_diff(cset, repo=repo)}
            )
        return list_diffs

    def get_tuids_from_revision(self, revision):
        """
        Gets the TUIDs for the files modified by a revision.

        :param revision: revision to get files from
        :return: list of (file, list(tuids)) tuples
        """
        result = []
        URL_TO_FILES = (
            str(HG_URL) + "/" + self.config.hg.branch + "/json-info/" + revision
        )
        try:
            mozobject = http.get_json(url=URL_TO_FILES, retry=RETRY)
        except Exception as e:
            Log.warning(
                "Unexpected error trying to get file list for revision {{revision}}",
                cause=e,
            )
            return None

        files = mozobject[revision]["files"]

        results = self.get_tuids(files, revision)
        return results

    @cache(duration=30 * MINUTE)
    def get_clog(self, clog_url):
        clog_obj = http.get_json(clog_url, retry=RETRY)
        return clog_obj

    @cache(duration=30 * MINUTE)
    def _check_branch(self, revision, branch):
        """
        Used to find out if the revision is in the given branch.

        :param revision: Revision to check.
        :param branch: Branch to check revision on.
        :return: True/False - Found it/Didn't find it
        """

        # Get a changelog
        res = True
        clog_url = str(HG_URL) + "/" + branch + "/json-log/" + revision
        clog_obj = None
        try:
            Log.note("Searching through changelog {{url}}", url=clog_url)
            clog_obj = self.get_clog(clog_url)
            if isinstance(clog_obj, (text_type, str)):
                Log.note(
                    "Revision {{cset}} does not exist in the {{branch}} branch",
                    cset=revision,
                    branch=branch,
                )
                res = False
        except Exception as e:
            Log.note(
                "Unexpected error getting changset-log for {{url}}: {{error}}",
                url=clog_url,
                error=e,
            )
            res = False
        return res

    def mthread_testing_get_tuids_from_files(
        self,
        files,
        revision,
        results,
        res_position,
        going_forward=False,
        repo=None,
        please_stop=None,
    ):
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
        results[res_position], _ = self.get_tuids_from_files(
            files, revision, going_forward=going_forward, repo=repo
        )
        Log.note("Thread {{pos}} is ending.", pos=res_position)
        return

    def _add_thread(self):
        with self.service_thread_locker:
            self.service_threads_running += 1

    def _remove_thread(self):
        with self.service_thread_locker:
            self.service_threads_running -= 1

    def get_thread_count(self):
        with self.service_thread_locker:
            threads_running = self.service_threads_running
        return threads_running

    def get_tuids_from_files(
        self,
        files,
        revision,
        going_forward=False,
        repo=None,
        use_thread=True,
        max_csets_proc=30,
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
        
        The following is a very simplified overview of how this function works:
        (1) When a file is requested, we check if it exists in the annotations
            and latestFileMods table.
            (i) If not, we get the annotation from hg.mozilla.org and give tuids
                to each of the lines and return this as a result.
        (2) If it does exist but it's at an older or newer revision,
            then we get it's frontier, which is the latest revision of the file available
            in the annotations table.
        (3) Using that frontier, we use the Clogger (csetLog table) to get us a range of
            revisions that we have to apply to the file at the given frontier to either
            move it forwards or backwards in time.
        (4) After this diff application stage, we now have tuids for the modified file.

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
        self._add_thread()
        completed = True

        if repo is None:
            repo = self.config.hg.branch
            check = self._check_branch(revision, repo)
            if not check:
                # Error was already output by _check_branch
                self._remove_thread()
                return [(file, []) for file in files], completed

        if repo in ("try",):
            # We don't need to keep latest file revisions
            # and other related things for this condition.

            # Enable the 'try' repo calls with ENABLE_TRY
            if ENABLE_TRY:
                result = (
                    self._get_tuids_from_files_try_branch(files, revision),
                    completed,
                )
            else:
                result = [(file, []) for file in files], completed

            self._remove_thread()
            return result

        result = []
        revision = revision[:12]
        files = [file.lstrip("/") for file in files]
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
                Log.note(
                    " {{percent|percent(decimal=0)}}|{{file}}",
                    file=file,
                    percent=count / total,
                )

            with self.conn.transaction() as t:
                latest_rev = self._get_latest_revision(file, t)
                already_ann = self._get_annotation(revision, file)

            # Check if the file has already been collected at
            # this revision and get the result if so
            if already_ann:
                result.append((file, self.destringify_tuids(already_ann)))
                latestFileMod_inserts[file] = (file, revision)
                log_existing_files.append("exists|" + file)
                continue
            elif already_ann == "":
                result.append((file, []))
                latestFileMod_inserts[file] = (file, revision)
                log_existing_files.append("removed|" + file)
                continue

            if latest_rev and latest_rev[0] != revision:
                # File has a frontier, let's update it
                if DEBUG:
                    Log.note("Will update frontier for file {{file}}.", file=file)
                frontier_update_list.append((file, latest_rev[0]))
            elif latest_rev == revision:
                with self.conn.transaction() as t:
                    t.execute(
                        "DELETE FROM latestFileMod WHERE file = " + quote_value(file)
                    )
                new_files.append(file)
                Log.note(
                    "Missing annotation for existing frontier - readding: "
                    "{{rev}}|{{file}} ",
                    file=file,
                    rev=revision,
                )
            else:
                Log.note(
                    "Frontier update - adding: " "{{rev}}|{{file}} ",
                    file=file,
                    rev=revision,
                )
                new_files.append(file)

        if DEBUG:
            Log.note(
                "Frontier update - already exist in DB: " "{{rev}} || {{file_list}} ",
                file_list=str(log_existing_files),
                rev=revision,
            )
        else:
            Log.note(
                "Frontier update - already exist in DB for {{rev}}: "
                "{{count}}/{{total}} | {{percent|percent}}",
                count=str(len(log_existing_files)),
                total=str(len(files)),
                rev=revision,
                percent=len(log_existing_files) / len(files),
            )

        if len(latestFileMod_inserts) > 0:
            with self.conn.transaction() as transaction:
                for _, inserts_list in jx.groupby(
                    latestFileMod_inserts.values(), size=SQL_BATCH_SIZE
                ):
                    transaction.execute(
                        "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES "
                        + sql_list(quote_list(i) for i in inserts_list)
                    )

        def update_tuids_in_thread(
            new_files, frontier_update_list, revision, using_thread, please_stop=None
        ):
            # Processes the new files and files which need their frontier updated
            # outside of the main thread as this can take a long time.

            result = []
            try:
                latestFileMod_inserts = {}
                if len(new_files) > 0:
                    # File has never been seen before, get it's initial
                    # annotation to work from in the future.
                    tmp_res = self.get_tuids(new_files, revision)
                    if tmp_res:
                        result.extend(tmp_res)
                    else:
                        Log.note(
                            "Error occured for files "
                            + str(new_files)
                            + " in revision "
                            + revision
                        )

                    # If this file has not been seen before,
                    # add it to the latest modifications, else
                    # it's already in there so update its past
                    # revisions.
                    for file in new_files:
                        latestFileMod_inserts[file] = (file, revision)

                Log.note(
                    "Finished updating frontiers. Updating DB table `latestFileMod`..."
                )
                if len(latestFileMod_inserts) > 0:
                    with self.conn.transaction() as transaction:
                        for _, inserts_list in jx.groupby(
                            latestFileMod_inserts.values(), size=SQL_BATCH_SIZE
                        ):
                            transaction.execute(
                                "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES "
                                + sql_list(quote_list(i) for i in inserts_list)
                            )

                # If we have files that need to have their frontier updated, do that now
                if len(frontier_update_list) > 0:
                    tmp = self._update_file_frontiers(
                        frontier_update_list,
                        revision,
                        going_forward=going_forward,
                        max_csets_proc=max_csets_proc,
                    )
                    result.extend(tmp)

            except Exception as e:
                Log.warning("Thread dead becasue of problem", cause=e)
                result = [[] for _ in range(len(new_files) + len(frontier_update_list))]
            finally:
                self._remove_thread()

                if using_thread:
                    self.statsdaemon.update_totals(0, len(result))

                Log.note("Completed work overflow for revision {{cset}}", cset=revision)
                return result

        threaded = False
        if use_thread:
            # If there are too many files to process, start a thread to do
            # that work and return completed as False.
            if len(new_files) + len(frontier_update_list) > FILES_TO_PROCESS_THRESH:
                threaded = True

        if threaded:
            completed = False
            Log.note("Incomplete response given")

            thread_count = 0
            prev_ind = 0
            curr_ind = 0

            while curr_ind <= len(frontier_update_list) or curr_ind <= len(new_files):
                thread_count += 1
                prev_ind = curr_ind
                curr_ind += WORK_OVERFLOW_BATCH_SIZE
                recomputed_new = new_files[prev_ind:curr_ind]
                recomputed_frontier_updates = frontier_update_list[prev_ind:curr_ind]
                Thread.run(
                    "get_tuids_from_files (" + Random.base64(9) + ")",
                    update_tuids_in_thread,
                    recomputed_new,
                    recomputed_frontier_updates,
                    revision,
                    threaded,
                )
            for _ in range(1, thread_count):  # Skip the first thread
                self._add_thread()
        else:
            result.extend(
                update_tuids_in_thread(
                    new_files, frontier_update_list, revision, threaded
                )
            )
            self._remove_thread()

        self.statsdaemon.update_totals(len(files), len(result))

        # Log memory growth periodically
        with self.count_locker:
            if self.count_locker.value >= MEMORY_LOG_INTERVAL:
                Log.note("Forcing Garbage collection to help with memory.")
                gc.collect()
                self.count_locker.value = 0

        return result, completed

    def _apply_diff(self, annotation, diff, cset, file):
        """
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
        """
        # Ignore merges, they have duplicate entries.
        if diff["merge"]:
            return annotation, file
        if file.lstrip("/") == "dev/null":
            return [], file

        list_to_insert = []
        new_ann = [x for x in annotation]
        new_ann.sort(key=lambda x: x.line)

        def add_one(tl_tuple, lines):
            start = tl_tuple.line
            return (
                lines[: start - 1]
                + [tl_tuple]
                + [
                    TuidMap(tmap.tuid, int(tmap.line) + 1)
                    for tmap in lines[start - 1 :]
                ]
            )

        def remove_one(start, lines):
            return lines[: start - 1] + [
                TuidMap(tmap.tuid, int(tmap.line) - 1) for tmap in lines[start:]
            ]

        for f_proc in diff["diffs"]:
            new_fname = f_proc["new"].name.lstrip("/")
            old_fname = f_proc["old"].name.lstrip("/")
            if new_fname != file and old_fname != file:
                continue
            if old_fname != new_fname:
                if new_fname == "dev/null":
                    return [], file
                # Change the file name so that new tuids
                # are correctly created.
                file = new_fname

            f_diff = f_proc["changes"]
            for change in f_diff:
                if change.action == "+":
                    tuid_tmp = self._get_one_tuid(
                        cset, file, change.line + 1
                    )
                    if tuid_tmp == None:
                        new_tuid = self.tuid()
                        list_to_insert.append((new_tuid, cset, file, change.line + 1))
                    else:
                        new_tuid = tuid_tmp
                    new_ann = add_one(TuidMap(new_tuid, change.line + 1), new_ann)
                elif change.action == "-":
                    new_ann = remove_one(change.line + 1, new_ann)
            break  # Found the file, exit searching

        if len(list_to_insert) > 0:
            ids = []
            records = []
            for inserts_list in list_to_insert:
                tuid, file, revision, line = inserts_list
                record = self._make_record_temporal(tuid, revision, file, line)
                records.append(record)
                ids.append(record["value"]["_id"])
            self.temporal.extend(records)
            query = self._query_result_size({"_id": ids})
            self.temporal.refresh()
            while self.temporal.search(query).hits.total != len(ids):
                Till(seconds=0.001).wait()
                self.temporal.refresh()

        return new_ann, file

    def _get_tuids_from_files_try_branch(self, files, revision):
        """
        Gets files from a try revision. It abuses the idea that try pushes
        will come from various, but stable points (if people make many
        pushes on that revision). Furthermore, updates are generally done
        to a revision that should eventually have tuids already in the DB
        (i.e. overtime as people update to revisions that have a tuid annotation).

        :param files: Files to query.
        :param revision: Revision to get them at.
        :return: List of (file, tuids) tuples.
        """

        repo = "try"
        result = []
        log_existing_files = []
        files_to_update = []

        # Check if the files were already annotated.
        for file in files:
            already_ann = self._get_annotation(revision, file)
            if already_ann and already_ann[0] == "":
                result.append((file, []))
                log_existing_files.append("removed|" + file)
                continue
            elif already_ann:
                result.append((file, self.destringify_tuids(already_ann)))
                log_existing_files.append("exists|" + file)
                continue
            else:
                files_to_update.append(file)

        if len(log_existing_files) > 0:
            Log.note(
                "Try revision run - existing entries: {{count}}/{{total}} | {{percent}}",
                count=str(len(log_existing_files)),
                total=str(len(files)),
                percent=str(100 * (len(log_existing_files) / len(files))),
            )

        if len(files_to_update) <= 0:
            Log.note(
                "Found all files for try revision request: {{cset}}", cset=revision
            )
            return result

        # There are files to process, so let's find all the diffs.
        found_mc_patch = False
        diffs_to_get = []  # Will contain diffs in reverse order of application
        curr_rev = revision
        mc_revision = ""
        jsonpushes_url = (
            str(HG_URL)
            + "/"
            + repo
            + "/"
            + "json-pushes?full=1&changeset="
            + str(revision)
        )
        try:
            pushes_obj = http.get_json(jsonpushes_url, retry=RETRY)
            if not pushes_obj or len(pushes_obj.keys()) == 0:
                raise Exception("Nothing found in json-pushes request.")
            elif len(pushes_obj.keys()) > 1:
                raise Exception(
                    "Too many push numbers found in json-pushes request, cannot handle it."
                )
            push_num = list(pushes_obj.keys())[0]

            if (
                "changesets" not in pushes_obj[push_num]
                or len(pushes_obj[push_num]["changesets"]) == 0
            ):
                raise Exception("Cannot find any changesets in this push.")

            # Get the diffs that are needed to be applied
            # along with the mozilla-central revision they are applied to.
            all_csets = pushes_obj[push_num]["changesets"]
            for count, cset_obj in enumerate(all_csets):
                node = cset_obj["node"]
                if "parents" not in cset_obj:
                    raise Exception(
                        "Cannot find parents in object for changeset: " + str(node)
                    )
                if count == 0:
                    mc_revision = cset_obj["parents"][0]
                if len(cset_obj["parents"]) > 1:
                    raise Exception(
                        "Cannot yet handle multiple parents for changeset: " + str(node)
                    )
                diffs_to_get.append(node)
        except Exception as e:
            Log.warning(
                "Unexpected error getting changset-log for {{url}}: {{error}}",
                url=jsonpushes_url,
                error=e,
            )
            return [(file, []) for file in files]

        added_files = {}
        removed_files = {}
        files_to_process = {}

        Log.note("Gathering diffs for: {{csets}}", csets=str(diffs_to_get))
        all_diffs = self.get_diffs(diffs_to_get, repo=repo)

        # Build a dict for faster access to the diffs
        parsed_diffs = {entry["cset"]: entry["diff"] for entry in all_diffs}
        for csets_diff in all_diffs:
            cset_len12 = csets_diff["cset"]
            parsed_diff = csets_diff["diff"]["diffs"]

            for f_added in parsed_diff:
                # Get new entries for removed files.
                new_name = f_added["new"].name.lstrip("/")
                old_name = f_added["old"].name.lstrip("/")

                # If we don't need this file, skip it
                if new_name not in files_to_update:
                    # If the file was removed, set a
                    # flag and return no tuids later.
                    if new_name == "dev/null":
                        removed_files[old_name] = True
                    continue

                if old_name == "dev/null":
                    added_files[new_name] = True
                    continue

                if new_name in files_to_process:
                    files_to_process[new_name].append(cset_len12)
                else:
                    files_to_process[new_name] = [cset_len12]

        # We've found a good patch (a public one), get it
        # for all files and apply the patch's onto it.
        curr_annotations = self.get_tuids(files, mc_revision)
        curr_annots_dict = {file: mc_annot for file, mc_annot in curr_annotations}

        anns_to_get = []
        ann_inserts = []
        tmp_results = {}

        with self.temporal_locker:
            for file in files_to_update:
                if file not in curr_annots_dict:
                    Log.note(
                        "WARNING: Missing annotation entry in mozilla-central branch revision {{cset}} "
                        "for {{file}}",
                        file=file,
                        cset=mc_revision,
                    )
                    # Try getting it from the try revision
                    anns_to_get.append(file)
                    continue

                if file in added_files:
                    Log.note("Try revision run - added: {{file}}", file=file)
                    anns_to_get.append(file)
                elif file in removed_files:
                    Log.note("Try revision run - removed: {{file}}", file=file)
                    ann_inserts.append((revision, file, ""))
                    tmp_results[file] = []
                elif file in files_to_process:
                    Log.note("Try revision run - modified: {{file}}", file=file)
                    csets_to_proc = files_to_process[file]
                    old_ann = curr_annots_dict[file]

                    # Apply all the diffs
                    tmp_res = old_ann
                    new_fname = file
                    for i in csets_to_proc:
                        tmp_res, new_fname = self._apply_diff(
                            tmp_res, parsed_diffs[i], i, new_fname
                        )

                    ann_inserts.append((revision, file, self.stringify_tuids(tmp_res)))
                    tmp_results[file] = tmp_res
                else:
                    # Nothing changed with the file, use it's current annotation
                    Log.note("Try revision run - not modified: {{file}}", file=file)
                    ann_inserts.append(
                        (revision, file, self.stringify_tuids(curr_annots_dict[file]))
                    )
                    tmp_results[file] = curr_annots_dict[file]

            # Insert and check annotations, get all that were
            # added by another thread.
            anns_added_by_other_thread = {}
            if len(ann_inserts) > 0:
                for _, tmp_inserts in jx.groupby(ann_inserts, size=SQL_ANN_BATCH_SIZE):
                    # Check if any were added in the mean time by another thread
                    recomputed_inserts = []
                    for rev, filename, tuids in tmp_inserts:
                        tmp_ann = self._get_annotation(
                            rev, filename
                        )
                        if not tmp_ann and tmp_ann != "":
                            recomputed_inserts.append((rev, filename, tuids))
                        else:
                            anns_added_by_other_thread[
                                filename
                            ] = self.destringify_tuids(tmp_ann)

                    try:
                        self.insert_annotations(recomputed_inserts)
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
        going_forward=False,
        initial_growth={},
    ):
        """
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
        """

        # Get the changelogs and revisions until we find the
        # last one we've seen, and get the modified files in
        # each one.

        # Holds the files modified up to the last frontiers.
        files_to_process = {}

        # Holds all frontiers to find
        remaining_frontiers = {
            cset for cset in list(set([frontier for _, frontier in frontier_list]))
        }

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

        # Get the ordered revisions to apply
        Log.note(
            "Getting changesets to apply on frontiers: {{frontier}}",
            frontier=str(list(remaining_frontiers)),
        )
        for cset in diffs_to_frontier:
            diffs_to_frontier[cset] = self.clogger.get_revnnums_from_range(
                revision, cset
            )

        Log.note("Diffs to apply: {{csets}}", csets=str(diffs_to_frontier))

        added_files = {}
        removed_files = {}
        parsed_diffs = {}

        # This list is used to determine what files
        file_to_frontier = {file: frontier for file, frontier in frontier_list}

        # If there is at least one frontier that was found
        # Only get diffs that are needed (if any frontiers were not found)
        diffs_cache = []
        for cset in diffs_to_frontier:
            diffs_cache.extend([rev for revnum, rev in diffs_to_frontier[cset]])

        Log.note("Gathering diffs for: {{csets}}", csets=str(diffs_cache))
        all_diffs = self.get_diffs(diffs_cache)

        # Build a dict for faster access to the diffs,
        # to be used later when applying them.
        parsed_diffs = {
            diff_entry["cset"]: diff_entry["diff"] for diff_entry in all_diffs
        }
        for csets_diff in all_diffs:
            cset_len12 = csets_diff["cset"]
            parsed_diff = csets_diff["diff"]["diffs"]

            for f_added in parsed_diff:
                new_name = f_added["new"].name.lstrip("/")
                old_name = f_added["old"].name.lstrip("/")

                if new_name in file_to_frontier:
                    files_to_process[new_name] = True
                elif old_name in file_to_frontier:
                    files_to_process[old_name] = True

        # Process each file that needs it based on the
        # files_to_process list.
        result = []
        ann_inserts = []
        latestFileMod_inserts = {}
        anns_to_get = []
        total = len(file_to_frontier)
        tmp_results = {}

        with self.conn.transaction() as transaction:
            for count, (file, old_frontier) in enumerate(frontier_list):
                # If the file was modified, get it's newest
                # annotation and update the file.
                tmp_res = None
                if file in files_to_process:
                    # Process this file using the diffs found
                    tmp_ann = self._get_annotation(old_frontier, file)
                    if (
                        tmp_ann is None
                        or tmp_ann == ""
                        or self.destringify_tuids(tmp_ann) is None
                    ):
                        Log.warning(
                            "{{file}} has frontier but can't find old annotation for it in {{rev}}, "
                            "restarting it's frontier.",
                            rev=old_frontier,
                            file=file,
                        )
                        anns_to_get.append(file)
                    else:
                        # File was modified, apply it's diffs
                        csets_to_proc = diffs_to_frontier[file_to_frontier[file]]
                        tmp_res = self.destringify_tuids(tmp_ann)
                        file_to_modify = AnnotateFile(
                            file,
                            [TuidLine(tuidmap, filename=file) for tuidmap in tmp_res],
                            tuid_service=self,
                        )

                        backwards = False
                        if len(csets_to_proc) >= 1 and revision == csets_to_proc[0][1]:
                            backwards = True

                            # Reverse the list, we apply the frontier
                            # diff first when going backwards.
                            csets_to_proc = csets_to_proc[::-1]
                            Log.note("Applying diffs backwards...")

                        # Going either forward or backwards requires
                        # us to remove the first revision, which is
                        # either the requested revision if we are going
                        # backwards or the current frontier, if we are
                        # going forward.
                        csets_to_proc = csets_to_proc[1:]

                        # Apply the diffs
                        for diff_count, (_, rev) in enumerate(csets_to_proc):

                            # Use next revision when going backwards
                            # to add new lines correctly.
                            next_rev = revision
                            if diff_count + 1 < len(csets_to_proc):
                                _, next_rev = csets_to_proc[diff_count + 1]

                            rev_to_proc = next_rev
                            if backwards:
                                file_to_modify = apply_diff_backwards(
                                    file_to_modify, parsed_diffs[rev]
                                )
                            else:
                                file_to_modify = apply_diff(
                                    file_to_modify, parsed_diffs[rev]
                                )
                                rev_to_proc = rev

                            try:
                                with self.temporal_locker:
                                    file_to_modify.create_and_insert_tuids(rev_to_proc)
                            except Exception as e:
                                file_to_modify.failed_file = True
                                Log.warning(
                                    "Failed to create and insert tuids - likely due to merge conflict.",
                                    cause=e,
                                )
                                break
                            file_to_modify.reset_new_lines()

                        if not file_to_modify.failed_file:
                            tmp_res = file_to_modify.lines_to_annotation()

                        ann_inserts.append(
                            (revision, file, self.stringify_tuids(tmp_res))
                        )
                        Log.note(
                            "Frontier update - modified: {{count}}/{{total}} - {{percent|percent(decimal=0)}} "
                            "| {{rev}}|{{file}} ",
                            count=count + 1,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total,
                        )
                else:
                    old_ann = self._get_annotation(old_frontier, file)
                    if old_ann is None or (old_ann == "" and file in added_files):
                        # File is new (likely from an error), or re-added - we need to create
                        # a new initial entry for this file.
                        anns_to_get.append(file)
                        Log.note(
                            "Frontier update - readded: {{count}}/{{total}} - {{percent|percent(decimal=0)}} "
                            "| {{rev}}|{{file}} ",
                            count=count + 1,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total,
                        )
                    else:
                        # File was not modified since last
                        # known revision
                        tmp_res = (
                            self.destringify_tuids(old_ann) if old_ann != "" else []
                        )
                        ann_inserts.append((revision, file, old_ann))
                        Log.note(
                            "Frontier update - not modified: {{count}}/{{total}} - {{percent|percent(decimal=0)}} "
                            "| {{rev}}|{{file}} ",
                            count=count + 1,
                            total=total,
                            file=file,
                            rev=revision,
                            percent=count / total,
                        )

                if tmp_res:
                    tmp_results[file] = tmp_res
                else:
                    Log.note(
                        "Error occured for file {{file}} in revision {{revision}}",
                        file=file,
                        revision=revision,
                    )
                    tmp_results[file] = []

                # If we have found all frontiers, update to the
                # latest revision. Otherwise, the requested
                # revision is too far away (can't be sure
                # if it's past). Unless we are told that we are
                # going forward.
                latestFileMod_inserts[file] = (file, revision)

            Log.note("Updating DB tables `latestFileMod` and `annotations`...")

            # No need to double-check if latesteFileMods has been updated before,
            # we perform an insert or replace any way.
            if len(latestFileMod_inserts) > 0:
                for _, inserts_list in jx.groupby(
                    latestFileMod_inserts.values(), size=SQL_BATCH_SIZE
                ):
                    transaction.execute(
                        "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES "
                        + sql_list(quote_list(i) for i in inserts_list)
                    )

            anns_added_by_other_thread = {}
            if len(ann_inserts) > 0:
                for _, tmp_inserts in jx.groupby(ann_inserts, size=SQL_ANN_BATCH_SIZE):
                    # Check if any were added in the mean time by another thread
                    recomputed_inserts = []
                    for rev, filename, string_tuids in tmp_inserts:
                        tmp_ann = self._get_annotation(rev, filename)
                        if not tmp_ann and tmp_ann != "":
                            recomputed_inserts.append((rev, filename, string_tuids))
                        else:
                            anns_added_by_other_thread[
                                filename
                            ] = self.destringify_tuids(tmp_ann)

                    if len(recomputed_inserts) <= 0:
                        continue

                    try:
                        self.insert_annotations(recomputed_inserts)
                    except Exception as e:
                        Log.error(
                            "Error inserting into annotations table: {{inserting}}",
                            inserting=recomputed_inserts,
                            cause=e,
                        )

        if len(anns_to_get) > 0:
            result.extend(self.get_tuids(anns_to_get, revision))

        for f in tmp_results:
            tuids = tmp_results[f]
            if f in anns_added_by_other_thread:
                tuids = anns_added_by_other_thread[f]
            result.append((copy.deepcopy(f), copy.deepcopy(tuids)))
        return result

    def get_tuids(self, files, revision, chunk=50, repo=None):
        """
        Wrapper for `_get_tuids` to limit the number of annotation calls to hg
        and separate the calls from DB transactions. Also used to simplify `_get_tuids`.

        :param files:
        :param revision:
        :param chunk:
        :param repo:
        :return:
        """
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
                new_files[count] = file.lstrip("/")

            annotations_to_get = []
            for file in new_files:
                already_ann = self._get_annotation(revision, file)
                if already_ann:
                    results.append((file, self.destringify_tuids(already_ann)))
                elif already_ann == "":
                    results.append((file, []))
                else:
                    annotations_to_get.append(file)

            if not annotations_to_get:
                # No new annotations to get, so get next set
                continue

            # Get all the annotations in parallel and
            # store in annotated_files and
            # prevent too many threads from starting up here.
            self.statsdaemon.update_threads_waiting(len(annotations_to_get))
            num_threads = chunk
            timeout = Till(seconds=ANN_WAIT_TIME.seconds)
            while num_threads >= chunk and not timeout:
                with self.ann_thread_locker:
                    num_threads = self.ann_threads_running
                    if num_threads <= chunk:
                        break
                Till(seconds=MAX_THREAD_WAIT_TIME.seconds).wait()
            self.statsdaemon.update_threads_waiting(-len(annotations_to_get))

            if timeout:
                Log.warning(
                    "Timeout {{timeout}} exceeded waiting to start annotation threads.",
                    timeout=MAX_ANN_REQUESTS_WAIT_TIME,
                )
                annotated_files = [[] for _ in annotations_to_get]
            else:
                # Recompute annotations to get here, in case we've waited
                # a while.
                old_annotations_len = len(annotations_to_get)
                new_annotations_to_get = []
                for file in annotations_to_get:
                    already_ann = self._get_annotation(revision, file)
                    if already_ann:
                        results.append((file, self.destringify_tuids(already_ann)))
                    elif already_ann == "":
                        results.append((file, []))
                    else:
                        new_annotations_to_get.append(file)
                annotations_to_get = new_annotations_to_get

                if not annotations_to_get:
                    continue

                annotated_files = [None] * len(annotations_to_get)
                threads = [
                    Thread.run(
                        str(thread_count),
                        self._get_hg_annotate,
                        revision,
                        annotations_to_get[thread_count],
                        annotated_files,
                        thread_count,
                        repo,
                    )
                    for thread_count, _ in enumerate(annotations_to_get)
                ]
                for t in threads:
                    t.join()

                # Help for memory, because `chunk` (or a lot of)
                # threads are started at once.
                del threads

            results.extend(
                self._get_tuids(
                    annotations_to_get,
                    revision,
                    annotated_files,
                    repo=repo,
                )
            )

            del annotations_to_get[:]
            del annotated_files[:]

        # Help for memory
        gc.collect()
        return results

    def insert_tuids_with_duplicates(
        self, file, revision, new_lines, line_origins
    ):
        """
        Inserts new lines while creating tuids and handles duplicate entries.
        :param new_lines: A list of new line numbers.
        :param line_origins: A list of all lines as tuples (file, revision, line)
        :return:
        """

        """
            HG Annotate Bug, Issue #58:
            Here is where we assign the new tuids for the first
            time we see duplicate entries - they are left
            in `new_line_origins` after duplicates are found.
            We only remove it from the lines to insert. In future
            requests, the tuid will be duplicated in _get_tuids.
        """

        new_line_origins = {
            line_num: (self.tuid(),) + line_origins[line_num - 1]
            for line_num in new_lines
        }

        duplicate_lines = {
            line_num + 1: line
            for line_num, line in enumerate(line_origins)
            if line in line_origins[:line_num]
        }
        if len(duplicate_lines) > 0:
            Log.note(
                "Duplicates found in {{file}} at {{cset}}: {{dupes}}",
                file=file,
                cset=revision,
                dupes=str(duplicate_lines),
            )
            lines_to_insert = [
                line
                for line_num, line in new_line_origins.items()
                if line_num not in duplicate_lines
            ]
        else:
            lines_to_insert = new_line_origins.values()

        records = []
        ids = []
        for part_of_insert in lines_to_insert:
            for tuid, f, rev, line_num in [part_of_insert]:
                record = self._make_record_temporal(tuid, rev, f, line_num)
                records.append(record)
                ids.append(record["value"]["_id"])

        self.temporal.extend(records)
        query = self._query_result_size({"_id": ids})
        self.temporal.refresh()
        while self.temporal.search(query).hits.total != len(ids):
            Till(seconds=0.001).wait()
            self.temporal.refresh()
        return new_line_origins

    def get_new_lines(self, line_origins):
        """
        Checks if any lines were already added.
        :param line_origins:
        :return:
        """
        file_names = list(set([f for f, _, _ in line_origins]))
        revs_to_find = list(set([rev for _, rev, _ in line_origins]))
        lines_to_find = list(set([line for _, _, line in line_origins]))

        query = {
            "size": 10000,
            "_source": {"includes": ["tuid", "file", "revision", "line"]},
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"file": file_names}},
                        {"terms": {"revision": revs_to_find}},
                        {"terms": {"line": lines_to_find}},
                    ]
                }
            },
        }

        result = self.temporal.search(query)

        query = {
            "_source": {"includes": ["annotation"]},
            "query": {
                "bool": {
                    "must": [{"terms": {"file": file_names}}, {"terms": {"revision": revs_to_find}}]
                }
            },
            "size": 10000,
        }
        temp = self.annotations.search(query).hits.hits


        existing_tuids_tmp = {r._id: r._source.tuid for r in result.hits.hits}

        # Explicitly remove reference cycle
        del file_names
        del revs_to_find
        del lines_to_find

        # Recompute existing tuids based on line_origins
        # entry ordering because we can't order them any other way
        # since the `line` entry in the `temporal` table is relative
        # to it's creation date, not the currently requested
        # annotation.
        existing_tuids = {}
        for line_num, ann_entry in enumerate(line_origins):
            file, rev, line = ann_entry
            id = rev + file + str(line)
            if id in existing_tuids_tmp:
                existing_tuids[line_num + 1] = copy.deepcopy(existing_tuids_tmp[id])

        new_lines = set(
            [line_num + 1 for line_num, _ in enumerate(line_origins)]
        ) - set(existing_tuids.keys())
        return new_lines, existing_tuids

    def _get_tuids(
        self, files, revision, annotated_files, repo=None
    ):
        """
        Returns (TUID, line) tuples for a given file at a given revision.

        Uses json-annotate to find all lines in this revision, then it updates
        the database with any missing revisions for the file changes listed
        in annotate. Then, we use the information from annotate coupled with the
        diff information that was inserted into the DB to return TUIDs. This way
        we don't have to deal with child, parents, dates, etc..

        :param files: list of files to process
        :param revision: revision at which to get the file
        :param annotated_files: annotations for each file
        :param repo: The branch to get tuids from
        :return: List of TuidMap objects
        """
        with self.temporal_locker:
            results = []
            for fcount, annotated_object in enumerate(annotated_files):
                file = files[fcount]
                # TODO: Replace old empty annotation if a new one is found
                # TODO: at the same revision and if it is not empty as well.
                # Make sure we are not adding the same thing another thread
                # added.
                tmp_ann = self._get_annotation(revision, file)
                if tmp_ann != None:
                    results.append((file, self.destringify_tuids(tmp_ann)))
                    continue

                # If it's not defined at this revision, we need to add it in
                errored = False
                if isinstance(annotated_object, (text_type, str)):
                    errored = True
                    Log.warning(
                        "{{file}} does not exist in the revision={{cset}} branch={{branch_name}}",
                        branch_name=repo,
                        cset=revision,
                        file=file,
                    )
                elif annotated_object is None:
                    Log.warning(
                        "Unexpected error getting annotation for: {{file}} in the revision={{cset}} branch={{branch_name}}",
                        branch_name=repo,
                        cset=revision,
                        file=file,
                    )
                    errored = True
                elif "annotate" not in annotated_object:
                    Log.warning(
                        "Missing annotate, type got: {{ann_type}}, expecting:dict returned when getting "
                        "annotation for: {{file}} in the revision {{cset}}",
                        cset=revision,
                        file=file,
                        ann_type=type(annotated_object),
                    )
                    errored = True

                if errored:
                    Log.note("Inserting dummy entry...")
                    self.insert_tuid_dummy(revision, file)
                    self.insert_annotate_dummy(revision, file)
                    results.append((file, []))
                    continue

                # Gather all missing csets and the
                # corresponding lines.
                line_origins = []
                for node in annotated_object["annotate"]:
                    cset_len12 = node["node"][:12]

                    # If the line added by `cset_len12` is not known
                    # add it. Use the 'abspath' field to determine the
                    # name of the file it was created in (in case it was
                    # changed). Copy to make sure we don't create a reference
                    # here.
                    line_origins.append(
                        copy.deepcopy(
                            (node["abspath"], cset_len12, int(node["targetline"]))
                        )
                    )

                # Update DB with any revisions found in annotated
                # object that are not in the DB.
                new_line_origins = {}
                new_lines, existing_tuids = self.get_new_lines(line_origins)
                if len(new_lines) > 0:
                    try:
                        new_line_origins = self.insert_tuids_with_duplicates(
                            file, revision, new_lines, line_origins
                        )

                        # Format so we don't have to use [0] to get at the tuids
                        for linenum in new_line_origins:
                            new_line_origins[linenum] = new_line_origins[linenum][0]
                    except Exception as e:
                        # Something broke for this file, ignore it and go to the
                        # next one.
                        Log.note("Failed to insert new tuids {{cause}}", cause=e)
                        continue

                tuids = []
                for line_ind, line_origin in enumerate(line_origins):
                    line_num = line_ind + 1
                    if line_num in existing_tuids:
                        tuids.append(TuidMap(existing_tuids[line_num], line_num))
                    else:
                        tuids.append(TuidMap(new_line_origins[line_num], line_num))

                str_tuids = self.stringify_tuids(tuids)
                entry = [(revision, file, str_tuids)]

                self.insert_annotations(entry)
                results.append((copy.deepcopy(file), copy.deepcopy(tuids)))

        return results

    def _daemon(self, please_stop, only_coverage_revisions=False):
        """
        Runs continuously to prefill the temporal and
        annotations table with the coverage revisions*.

        * A coverage revision is a revision which has had
        code coverage run on it.

        :param please_stop: Used to stop the daemon
        :return: None
        """
        while not please_stop:
            # Get all known files and their latest revisions on the frontier
            files_n_revs = self.conn.get("SELECT file, revision FROM latestFileMod")

            # Split these files into groups of revisions to make it
            # easier to update them. If we group them together, we
            # may end up updating groups that are new back to older
            # revisions.
            revs = {
                rev: [] for rev in set([file_n_rev[1] for file_n_rev in files_n_revs])
            }
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
                final_rev = ""
                found_last_frontier = False
                Log.note("Searching for frontier: {{frontier}} ", frontier=frontier)
                Log.note(
                    "HG URL: {{url}}",
                    url=str(HG_URL) + "/" + self.config.hg.branch + "/rev/" + frontier,
                )
                while not found_last_frontier:
                    # Get a changelog
                    clog_url = (
                        str(HG_URL)
                        + "/"
                        + self.config.hg.branch
                        + "/json-log/"
                        + final_rev
                    )
                    try:
                        clog_obj = self.get_clog(clog_url)
                    except Exception as e:
                        Log.error(
                            "Unexpected error getting changset-log for {{url}}",
                            url=clog_url,
                            error=e,
                        )

                    cset = ""
                    still_looking = True
                    # For each changeset/node
                    for clog_cset in clog_obj["changesets"]:
                        cset = clog_cset["node"][:12]
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
                    active_data_url = "http://activedata.allizom.org/query"
                    query_json = {
                        "limit": 1000,
                        "from": "task",
                        "where": {
                            "and": [
                                {"in": {"build.type": ["ccov", "jsdcov"]}},
                                {"gte": {"run.timestamp": {"date": "today-day"}}},
                                {"eq": {"repo.branch.name": self.config.hg.branch}},
                            ]
                        },
                        "select": [
                            {"aggregate": "min", "value": "run.timestamp"},
                            {"aggregate": "count"},
                        ],
                        "groupby": ["repo.changeset.id12"],
                    }
                    coverage_revisions_resp = http.post_json(
                        active_data_url, retry=RETRY, data=query_json
                    )
                    coverage_revisions = [
                        rev_arr[0] for rev_arr in coverage_revisions_resp.data
                    ]

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
                        Log.note(
                            "Moving frontier {{frontier}} forward to {{cset}}.",
                            frontier=prev_cset,
                            cset=cset,
                        )

                    # Update files
                    self.get_tuids_from_files(files, cset)

                    ran_changesets = True
                    prev_cset = cset

            if not ran_changesets:
                (please_stop | Till(seconds=DAEMON_WAIT_AT_NEWEST.seconds)).wait()


TEMPORAL_SCHEMA = {
    "settings": {"index.number_of_replicas": 1, "index.number_of_shards": 1},
    "mappings": {
        "temporaltype": {
            "_all": {"enabled": False},
            "properties": {
                "tuid": {"type": "integer", "store": True},
                "revision": {"type": "keyword", "store": True},
                "file": {"type": "keyword", "store": True},
                "line": {"type": "integer", "store": True},
            },
        }
    },
}


ANNOTATIONS_SCHEMA = {
    "settings": {"index.number_of_replicas": 1, "index.number_of_shards": 1},
    "mappings": {
        "annotationstype": {
            "_all": {"enabled": False},
            "properties": {
                "revision": {"type": "keyword", "store": True},
                "file": {"type": "keyword", "store": True},
                "annotation": {"type": "keyword", "ignore_above": 20, "store": True},
            },
        }
    },
}
