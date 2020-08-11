# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import time

# Use import as follows to prevent
# circular dependency conflict for
# TUIDService, which makes use of the
# Clogger
import tuid.service
from jx_elasticsearch import elasticsearch
from jx_python import jx
from mo_dots import Null, coalesce, set_default, wrap
from mo_hg.hg_mozilla_org import HgMozillaOrg
from mo_logs import Log
from mo_logs.exceptions import suppress_exception
from mo_threads import Till, Thread, Lock, Queue, Signal
from mo_times.durations import DAY
from mo_http import http
from tuid import sql
from tuid.util import HG_URL, insert, delete

RETRY = {"times": 3, "sleep": 5}
SQL_CSET_BATCH_SIZE = 500
CSET_TIP_WAIT_TIME = 5 * 60  # seconds
CSET_BACKFILL_WAIT_TIME = 1 * 60  # seconds
CSET_MAINTENANCE_WAIT_TIME = 30 * 60  # seconds
CSET_DELETION_WAIT_TIME = 1 * 60  # seconds
TUID_EXISTENCE_WAIT_TIME = 1 * 60  # seconds
TIME_TO_KEEP_ANNOTATIONS = 5 * DAY
MAX_TIPFILL_CLOGS = 400  # changeset logs
MAX_BACKFILL_CLOGS = 1000  # changeset logs
CHANGESETS_PER_CLOG = 20  # changesets
BACKFILL_REVNUM_TIMEOUT = int(MAX_BACKFILL_CLOGS * 2.5)  # Assume 2.5 seconds per clog
MINIMUM_PERMANENT_CSETS = 200  # changesets
MAXIMUM_NONPERMANENT_CSETS = 1500  # changesets
SIGNAL_MAINTENANCE_CSETS = int(MAXIMUM_NONPERMANENT_CSETS + (0.2 * MAXIMUM_NONPERMANENT_CSETS))
UPDATE_VERY_OLD_FRONTIERS = False
CACHE_WAIT_TIME = 15  # seconds
CACHING_BATCH_SIZE = 50

SINGLE_CLOGGER = None


class Clogger:

    # Singleton of the look-ahead scanner Clogger
    SINGLE_CLOGGER = None

    def __new__(cls, *args, **kwargs):
        if cls.SINGLE_CLOGGER is None:
            cls.SINGLE_CLOGGER = object.__new__(cls)
        return cls.SINGLE_CLOGGER

    def __init__(
        self, conn=None, tuid_service=None, start_workers=True, new_table=False, kwargs=None
    ):
        try:
            self.config = kwargs
            self.conn = conn if conn else sql.Sql(self.config.database.name)
            self.hg_cache = (
                HgMozillaOrg(kwargs=self.config.hg_cache, use_cache=True)
                if self.config.hg_cache
                else Null
            )
            self.esconfig = None
            if self.config.tuid:
                self.esconfig = self.config.tuid.esclogger.csetLog
            else:
                self.esconfig = self.config.esclogger.csetLog
            self.es = elasticsearch.Cluster(kwargs=self.esconfig)

            self.tuid_service = (
                tuid_service
                if tuid_service
                else tuid.service.TUIDService(
                    kwargs=self.config.tuid, conn=self.conn, clogger=self
                )
            )
            self.rev_locker = Lock()
            self.working_locker = Lock()
            self.csetLog_locker = Lock()

            if new_table:
                try:
                    index = self.es.get_canonical_index(self.esconfig.index)
                    response = self.es.delete_index(index)
                except Exception as e:
                    Log.warning(
                        "could not delete csetlog index because (mostly index has not yet created): {{cause}}",
                        cause=str(e),
                    )

            self.init_db()
            self.next_revnum = self.get_revnum_stats("max") + 1

            self.csets_todo_backwards = Queue(name="Clogger.csets_todo_backwards")
            self.caching_signal = Signal(name="Clogger.caching_signal")

            if "tuid" in self.config:
                self.config = self.config.tuid

            self.disable_backfilling = False
            self.disable_tipfilling = False
            self.disable_caching = False

            self.backfill_thread = None
            self.tipfill_thread = None
            self.caching_thread = None

            # Make sure we are filled before allowing queries
            query = {"aggs": {"output": {"value_count": {"field": "revnum"}}}, "size": 0}
            numrevs = int(self.csetlog.search(query).aggregations.output.value)
            if numrevs < MINIMUM_PERMANENT_CSETS:
                Log.note(
                    "Filling in csets to hold {{minim}} csets.", minim=MINIMUM_PERMANENT_CSETS
                )
                oldest_rev = "tip"

                query = {
                    "_source": {"includes": ["revision"]},
                    "sort": [{"revnum": {"order": "asc"}}],
                    "size": 1,
                }
                tmp = self.csetlog.search(query).hits.hits[0]._source.revision
                if tmp:
                    oldest_rev = tmp
                self._fill_in_range(MINIMUM_PERMANENT_CSETS - numrevs, oldest_rev, timestamp=False)

            Log.note(
                "Table is filled with atleast {{minim}} entries.", minim=MINIMUM_PERMANENT_CSETS
            )

            if start_workers:
                self.start_workers()

        except Exception as e:
            Log.warning("Cannot setup clogger: {{cause}}", cause=str(e))

    def get_revnum_stats(self, query_required):
        query = None
        if query_required == "min":
            query = {"size": 0, "aggs": {"value": {"min": {"field": "revnum"}}}}
        elif query_required == "max":
            query = {"size": 0, "aggs": {"value": {"max": {"field": "revnum"}}}}
        return coalesce(eval(str(self.csetlog.search(query).aggregations.value.value)), 0)

    def _query_result_size(self, terms):
        query = {"size": 0, "query": {"terms": terms}}
        return query

    def _make_record_csetlog(self, revnum, revision, timestamp):
        record = {"_id": revnum, "revnum": revnum, "revision": revision, "timestamp": timestamp}
        return {"value": record}

    def start_backfilling(self):
        if not self.backfill_thread:
            self.backfill_thread = Thread.run("clogger-backfill", self.fill_backward_with_list)

    def start_tipfillling(self):
        if not self.tipfill_thread:
            self.tipfill_thread = Thread.run("clogger-tip", self.fill_forward_continuous)

    def start_caching(self):
        if not self.caching_thread:
            self.caching_thread = Thread.run("caching-daemon", self.caching_daemon)

    def start_workers(self):
        self.start_tipfillling()
        self.start_backfilling()
        self.start_caching()
        Log.note("Started clogger workers.")

    def init_db(self):
        csetLog = self.esconfig
        set_default(csetLog, {"schema": CSETLOG_SCHEMA})
        self.csetlog = self.es.get_or_create_index(kwargs=csetLog)
        self.csetlog.refresh()

        total = self.csetlog.search({"size": 0})
        while not total.hits:
            total = self.csetlog.search({"size": 0})
        with suppress_exception:
            self.csetlog.add_alias()

    def disable_all(self):
        self.disable_tipfilling = True
        self.disable_backfilling = True
        self.disable_caching = True

    def revnum(self):
        """
        :return: max revnum that was added
        """
        return self.get_revnum_stats("max")

    def get_tip(self):
        query = {
            "_source": {"includes": ["revision"]},
            "sort": [{"revnum": {"order": "desc"}}],
            "size": 1,
        }
        result = self.csetlog.search(query)
        tmp = (result.hits.hits[0].sort[0], result.hits.hits[0]._source.revision)
        return tmp

    def get_tail(self):
        query = {
            "_source": {"includes": ["revision"]},
            "sort": [{"revnum": {"order": "asc"}}],
            "size": 1,
        }
        result = self.csetlog.search(query)
        tmp = (result.hits.hits[0].sort[0], result.hits.hits[0]._source.revision)
        return tmp

    def _get_clog(self, clog_url):
        try:
            Log.note("Searching through changelog {{url}}", url=clog_url)
            clog_obj = http.get_json(clog_url, retry=RETRY)
            return clog_obj
        except Exception as e:
            Log.error(
                "Unexpected error getting changset-log for {{url}}: {{error}}",
                url=clog_url,
                error=e,
            )

    def _get_one_revision(self, cset_entry):
        # Returns a single revision if it exists
        _, rev, _ = cset_entry
        query = {
            "_source": {"includes": ["revision"]},
            "query": {"bool": {"must": [{"term": {"revision": rev}}]}},
            "size": 1,
        }
        temp = self.csetlog.search(query).hits.hits[0]._source.revision
        return temp

    def _get_one_revnum(self, rev):
        # Returns a single revnum if it exists
        query = {
            "_source": {"includes": ["revnum"]},
            "query": {"bool": {"must": [{"term": {"revision": rev}}]}},
            "size": 1,
        }
        temp = self.csetlog.search(query).hits.hits[0]._source.revnum
        return temp

    def _get_revnum_exists(self, rev):
        # Returns a single revnum if it exists
        query = {
            "_source": {"includes": ["revnum"]},
            "query": {"bool": {"must": [{"term": {"revnum": rev}}]}},
            "size": 0,
        }
        temp = self.csetlog.search(query).hits.total
        return temp

    def _get_revnum_range(self, revnum1, revnum2):
        # Returns a range of revision numbers (that is inclusive)
        high_num = max(revnum1, revnum2)
        low_num = min(revnum1, revnum2)

        total = self.csetlog.search({"size": 0})
        query = {
            "size": total.hits.total,
            "_source": {"includes": ["revnum", "revision"]},
            "query": {
                "bool": {"must": [{"range": {"revnum": {"gte": low_num, "lte": high_num}}}]}
            },
        }
        result = self.csetlog.search(query).hits.hits
        temp = []
        for r in result:
            temp.append((r._source.revnum, r._source.revision))
        return temp

    def add_cset_entries(self, ordered_rev_list, timestamp=False, number_forward=True):
        """
        Adds a list of revisions to the table. Assumes ordered_rev_list is an ordered
        based on how changesets are found in the changelog. Going forwards or backwards is dealt
        with by flipping the list
        :param ordered_cset_list: Order given from changeset log searching.
        :param timestamp: If false, records are kept indefinitely
                          but if holes exist: (delete, None, delete, None)
                          those delete's with None's around them
                          will not be deleted.
        :param numbered: If True, this function will number the revision list
                         by going forward from max(revNum), else it'll go backwards
                         from revNum, then add X to all revnums and self.next_revnum
                         where X is the length of ordered_rev_list
        :return:
        """

        current_min = self.get_revnum_stats("min")
        current_max = self.get_revnum_stats("max")
        direction = -1
        start = current_min - 1
        if number_forward:
            direction = 1
            start = current_max + 1
            ordered_rev_list = ordered_rev_list[::-1]

        insert_list = [
            (start + direction * count, rev, int(time.time()) if timestamp else -1)
            for count, rev in enumerate(ordered_rev_list)
        ]

        # In case of overlapping requests
        fmt_insert_list = []
        for cset_entry in insert_list:
            tmp = self._get_one_revision(cset_entry)
            if not tmp:
                fmt_insert_list.append(cset_entry)

        # for _, tmp_insert_list in jx.chunk(fmt_insert_list, size=SQL_CSET_BATCH_SIZE):
        records = wrap(
            [
                self._make_record_csetlog(revnum, revision, timestamp)
                for revnum, revision, timestamp in fmt_insert_list
            ]
        )
        insert(self.csetlog, records)

    def _fill_in_range(self, parent_cset, child_cset, timestamp=False, number_forward=True):
        """
        Fills cset logs in a certain range. 'parent_cset' can be an int and in that case,
        we get that many changesets instead. If parent_cset is an int, then we consider
        that we are going backwards (number_forward is False) and we ignore the first
        changeset of the first log, and we ignore the setting for number_forward.
        Otherwise, we continue until we find the given 'parent_cset'.
        :param parent_cset:
        :param child_cset:
        :param timestamp:
        :param number_forward:
        :return:
        """
        csets_to_add = []
        found_parent = False
        find_parent = False
        if type(parent_cset) != int:
            find_parent = True
        elif parent_cset >= MAX_BACKFILL_CLOGS * CHANGESETS_PER_CLOG:
            Log.warning(
                "Requested number of new changesets {{num}} is too high. "
                "Max number that can be requested is {{maxnum}}.",
                num=parent_cset,
                maxnum=MAX_BACKFILL_CLOGS * CHANGESETS_PER_CLOG,
            )
            return None

        csets_found = 0
        clogs_seen = 0
        final_rev = child_cset
        while not found_parent and clogs_seen < MAX_BACKFILL_CLOGS:
            clog_url = str(HG_URL) + "/" + self.config.hg.branch + "/json-log/" + final_rev
            clog_obj = self._get_clog(clog_url)
            clog_csets_list = list(clog_obj["changesets"])
            for clog_cset in clog_csets_list[:-1]:
                if not number_forward and csets_found <= 0:
                    # Skip this entry it already exists
                    csets_found += 1
                    continue

                nodes_cset = clog_cset["node"][:12]
                if find_parent:
                    if nodes_cset == parent_cset:
                        found_parent = True
                        if not number_forward:
                            # When going forward this entry is
                            # the given parent
                            csets_to_add.append(nodes_cset)
                        break
                else:
                    if csets_found + 1 > parent_cset:
                        found_parent = True
                        if not number_forward:
                            # When going forward this entry is
                            # the given parent (which is supposed
                            # to already exist)
                            csets_to_add.append(nodes_cset)
                        break
                    csets_found += 1
                csets_to_add.append(nodes_cset)
            if found_parent == True:
                break

            clogs_seen += 1
            final_rev = clog_csets_list[-1]["node"][:12]

        if found_parent:
            self.add_cset_entries(csets_to_add, timestamp=timestamp, number_forward=number_forward)
        else:
            Log.warning(
                "Couldn't find the end of the request for {{request}}. "
                "Max number that can be requested through _fill_in_range is {{maxnum}}.",
                request={
                    "parent_cset": parent_cset,
                    "child_cset": child_cset,
                    "number_forward": number_forward,
                },
                maxnum=MAX_BACKFILL_CLOGS * CHANGESETS_PER_CLOG,
            )
            return None
        return csets_to_add

    def initialize_to_range(self, old_rev, new_rev, delete_old=True):
        """
        Used in service testing to get to very old
        changesets quickly.
        :param old_rev: The oldest revision to keep
        :param new_rev: The revision to start searching from
        :return:
        """
        old_settings = self.disable_tipfilling, self.disable_backfilling, self.disable_caching
        self.disable_tipfilling = True
        self.disable_backfilling = True
        self.disable_caching = True

        old_rev = old_rev[:12]
        new_rev = new_rev[:12]

        with self.working_locker:
            if delete_old:
                filter = {"match_all": {}}
                delete(self.csetlog, filter)

            max_revnum = self.get_revnum_stats("max") + 1
            self.csetlog.add(self._make_record_csetlog(max_revnum, new_rev, -1))
            self.csetlog.refresh()
            while not self._get_revnum_exists(max_revnum):
                Till(seconds=0.001).wait()

            self._fill_in_range(old_rev, new_rev, timestamp=True, number_forward=False)

        self.disable_tipfilling, self.disable_backfilling, self.disable_caching = old_settings

    def fill_backward_with_list(self, please_stop=None):
        """
        Expects requests of the tuple form: (parent_cset, timestamp)
        parent_cset can be an int X to go back by X changesets, or
        a string to search for going backwards in time. If timestamp
        is false, no timestamps will be added to the entries.
        :param please_stop:
        :return:
        """
        while not please_stop:
            try:
                request = self.csets_todo_backwards.pop(till=please_stop)
                if please_stop:
                    break

                # If backfilling is disabled, all requests
                # are ignored.
                if self.disable_backfilling:
                    Till(till=CSET_BACKFILL_WAIT_TIME).wait()
                    continue

                if request:
                    parent_cset, timestamp = request
                else:
                    continue

                with self.working_locker:
                    parent_revnum = self._get_one_revnum(parent_cset)
                    if parent_revnum != None:
                        continue

                    _, oldest_revision = self.get_tail()

                    self._fill_in_range(
                        parent_cset, oldest_revision, timestamp=timestamp, number_forward=False
                    )
                Log.note("Finished {{cset}}", cset=parent_cset)
            except Exception as e:
                Log.warning("Unknown error occurred during backfill: ", cause=e)

    def update_tip(self):
        """
        Returns False if the tip is already at the newest, or True
        if an update has taken place.
        :return:
        """
        clog_obj = self._get_clog(str(HG_URL) + "/" + self.config.hg.branch + "/json-log/tip")

        _, newest_known_rev = self.get_tip()

        # If we are still at the newest, wait for CSET_TIP_WAIT_TIME seconds
        # before checking again.
        first_clog_entry = clog_obj["changesets"][0]["node"][:12]
        if newest_known_rev == first_clog_entry:
            return False

        csets_to_gather = None
        if not newest_known_rev:
            Log.note(
                "No revisions found in table, adding {{minim}} entries...",
                minim=MINIMUM_PERMANENT_CSETS,
            )
            csets_to_gather = MINIMUM_PERMANENT_CSETS

        found_newest_known = False
        csets_to_add = []
        csets_found = 0
        clogs_seen = 0
        Log.note("Found new revisions. Updating csetLog tip to {{rev}}...", rev=first_clog_entry)
        while not found_newest_known and clogs_seen < MAX_TIPFILL_CLOGS:
            clog_csets_list = list(clog_obj["changesets"])
            for clog_cset in clog_csets_list[:-1]:
                nodes_cset = clog_cset["node"][:12]
                if not csets_to_gather:
                    if nodes_cset == newest_known_rev:
                        found_newest_known = True
                        break
                else:
                    if csets_found >= csets_to_gather:
                        found_newest_known = True
                        break
                csets_found += 1
                csets_to_add.append(nodes_cset)
            if not found_newest_known:
                # Get the next page
                clogs_seen += 1
                final_rev = clog_csets_list[-1]["node"][:12]
                clog_url = str(HG_URL) + "/" + self.config.hg.branch + "/json-log/" + final_rev
                clog_obj = self._get_clog(clog_url)

        if clogs_seen >= MAX_TIPFILL_CLOGS:
            Log.error(
                "Too many changesets, can't find last tip or the number is too high: {{rev}}. "
                "Maximum possible to request is {{maxnum}}",
                rev=coalesce(newest_known_rev, csets_to_gather),
                maxnum=MAX_TIPFILL_CLOGS * CHANGESETS_PER_CLOG,
            )
            return False

        with self.working_locker:
            Log.note("Adding {{csets}}", csets=csets_to_add)
            self.add_cset_entries(csets_to_add, timestamp=False)
        return True

    def fill_forward_continuous(self, please_stop=None):
        while not please_stop:
            try:
                while not please_stop and not self.disable_tipfilling and self.update_tip():
                    pass
                (please_stop | Till(seconds=CSET_TIP_WAIT_TIME)).wait()
            except Exception as e:
                Log.warning("Unknown error occurred during tip filling:", cause=e)

    def get_old_cset_revnum(self, revision):
        self.csets_todo_backwards.add((revision, True))

        revnum = None
        timeout = Till(seconds=BACKFILL_REVNUM_TIMEOUT)
        while not timeout:
            revnum = self._get_one_revnum(revision)

            if revnum != None:
                break
            else:
                Log.note("Waiting for backfill to complete...")
            Till(seconds=CSET_BACKFILL_WAIT_TIME).wait()

        if timeout:
            Log.error(
                "Cannot find revision {{rev}} after waiting {{timeout}} seconds",
                rev=revision,
                timeout=BACKFILL_REVNUM_TIMEOUT,
            )
        return revnum

    def get_revnnums_from_range(self, revision1, revision2):
        revnum1 = self._get_one_revnum(revision1)
        revnum2 = self._get_one_revnum(revision2)
        if revnum1 == None or revnum2 == None:
            did_an_update = self.update_tip()
            if did_an_update:
                revnum1 = self._get_one_revnum(revision1)
                revnum2 = self._get_one_revnum(revision2)

            if revnum1 == None:
                revnum1 = self.get_old_cset_revnum(revision1)
                # Refresh the second entry
                revnum2 = self._get_one_revnum(revision2)

            if revnum2 == None:
                revnum2 = self.get_old_cset_revnum(revision2)

                # The first revnum might change also
                revnum1 = self._get_one_revnum(revision1)

        result = self._get_revnum_range(revnum1, revnum2)
        return sorted(result, key=lambda x: int(x[0]))

    def caching_daemon(self, please_stop=None):
        """
        This daemon caches the annotations for the files available
        in the LatestFileMod to tip of csetLog table
        """
        while not please_stop:
            try:
                # Wait until gets a signal
                # to begin (or end).
                (self.caching_signal | please_stop).wait()
                Till(seconds=CACHE_WAIT_TIME).wait()

                if please_stop:
                    break
                if self.caching_signal._go == False or self.disable_caching:
                    continue

                # Get current tip
                tip_revision = self.get_tip()[1]
                with self.conn.transaction() as t:
                    files_to_update = t.get(
                        "SELECT file FROM latestFileMod WHERE revision != ? limit 1000",
                        (tip_revision),
                    )

                for _, fs in jx.chunk(files_to_update, size=CACHING_BATCH_SIZE):
                    if self.caching_signal._go == False:
                        break
                    files = [f[0] for f in fs]
                    # Update file to the tip revision
                    self.tuid_service.get_tuids_from_files(files, tip_revision, etl=False)
            except Exception as e:
                Log.warning("Unknown error occurred during caching: ", cause=e)


CSETLOG_SCHEMA = {
    "settings": {"index.number_of_replicas": 1, "index.number_of_shards": 1},
    "mappings": {
        "csetlogtype": {
            "_all": {"enabled": False},
            "properties": {
                "revnum": {"type": "integer", "store": True},
                "revision": {"type": "keyword", "store": True},
                "timestamp": {"type": "integer", "store": True},
            },
        }
    },
}
