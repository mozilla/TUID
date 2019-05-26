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
from jx_python import jx
from mo_dots import Null, coalesce, set_default
from mo_hg.hg_mozilla_org import HgMozillaOrg
from mo_logs import Log
from mo_logs.exceptions import suppress_exception
from mo_threads import Till, Thread, Lock, Queue, Signal
from mo_threads.threads import ALL
from mo_times.durations import DAY
from pyLibrary.env import http, elasticsearch
from pyLibrary.sql import sql_list, quote_set
from tuid import sql
from tuid.util import HG_URL, insert_into_db_chunked

RETRY = {"times": 3, "sleep": 5}
SQL_CSET_BATCH_SIZE = 500
CSET_TIP_WAIT_TIME = 5 * 60 # seconds
CSET_BACKFILL_WAIT_TIME = 1 * 60 # seconds
CSET_MAINTENANCE_WAIT_TIME = 30 * 60 # seconds
CSET_DELETION_WAIT_TIME = 1 * 60 # seconds
TUID_EXISTENCE_WAIT_TIME = 1 * 60 # seconds
TIME_TO_KEEP_ANNOTATIONS = 5 * DAY
MAX_TIPFILL_CLOGS = 400  # changeset logs
MAX_BACKFILL_CLOGS = 1000 # changeset logs
CHANGESETS_PER_CLOG = 20 # changesets
BACKFILL_REVNUM_TIMEOUT = int(MAX_BACKFILL_CLOGS * 2.5) # Assume 2.5 seconds per clog
MINIMUM_PERMANENT_CSETS = 200 # changesets
MAXIMUM_NONPERMANENT_CSETS = 1500 # changesets
SIGNAL_MAINTENANCE_CSETS = int(MAXIMUM_NONPERMANENT_CSETS + (0.2 * MAXIMUM_NONPERMANENT_CSETS))
UPDATE_VERY_OLD_FRONTIERS = False

SINGLE_CLOGGER = None

class Clogger:

    # Singleton of the look-ahead scanner Clogger
    SINGLE_CLOGGER = None
    def __new__(cls, *args, **kwargs):
        if cls.SINGLE_CLOGGER is None:
            cls.SINGLE_CLOGGER = object.__new__(cls)
        return cls.SINGLE_CLOGGER


    def __init__(self, conn=None, tuid_service=None, start_workers=True, new_table=False, kwargs=None):
        try:
            self.config = kwargs
            self.conn = conn if conn else sql.Sql(self.config.database.name)
            self.hg_cache = HgMozillaOrg(kwargs=self.config.hg_cache, use_cache=True) if self.config.hg_cache else Null
            self.esconfig = None
            if self.config.tuid:
                self.esconfig = self.config.tuid.esclogger.csetLog
            else:
                self.esconfig = self.config.esclogger.csetLog
            self.es = elasticsearch.Cluster(kwargs=self.esconfig)

            self.tuid_service = tuid_service if tuid_service else tuid.service.TUIDService(
                kwargs=self.config.tuid, conn=self.conn, clogger=self
            )
            self.rev_locker = Lock()
            self.working_locker = Lock()
            self.csetLog_locker = Lock()

            if new_table:
                try:
                    index = self.es.get_canonical_index(self.esconfig.index)
                    response = self.es.delete_index(index)
                except Exception as e:
                    Log.warning("could not delete csetlog index because (mostly index has not yet created): {{cause}}", cause=str(e))


            self.init_db()
            query = self.min_max_dsl("max")
            self.next_revnum = coalesce(eval(str(self.csetlog.search(query).aggregations.value.value)),0)+1

            self.csets_todo_backwards = Queue(name="Clogger.csets_todo_backwards")
            self.deletions_todo = Queue(name="Clogger.deletions_todo")
            self.maintenance_signal = Signal(name="Clogger.maintenance_signal")

            if 'tuid' in self.config:
                self.config = self.config.tuid

            self.disable_backfilling = False
            self.disable_tipfilling = False
            self.disable_deletion = False
            self.disable_maintenance = False

            self.backfill_thread = None
            self.tipfill_thread = None
            self.deletion_thread = None
            self.maintenance_thread = None

            # Make sure we are filled before allowing queries
            query = {
                "aggs": {"output": {"value_count": {"field": "revnum"}}},
                "size": 0
            }
            numrevs = int(self.csetlog.search(query).aggregations.output.value)
            if numrevs < MINIMUM_PERMANENT_CSETS:
                Log.note("Filling in csets to hold {{minim}} csets.", minim=MINIMUM_PERMANENT_CSETS)
                oldest_rev = 'tip'

                query = {
                    "_source" : { "includes" : ["revision"]},
                    "sort": [{ "revnum": { "order": "asc" }}],
                    "size": 1
                }
                tmp = self.csetlog.search(query).hits.hits[0]._source.revision
                if tmp:
                    oldest_rev = tmp
                self._fill_in_range(
                    MINIMUM_PERMANENT_CSETS - numrevs,
                    oldest_rev,
                    timestamp=False
                )

            Log.note(
                "Table is filled with atleast {{minim}} entries.",
                minim=MINIMUM_PERMANENT_CSETS
            )

            if start_workers:
                self.start_workers()

        except Exception as e:
            Log.warning("Cannot setup clogger: {{cause}}", cause=str(e))


    def min_max_dsl(self, query_required):
        query = None
        if query_required == "min":
            query = {
                "size" : 0,
                "aggs" : {
                    "value" : { "min" : { "field" : "revnum" } }
                }
            }

        elif query_required == "max":
            query = {
                "size" : 0,
                "aggs" : {
                    "value" : { "max" : { "field" : "revnum" } }
                }
            }
        return query


    def start_backfilling(self):
        if not self.backfill_thread:
            self.backfill_thread = Thread.run('clogger-backfill', self.fill_backward_with_list)


    def start_tipfillling(self):
        if not self.tipfill_thread:
            self.tipfill_thread = Thread.run('clogger-tip', self.fill_forward_continuous)


    def start_maintenance(self):
        if not self.maintenance_thread:
            self.maintenance_thread = Thread.run('clogger-maintenance', self.csetLog_maintenance)


    def start_deleter(self):
        if not self.deletion_thread:
            self.deletion_thread = Thread.run('clogger-deleter', self.csetLog_deleter)


    def start_workers(self):
        self.start_tipfillling()
        self.start_backfilling()
        self.start_maintenance()
        self.start_deleter()
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
        self.disable_maintenance = True
        self.disable_deletion = True


    def revnum(self):
        """
        :return: max revnum that was added
        """
        query = self.min_max_dsl("max")
        tmp = coalesce(eval(str(self.csetlog.search(query).aggregations.value.value)),0)
        return tmp

    def get_tip(self):
        query = {
            "_source": {"includes": ["revision"]},
            "sort": [{"revnum": {"order": "desc"}}],
            "size": 1
        }
        result = self.csetlog.search(query)
        tmp = (result.hits.hits[0].sort[0], result.hits.hits[0]._source.revision)
        return tmp


    def get_tail(self):
        query = {
            "_source": {"includes": ["revision"]},
            "sort": [{"revnum": {"order": "asc"}}],
            "size": 1
        }
        result = self.csetlog.search(query)
        tmp = (result.hits.hits[0].sort[0],result.hits.hits[0]._source.revision)
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
                error=e
            )


    def _get_one_revision(self, cset_entry):
        # Returns a single revision if it exists
        _, rev, _ = cset_entry
        query = {
            "_source": {"includes": ["revision"]},
            "query": { "bool": { "must": [ { "term": { "revision": rev } } ] } },
            "size": 1
        }
        temp = self.csetlog.search(query).hits.hits[0]._source.revision
        if temp == 0 or temp:
            return (temp, )
        else:
            return None


    def _get_one_revnum(self, rev):
        # Returns a single revnum if it exists
        query = {
            "_source": {"includes": ["revnum"]},
            "query": {
                "bool": {
                    "must": [{ "term": { "revision": rev} }]
                }
            },
            "size": 1
        }
        temp = self.csetlog.search(query).hits.hits[0]._source.revnum
        if temp == 0 or temp:
            return (temp, )
        else:
            return None


    def _get_revnum_exists(self, rev):
        # Returns a single revnum if it exists
        query = {
            "_source": {"includes": ["revnum"]},
            "query": {
                "bool": {
                    "must": [{ "term": { "revnum": rev} }]
                }
            },
            "size": 1
        }
        temp = self.csetlog.search(query).hits.hits[0]._source.revnum
        if temp == 0 or temp:
            return (temp, )
        else:
            return None


    def _get_revnum_range(self, revnum1, revnum2):
        # Returns a range of revision numbers (that is inclusive)
        high_num = max(revnum1, revnum2)
        low_num = min(revnum1, revnum2)

        total = self.csetlog.search({"size": 0})
        query = {
            "size": total.hits.total,
            "_source": {"includes": ["revnum", "revision"]},
            "query": {
                "bool": {
                    "must": [{"range": {"revnum": {"gte": low_num, "lte": high_num}}}]
                }
            }
        }
        result = self.csetlog.search(query).hits.hits
        temp = []
        for r in result:
            temp.append((r._source.revnum, r._source.revision))
        return temp


    def check_for_maintenance(self):
        '''
        Returns True if the maintenance worker should be run now,
        and False otherwise.
        :return:
        '''
        query = {
            "aggs": {"output": {"value_count": {"field": "revnum"}}},
            "size": 0
        }
        numrevs = int(self.csetlog.search(query).aggregations.output.value)

        Log.note("Number of csets in csetLog table: {{num}}", num=numrevs)
        if numrevs >= SIGNAL_MAINTENANCE_CSETS:
            return True
        return False


    def add_cset_entries(self, ordered_rev_list, timestamp=False, number_forward=True):
        '''
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
        '''

        query = self.min_max_dsl("min")
        current_min = coalesce(eval(str(self.csetlog.search(query).aggregations.value.value)), 0)
        query = self.min_max_dsl("max")
        current_max = coalesce(eval(str(self.csetlog.search(query).aggregations.value.value)), 0)

        direction = -1
        start = current_min - 1
        if number_forward:
            direction = 1
            start = current_max + 1
            ordered_rev_list = ordered_rev_list[::-1]

        insert_list = [
            (
                start + direction * count,
                rev,
                int(time.time()) if timestamp else -1
            )
            for count, rev in enumerate(ordered_rev_list)
        ]

        # In case of overlapping requests
        fmt_insert_list = []
        for cset_entry in insert_list:
            tmp = self._get_one_revision(cset_entry)
            if not tmp:
                fmt_insert_list.append(cset_entry)

        for _, tmp_insert_list in jx.groupby(fmt_insert_list, size=SQL_CSET_BATCH_SIZE):
            for revnum, revision, timestamp in tmp_insert_list:
                record={"_id":revnum, "revnum":revnum, "revision":revision, "timestamp":timestamp}
                self.csetlog.add({"value":record})
                self.csetlog.refresh()
                while not self._get_revnum_exists(revnum):
                    Till(seconds=.001).wait()

        # Start a maintenance run if needed
        if self.check_for_maintenance():
            Log.note("Scheduling maintenance run on clogger.")
            self.maintenance_signal.go()



    def _fill_in_range(self, parent_cset, child_cset, timestamp=False, number_forward=True):
        '''
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
        '''
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
                maxnum=MAX_BACKFILL_CLOGS * CHANGESETS_PER_CLOG
            )
            return None

        csets_found = 0
        clogs_seen = 0
        final_rev = child_cset
        while not found_parent and clogs_seen < MAX_BACKFILL_CLOGS:
            clog_url = str(HG_URL) + "/" + self.config.hg.branch + "/json-log/" + final_rev
            clog_obj = self._get_clog(clog_url)
            clog_csets_list = list(clog_obj['changesets'])
            for clog_cset in clog_csets_list[:-1]:
                if not number_forward and csets_found <= 0:
                    # Skip this entry it already exists
                    csets_found += 1
                    continue

                nodes_cset = clog_cset['node'][:12]
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
            final_rev = clog_csets_list[-1]['node'][:12]

        if found_parent:
            self.add_cset_entries(csets_to_add, timestamp=timestamp, number_forward=number_forward)
        else:
            Log.warning(
                "Couldn't find the end of the request for {{request}}. "
                "Max number that can be requested through _fill_in_range is {{maxnum}}.",
                request={
                    'parent_cset': parent_cset,
                    'child_cset':child_cset,
                    'number_forward': number_forward
                },
                maxnum=MAX_BACKFILL_CLOGS * CHANGESETS_PER_CLOG
            )
            return None
        return csets_to_add



    def initialize_to_range(self, old_rev, new_rev, delete_old=True):
        '''
        Used in service testing to get to very old
        changesets quickly.
        :param old_rev: The oldest revision to keep
        :param new_rev: The revision to start searching from
        :return:
        '''
        old_settings = [
            self.disable_tipfilling,
            self.disable_backfilling,
            self.disable_maintenance,
            self.disable_deletion
        ]
        self.disable_tipfilling = True
        self.disable_backfilling = True
        self.disable_maintenance = True
        self.disable_deletion = True

        old_rev = old_rev[:12]
        new_rev = new_rev[:12]

        with self.working_locker:
            if delete_old:
                filter = { "match_all": {} }
                self.csetlog.delete_record(filter)
                self.csetlog.refresh()
                query = { "size": 0 }
                result = self.csetlog.search(query)
                while result.hits.total != 0:
                    Till(seconds=.001).wait()
                    result = self.csetlog.search(query)


            #since no auto addition possible
            query = self.min_max_dsl("max")
            max_revnum = coalesce(eval(str(self.csetlog.search(query).aggregations.value.value)), 0) + 1
            record = {"_id":max_revnum, "revnum": max_revnum, "revision": new_rev, "timestamp": -1}
            self.csetlog.add({"value": record})
            self.csetlog.refresh()
            while not self._get_revnum_exists(max_revnum):
                Till(seconds=.001).wait()

            self._fill_in_range(old_rev, new_rev, timestamp=True, number_forward=False)

        self.disable_tipfilling = old_settings[0]
        self.disable_backfilling = old_settings[1]
        self.disable_maintenance = old_settings[2]
        self.disable_deletion = old_settings[3]


    def fill_backward_with_list(self, please_stop=None):
        '''
        Expects requests of the tuple form: (parent_cset, timestamp)
        parent_cset can be an int X to go back by X changesets, or
        a string to search for going backwards in time. If timestamp
        is false, no timestamps will be added to the entries.
        :param please_stop:
        :return:
        '''
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
                    if parent_revnum:
                        continue

                    _, oldest_revision = self.get_tail()

                    self._fill_in_range(
                        parent_cset,
                        oldest_revision,
                        timestamp=timestamp,
                        number_forward=False
                    )
                Log.note("Finished {{cset}}", cset=parent_cset)
            except Exception as e:
                Log.warning("Unknown error occurred during backfill: ", cause=e)


    def update_tip(self):
        '''
        Returns False if the tip is already at the newest, or True
        if an update has taken place.
        :return:
        '''
        clog_obj = self._get_clog(
            str(HG_URL) + "/" + self.config.hg.branch + "/json-log/tip"
        )

        _, newest_known_rev = self.get_tip()

        # If we are still at the newest, wait for CSET_TIP_WAIT_TIME seconds
        # before checking again.
        first_clog_entry = clog_obj['changesets'][0]['node'][:12]
        if newest_known_rev == first_clog_entry:
            return False

        csets_to_gather = None
        if not newest_known_rev:
            Log.note(
                "No revisions found in table, adding {{minim}} entries...",
                minim=MINIMUM_PERMANENT_CSETS
            )
            csets_to_gather = MINIMUM_PERMANENT_CSETS

        found_newest_known = False
        csets_to_add = []
        csets_found = 0
        clogs_seen = 0
        Log.note("Found new revisions. Updating csetLog tip to {{rev}}...", rev=first_clog_entry)
        while not found_newest_known and clogs_seen < MAX_TIPFILL_CLOGS:
            clog_csets_list = list(clog_obj['changesets'])
            for clog_cset in clog_csets_list[:-1]:
                nodes_cset = clog_cset['node'][:12]
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
                final_rev = clog_csets_list[-1]['node'][:12]
                clog_url = str(HG_URL) + "/" + self.config.hg.branch + "/json-log/" + final_rev
                clog_obj = self._get_clog(clog_url)

        if clogs_seen >= MAX_TIPFILL_CLOGS:
            Log.error(
                "Too many changesets, can't find last tip or the number is too high: {{rev}}. "
                "Maximum possible to request is {{maxnum}}",
                rev=coalesce(newest_known_rev, csets_to_gather),
                maxnum=MAX_TIPFILL_CLOGS * CHANGESETS_PER_CLOG
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


    def csetLog_maintenance(self, please_stop=None):
        '''
        Handles deleting old csetLog entries and timestamping
        revisions once they pass the length for permanent
        storage for deletion later.
        :param please_stop:
        :return:
        '''
        while not please_stop:
            try:
                # Wait until something signals the maintenance cycle
                # to begin (or end).
                (self.maintenance_signal | please_stop).wait()

                if please_stop:
                    break
                if self.disable_maintenance:
                    continue

                Log.warning(
                    "Starting clog maintenance. Since this doesn't start often, "
                    "we need to explicitly see when it's started with this warning."
                )

                # Reset signal so we don't request
                # maintenance infinitely.
                self.csetlog.refresh()
                with self.maintenance_signal.lock:
                    self.maintenance_signal._go = False

                with self.working_locker:
                    all_data = None
                    total = self.csetlog.search({"size":0})
                    query = {
                        "size": total.hits.total,
                        "_source": {"includes": ["revnum","revision","timestamp"]},
                        "sort": [{"revnum": {"order": "asc"}}]
                    }
                    temp = self.csetlog.search(query)
                    all_data = []
                    for i in temp.hits.hits:
                        all_data.append((i._source.revnum,i._source.revision,i._source.timestamp))

                    # Restore maximum permanents (if overflowing)
                    new_data = []
                    modified = False
                    for count, (revnum, revision, timestamp) in enumerate(all_data[::-1]):
                        if count < MINIMUM_PERMANENT_CSETS:
                            if timestamp != -1:
                                modified = True
                                new_data.append((revnum, revision, -1))
                            else:
                                new_data.append((revnum, revision, timestamp))
                        elif type(timestamp) != int or timestamp == -1:
                            modified = True
                            new_data.append((revnum, revision, int(time.time())))
                        else:
                            new_data.append((revnum, revision, timestamp))

                    # Delete annotations at revisions with timestamps
                    # that are too old. The csetLog entries will have
                    # their timestamps reset here.
                    new_data1 = []
                    annrevs_to_del = []
                    current_time = time.time()
                    for count, (revnum, revision, timestamp) in enumerate(new_data[::-1]):
                        new_timestamp = timestamp
                        if timestamp != -1:
                            if current_time >= timestamp + TIME_TO_KEEP_ANNOTATIONS.seconds:
                                modified = True
                                new_timestamp = current_time
                                annrevs_to_del.append(revision)
                        new_data1.append((revnum, revision, new_timestamp))

                    if len(annrevs_to_del) > 0:
                        # Delete any latestFileMod and annotation entries
                        # that are too old.
                        Log.note(
                            "Deleting annotations and latestFileMod for revisions for being "
                            "older than {{oldest}}: {{revisions}}",
                            oldest=TIME_TO_KEEP_ANNOTATIONS,
                            revisions=annrevs_to_del
                        )
                        with self.conn.transaction() as t:
                            t.execute(
                                "DELETE FROM latestFileMod WHERE revision IN " +
                                quote_set(annrevs_to_del)
                            )
                            t.execute(
                                "DELETE FROM annotations WHERE revision IN " +
                                quote_set(annrevs_to_del)
                            )

                    # Delete any overflowing entries
                    new_data2 = new_data1
                    reved_all_data = all_data[::-1]
                    deleted_data = reved_all_data[MAXIMUM_NONPERMANENT_CSETS:]
                    delete_overflowing_revstart = None
                    if len(deleted_data) > 0:
                        _, delete_overflowing_revstart, _ = deleted_data[0]
                        new_data2 = set(all_data) - set(deleted_data)

                        # Update old frontiers if requested, otherwise
                        # they will all get deleted by the csetLog_deleter
                        # worker
                        if UPDATE_VERY_OLD_FRONTIERS:
                            _, max_revision, _ = all_data[-1]
                            for _, revision, _ in deleted_data:
                                with self.conn.transaction() as t:
                                    old_files = t.get(
                                        "SELECT file FROM latestFileMod WHERE revision=?",
                                        (revision,)
                                    )
                                if old_files is None or len(old_files) <= 0:
                                    continue

                                self.tuid_service.get_tuids_from_files(
                                    old_files,
                                    max_revision,
                                    going_forward=True,
                                )

                                still_exist = True
                                while still_exist and not please_stop:
                                    Till(seconds=TUID_EXISTENCE_WAIT_TIME).wait()
                                    with self.conn.transaction() as t:
                                        old_files = t.get(
                                            "SELECT file FROM latestFileMod WHERE revision=?",
                                            (revision,)
                                        )
                                    if old_files is None or len(old_files) <= 0:
                                        still_exist = False

                    # Update table and schedule a deletion
                    if modified:
                        for revnum, revision, timestamp in new_data2:
                            record = {"_id":revnum, "revnum": revnum, "revision": revision, "timestamp": timestamp}
                            if not self._get_revnum_exists(revnum):
                                filter = {"term": {"revnum": revnum}}
                                self.csetlog.delete_record(filter)
                                self.csetlog.refresh()
                                query = {"query": {"term": {"revnum": revnum}}}
                                result = self.csetlog.search(query)
                                while len(result.hits.hits) != 0:
                                    Till(seconds=.001).wait()
                                    result = self.csetlog.search(query)

                            self.csetlog.add({"value": record})
                            self.csetlog.refresh()
                            while not self._get_revnum_exists(revnum):
                                Till(seconds=.001).wait()

                    if not deleted_data:
                        continue

                    Log.note("Scheduling {{num_csets}} for deletion", num_csets=len(deleted_data))
                    self.deletions_todo.add(delete_overflowing_revstart)
            except Exception as e:
                Log.warning("Unexpected error occured while maintaining csetLog, continuing to try: ", cause=e)
        return


    def csetLog_deleter(self, please_stop=None):
        '''
        Deletes changesets from the csetLog table
        and also changesets from the annotation table
        that have revisions matching the given changesets.
        Accepts lists of csets from self.deletions_todo.
        :param please_stop:
        :return:
        '''
        while not please_stop:
            try:
                request = self.deletions_todo.pop(till=please_stop)
                if please_stop:
                    break

                # If deletion is disabled, ignore the current
                # request - it will need to be re-requested.
                if self.disable_deletion:
                    Till(till=CSET_DELETION_WAIT_TIME).wait()
                    continue

                with self.working_locker:
                    first_cset = request

                    # Since we are deleting and moving stuff around in the
                    # TUID tables, we need everything to be contained in
                    # one transaction with no interruptions.
                    with self.conn.transaction() as t:
                        revnum = self._get_one_revnum(first_cset)[0]
                        total = self.csetlog.search({"size": 0})
                        query = {
                            "size": total.hits.total,
                            "_source": {"includes": ["revnum", "revision"]},
                            "query": {
                                "bool": {
                                    "must": [{"range": {"revnum": {"lte": revnum}}}]
                                }
                            }
                        }
                        csets_to_del_temp = self.csetlog.search(query)
                        csets_to_del = []
                        for r in csets_to_del_temp.hits.hits:
                            csets_to_del.append((r._source.revnum, r._source.revision))

                        csets_to_del = [cset for _, cset in csets_to_del]
                        existing_frontiers = t.query(
                            "SELECT revision FROM latestFileMod WHERE revision IN " +
                            quote_set(csets_to_del)
                        ).data

                        existing_frontiers = [existing_frontiers[i][0] for i, _ in enumerate(existing_frontiers)]
                        Log.note(
                            "Deleting all annotations and changeset log entries with revisions in the list: {{csets}}",
                            csets=csets_to_del
                        )

                        if len(existing_frontiers) > 0:
                            # This handles files which no longer exist anymore in
                            # the main branch.
                            Log.note(
                                "Deleting existing frontiers for revisions: {{revisions}}",
                                revisions=existing_frontiers
                            )
                            t.execute(
                                "DELETE FROM latestFileMod WHERE revision IN " +
                                quote_set(existing_frontiers)
                            )

                        Log.note("Deleting annotations...")
                        t.execute(
                            "DELETE FROM annotations WHERE revision IN " +
                            quote_set(csets_to_del)
                        )

                        Log.note(
                            "Deleting {{num_entries}} csetLog entries...",
                            num_entries=len(csets_to_del)
                        )

                        filter = {"terms": {"revision": csets_to_del}}
                        self.csetlog.delete_record(filter)
                        self.csetlog.refresh()
                        query = { "query": {"terms": { "revision": csets_to_del } } }
                        result = self.csetlog.search(query)
                        while len(result.hits.hits) != 0:
                            Till(seconds=.001).wait()
                            result = self.csetlog.search(query)

            except Exception as e:
                Log.warning("Unexpected error occured while deleting from csetLog:", cause=e)
                Till(seconds=CSET_DELETION_WAIT_TIME).wait()
        return


    def get_old_cset_revnum(self, revision):
        self.csets_todo_backwards.add((revision, True))

        revnum = None
        timeout = Till(seconds=BACKFILL_REVNUM_TIMEOUT)
        while not timeout:
            revnum = self._get_one_revnum(revision)

            if revnum:
                break
            else:
                Log.note("Waiting for backfill to complete...")
            Till(seconds=CSET_BACKFILL_WAIT_TIME).wait()

        if timeout:
            Log.error(
                "Cannot find revision {{rev}} after waiting {{timeout}} seconds",
                rev=revision,
                timeout=BACKFILL_REVNUM_TIMEOUT
            )
        return revnum


    def get_revnnums_from_range(self, revision1, revision2):
        revnum1 = self._get_one_revnum(revision1)
        revnum2 = self._get_one_revnum(revision2)
        if not revnum1 or not revnum2:
            did_an_update = self.update_tip()
            if did_an_update:
                revnum1 = self._get_one_revnum(revision1)
                revnum2 = self._get_one_revnum(revision2)

            if not revnum1:
                revnum1 = self.get_old_cset_revnum(revision1)
                # Refresh the second entry
                revnum2 = self._get_one_revnum(revision2)

            if not revnum2:
                revnum2 = self.get_old_cset_revnum(revision2)

                # The first revnum might change also
                revnum1 = self._get_one_revnum(revision1)

        result = self._get_revnum_range(revnum1[0], revnum2[0])
        return sorted(
            result,
            key=lambda x: int(x[0])
        )


CSETLOG_SCHEMA = {
    "settings": {
        "index.number_of_replicas": 1,
        "index.number_of_shards": 1
    },
    "mappings": {
        "csetlogtype": {
            "_all": {
                "enabled": False
            },
            "properties": {
                "revnum": {"type": "integer", "store": True},
                "revision": {"type": "keyword", "store": True},
                "timestamp": {"type": "integer", "store": True}
            }
        }
    }
}