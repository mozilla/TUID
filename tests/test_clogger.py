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

import pytest

from mo_dots import Null
from mo_logs import Log, Except
from mo_threads import Thread, Till
from mo_times import Timer
from pyLibrary.env import http
from pyLibrary.sql import sql_list, sql_iso
from pyLibrary.sql.sqlite import quote_value, DOUBLE_TRANSACTION_ERROR
from tuid.clogger import Clogger
from tuid import sql

_clogger = None
_conn = None

DEBUG = False
HG_URL = "https://hg.mozilla.org/"

@pytest.fixture
def clogger(config, new_db):
    global _clogger
    global _conn
    _conn = sql.Sql(config.tuid.database.name)
    if new_db == 'yes':
        return Clogger(conn=_conn, new_table=True, kwargs=config)
    elif new_db == 'no':
        if _clogger is None:
            _clogger = Clogger(conn=_conn, new_table=True, kwargs=config)
        return _clogger
    else:
        Log.error("expecting 'yes' or 'no'")


def test_initializing(clogger):
    # If clogger is set up properly
    # this test will always pass
    assert clogger


def test_tipfilling(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_deletion = True
    clogger.disable_maintenance = True
    clogger.csetlog.flush(forced=True)

    num_trys = 50
    wait_time = 2
    current_tip = None
    query = {
        "_source": {"includes": ["revision", "revnum"]},
        "sort": [{"revnum": {"order": "desc"}}],
        "size": 1
    }
    result = clogger.csetlog.search(query)
    current_tip = result.hits.hits[0]._source.revision

    filter = {"match_all": {}}
    clogger.csetlog.delete_record(filter)

    clogger.disable_tipfilling = False

    new_tip = None
    while num_trys > 0:
        result = clogger.csetlog.search(query)
        new_tip = (result.hits.hits[0]._source.revnum, result.hits.hits[0]._source.revision)
        if new_tip:
            if current_tip == new_tip[1]:
                new_tip = new_tip[1]
                break
        num_trys -= 1
        Till(seconds=wait_time).wait()

    assert num_trys > 0
    assert current_tip == new_tip


def test_backfilling_to_revision(clogger):
    clogger.disable_backfilling = False
    clogger.disable_tipfilling = True
    clogger.disable_deletion = True
    clogger.disable_maintenance = True
    clogger.csetlog.flush(forced=True)

    num_trys = 50
    wait_time = 2
    num_to_go_back = 10

    query = {
        "_source": {"includes": ["revision", "revnum"]},
        "sort": [{"revnum": {"order": "asc"}}],
        "size": 1
    }
    result = clogger.csetlog.search(query)
    oldest_revnum = result.hits.hits[0]._source.revnum
    oldest_rev = result.hits.hits[0]._source.revision

    new_old_rev = None
    clog_url = HG_URL + clogger.config.hg.branch + '/' + 'json-log/' + oldest_rev
    clog_obj_list = list(clogger._get_clog(clog_url)['changesets'])
    for count, clog_obj in enumerate(clog_obj_list[1:]):
        if count + 1 >= num_to_go_back:
            new_old_rev = clog_obj['node'][:12]
            break

    clogger.csets_todo_backwards.add((new_old_rev, True))

    new_ending = None
    while num_trys > 0:
        query = {
            "_source": {"includes": ["revision", "revnum"]},
            "sort": [{"revnum": {"order": "asc"}}],
            "size": 1
        }
        result = clogger.csetlog.search(query)
        new_ending = result.hits.hits[0]._source.revision
        DEBUG and Log.note("{{data}}", data=(oldest_rev, new_old_rev, new_ending))
        if new_ending == new_old_rev:
            break
        else:
            Till(seconds=wait_time).wait()
            num_trys -= 1

    assert num_trys > 0
    assert new_old_rev == new_ending

    # Check that revnum's were properly handled
    expected_revnum = oldest_revnum - num_to_go_back
    query = {
            "_source": {"includes": ["revnum"]},
            "query": { "bool": { "must": [ { "term": { "revision": new_ending } } ] } },
            "size": 1
    }
    result = clogger.csetlog.search(query)
    new_revnum = result.hits.hits[0]._source.revnum
    assert expected_revnum == new_revnum


def test_backfilling_by_count(clogger):
    clogger.disable_backfilling = False
    clogger.disable_tipfilling = True
    clogger.disable_deletion = True
    clogger.disable_maintenance = True
    clogger.csetlog.flush(forced=True)

    num_trys = 50
    wait_time = 2
    num_to_go_back = 10

    query = {
        "_source": {"includes": ["revision", "revnum"]},
        "sort": [{"revnum": {"order": "asc"}}],
        "size": 1
    }
    result = clogger.csetlog.search(query)
    oldest_revnum = result.hits.hits[0]._source.revnum
    oldest_rev = result.hits.hits[0]._source.revision

    new_old_rev = None
    clog_url = HG_URL + clogger.config.hg.branch + '/' + 'json-log/' + oldest_rev
    clog_obj_list = list(clogger._get_clog(clog_url)['changesets'])
    for count, clog_obj in enumerate(clog_obj_list[1:]):
        if count >= num_to_go_back - 1:
            new_old_rev = clog_obj['node'][:12]
            break

    clogger.csets_todo_backwards.add((num_to_go_back, True))

    new_ending = None
    new_revnum = None
    while num_trys > 0:
        with clogger.conn.transaction() as t:
            query = {
                "_source": {"includes": ["revision", "revnum"]},
                "sort": [{"revnum": {"order": "asc"}}],
                "size": 1
            }
            result = clogger.csetlog.search(query)
            new_ending = result.hits.hits[0]._source.revision
            DEBUG and Log.note("{{data}}", data=(oldest_rev, new_old_rev, new_ending))
            if new_ending == new_old_rev:
                query = {
                    "_source": {"includes": ["revnum"]},
                    "query": {"bool": {"must": [{"term": {"revision": new_ending}}]}},
                    "size": 1
                }
                result = clogger.csetlog.search(query)
                new_revnum = result.hits.hits[0]._source.revnum
                break
        if new_ending != new_old_rev:
            Till(seconds=wait_time).wait()
            num_trys -= 1

    assert num_trys > 0
    assert new_old_rev == new_ending

    # Check that revnum's were properly handled
    expected_revnum = oldest_revnum - num_to_go_back
    assert expected_revnum == new_revnum


def test_maintenance_and_deletion(clogger):
    # IMPORTANT: Assumes that the max csets is 100
    clogger.disable_tipfilling = True

    # Temporarily disable these workers
    clogger.disable_maintenance = True
    clogger.disable_deletion = True
    clogger.csetlog.flush(forced=True)

    max_revs = 100
    extra_to_add = 50
    num_trys = 50
    wait_time = 2

    with clogger.conn.transaction() as t:
        query = {
            "aggs": {"output": {"value_count": {"field": "revnum"}}},
            "size": 0
        }
        revnums_in_db = int(clogger.csetlog.search(query).aggregations.output.value)

    num_csets_missing = max_revs - revnums_in_db
    if num_csets_missing > 0:
        extra_to_add += num_csets_missing

    with clogger.conn.transaction() as t:
        _, tail_cset = clogger.get_tail(t)

    clogger.csets_todo_backwards.add((extra_to_add, True))
    clogger.disable_backfilling = False
    new_tail = None
    tmp_num_trys = 0
    while tmp_num_trys < num_trys:
        Till(seconds=wait_time).wait()
        with clogger.conn.transaction() as t:
            _, new_tail = clogger.get_tail(t)
        if new_tail != tail_cset:
            break
        tmp_num_trys += 1
    assert tmp_num_trys < num_trys

    inserts_list_latestFileMod = [
        ('file1', new_tail),
        ('file2', new_tail)
    ]
    inserts_list_annotations = [
        ('file1', new_tail, ''),
        ('file2', new_tail, '')
    ]
    with clogger.conn.transaction() as t:
        t.execute(
            "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
            sql_list(
                sql_iso(sql_list(map(quote_value, i)))
                for i in inserts_list_latestFileMod
            )
        )
        t.execute(
            "INSERT OR REPLACE INTO annotations (file, revision, annotation) VALUES " +
            sql_list(
                sql_iso(sql_list(map(quote_value, i)))
                for i in inserts_list_annotations
            )
        )
        query = {
            "aggs": {"output": {"value_count": {"field": "revnum"}}},
            "size": 0
        }
        revnums_in_db = int(clogger.csetlog.search(query).aggregations.output.value)
    if revnums_in_db <= max_revs:
        Log.note("Maintenance worker already ran.")
        assert True
        return

    clogger.disable_backfilling = True
    clogger.disable_deletion = False
    clogger.disable_maintenance = False

    wait_time = 10
    tmp_num_trys = 0
    while tmp_num_trys < num_trys:
        Till(seconds=wait_time).wait()
        query = {
            "aggs": {"output": {"value_count": {"field": "revnum"}}},
            "size": 0
        }
        revnums_in_db = int(clogger.csetlog.search(query).aggregations.output.value)
        if revnums_in_db <= max_revs:
            break
        tmp_num_trys += 1

    assert tmp_num_trys < num_trys

    # Check that latestFileMods entries were deleted.
    latest_rev = clogger.conn.get_one("SELECT 1 FROM latestFileMod WHERE revision=?", (new_tail,))
    assert not latest_rev

    # Check that annotations were deleted.
    annotates = clogger.conn.get_one("SELECT 1 FROM annotations WHERE revision=?", (new_tail,))
    assert not annotates


def test_deleting_old_annotations(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True
    clogger.csetlog.flush(forced=True)

    min_permanent = 10
    num_trys = 50
    wait_time = 2
    new_timestamp = 1

    # Add extra non-permanent revisions if needed
    query = {
            "aggs": {"output": {"value_count": {"field": "revnum"}}},
            "size": 0
        }
    clogger.csetlog.flush(forced=True)
    total_revs = int(clogger.csetlog.search(query).aggregations.output.value)
    if total_revs <= min_permanent:
        clogger.csets_todo_backwards.add((50, True))
        clogger.disable_backfilling = False

        tmp_num_trys = 0
        while tmp_num_trys < num_trys:
            Till(seconds=wait_time).wait()
            query = {
                "aggs": {"output": {"value_count": {"field": "revnum"}}},
                "size": 0
            }
            new_total_revs = int(clogger.csetlog.search(query).aggregations.output.value)
            if new_total_revs > total_revs:
                break
            tmp_num_trys += 1
        assert tmp_num_trys < num_trys

        clogger.disable_backfilling = True

    with clogger.conn.transaction() as t:
        tail_tipnum, tail_cset = clogger.get_tail(t)

    inserts_list_latestFileMod = [
        ('file1', tail_cset),
        ('file2', tail_cset)
    ]
    inserts_list_annotations = [
        ('file1', tail_cset, ''),
        ('file2', tail_cset, '')
    ]

    with clogger.conn.transaction() as t:
        t.execute(
            "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
            sql_list(
                sql_iso(sql_list(map(quote_value, i)))
                for i in inserts_list_latestFileMod
            )
        )
        t.execute(
            "INSERT OR REPLACE INTO annotations (file, revision, annotation) VALUES " +
            sql_list(
                sql_iso(sql_list(map(quote_value, i)))
                for i in inserts_list_annotations
            )
        )
        for revnum, revision, timestamp in [(tail_tipnum, tail_cset, new_timestamp)]:
            record = {"revnum": revnum, "revision": revision, "timestamp": timestamp}
            clogger.csetlog.add({"value": record})
            while revnum != clogger._get_revnum_exists("t", revnum):
                Till(seconds=.001).wait()

    # Start maintenance
    clogger.disable_maintenance = False
    clogger.maintenance_signal.go()

    tmp_num_trys = 0
    while tmp_num_trys < num_trys:
        Till(seconds=wait_time).wait()
        latest_rev = clogger.conn.get_one("SELECT 1 FROM latestFileMod WHERE revision=?", (tail_cset,))
        annotates = clogger.conn.get_one("SELECT 1 FROM annotations WHERE revision=?", (tail_cset,))
        if not annotates and not latest_rev:
            break
        tmp_num_trys += 1
    assert tmp_num_trys < num_trys


def test_partial_tipfilling(clogger):
    clogger.csetlog.flush(forced=True)
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True
    clogger.csetlog.flush(forced=True)

    num_trys = 50
    wait_time = 2
    clogger.csetlog.flush(forced=True)
    query = {
        "aggs": {"output": {"value_count": {"field": "revnum"}}},
        "size": 0
    }
    prev_total_revs = int(clogger.csetlog.search(query).aggregations.output.value)
    with clogger.conn.transaction() as t:
        max_tip_num, _ = clogger.get_tip(t)
        filter = {"bool": { "must": [{"range": {"revnum": {"gte": max_tip_num-5}}}] } }
        clogger.csetlog.delete_record(filter)
        query = {"query": filter}
        result = clogger.csetlog.search(query)
        while len(result.hits.hits) != 0:
            Till(seconds=.001).wait()
            result = clogger.csetlog.search(query)

    clogger.disable_tipfilling = False
    tmp_num_trys = 0
    while tmp_num_trys < num_trys:
        Till(seconds=wait_time).wait()
        query = {
            "aggs": {"output": {"value_count": {"field": "revnum"}}},
            "size": 0
        }
        revnums_in_db = int(clogger.csetlog.search(query).aggregations.output.value)
        if revnums_in_db == prev_total_revs:
            break
        tmp_num_trys += 1
    assert tmp_num_trys < num_trys


def test_get_revnum_range_backfill(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True
    clogger.csetlog.flush(forced=True)

    # Get the current tail, go 10 changesets back and request
    # the final one as the second revision.
    with clogger.conn.transaction() as t:
        rev1, oldest_rev = clogger.get_tail(t)

    num_to_go_back = 10
    rev2 = None
    clog_url = HG_URL + clogger.config.hg.branch + '/' + 'json-log/' + oldest_rev
    clog_obj_list = list(clogger._get_clog(clog_url)['changesets'])
    for count, clog_obj in enumerate(clog_obj_list[1:]):
        if count >= num_to_go_back - 1:
            rev2 = clog_obj['node'][:12]
            break

    assert oldest_rev and rev2

    clogger.disable_backfilling = False
    revnums = clogger.get_revnnums_from_range(oldest_rev, rev2)

    assert len(revnums) == 11

    for revnum, revision in revnums:
        assert revision


def test_get_revnum_range_tipfill(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True
    clogger.csetlog.flush(forced=True)

    # Get the current tip, delete it, then request it's
    # revnum range up to a known revision
    with clogger.conn.transaction() as t:
        tip_num, tip_rev = clogger.get_tip(t)
        filter = {"bool": { "must": [{"range": {"revnum": {"gte": tip_num-5}}}] } }
        clogger.csetlog.delete_record(filter)
        query = {"query": filter}
        result = clogger.csetlog.search(query)
        while len(result.hits.hits) != 0:
            Till(seconds=.001).wait()
            result = clogger.csetlog.search(query)

        _, new_tip_rev = clogger.get_tip(t)

    assert tip_rev and new_tip_rev

    # Test out of order revision requests here also
    revnums = clogger.get_revnnums_from_range(new_tip_rev, tip_rev)

    assert len(revnums) == 7

    for revnum, revision in revnums:
        assert revision


def test_get_revnum_range_tipnback(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True
    clogger.csetlog.flush(forced=True)

    for ordering in range(2):
        # Used for testing output
        query = {
            "aggs": {"output": {"value_count": {"field": "revnum"}}},
            "size": 0
        }
        prev_total_revs = int(clogger.csetlog.search(query).aggregations.output.value)
        expected_total_revs = prev_total_revs + 10

        # Get the current tail, go 10 changesets back and request
        # the final one as the second revision.
        with clogger.conn.transaction() as t:
            rev1, oldest_rev = clogger.get_tail(t)

        # Get the current tip, delete it, then request it's revnum range
        # to a non-existent (backfill required) revision in the past.
        with clogger.conn.transaction() as t:
            tip_num, tip_rev = clogger.get_tip(t)
            filter = {"bool": {"must": [{"range": {"revnum": {"gte": tip_num - 5}}}]}}
            clogger.csetlog.delete_record(filter)
            query = {"query": filter}
            result = clogger.csetlog.search(query)
            while len(result.hits.hits) != 0:
                Till(seconds=.001).wait()
                result = clogger.csetlog.search(query)

            _, new_tip_rev = clogger.get_tip(t)

        num_to_go_back = 10
        rev2 = None
        clog_url = HG_URL + clogger.config.hg.branch + '/' + 'json-log/' + oldest_rev
        clog_obj_list = list(clogger._get_clog(clog_url)['changesets'])
        for count, clog_obj in enumerate(clog_obj_list[1:]):
            if count >= num_to_go_back - 1:
                rev2 = clog_obj['node'][:12]
                break

        assert rev2 and new_tip_rev

        clogger.disable_backfilling = False
        if ordering == 0:
            revnums = clogger.get_revnnums_from_range(tip_rev, rev2)
        else:
            revnums = clogger.get_revnnums_from_range(rev2, tip_rev)
        clogger.disable_backfilling = True

        assert len(revnums) == expected_total_revs

        for revnum, revision in revnums:
            assert revision