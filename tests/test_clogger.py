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
        return Clogger(conn=_conn, kwargs=config)
    elif new_db == 'no':
        if _clogger is None:
            _clogger = Clogger(conn=_conn, kwargs=config)
        return _clogger
    else:
        Log.error("expecting 'yes' or 'no'")


def test_initializing(clogger):
    # If clogger is set up properly
    # this test will always pass
    assert clogger


def test_tipfilling(clogger):
    clogger.disable_tipfilling = False
    clogger.disable_backfilling = True
    clogger.disable_deletion = True
    clogger.disable_maintenance = True

    num_trys = 50
    wait_time = 2
    current_tip = None
    with clogger.conn.transaction() as t:
        current_tip = t.get_one("SELECT max(revnum) AS revnum, revision FROM csetLog")[1]
        t.execute("DELETE FROM csetLog")

    new_tip = None
    while num_trys > 0:
        nothing_exists = True
        new_tip = None
        while nothing_exists:
            new_tip = clogger.conn.get_one("SELECT max(revnum) AS revnum, revision FROM csetLog")[1]
            if new_tip:
                nothing_exists = False
            else:
                Till(seconds=wait_time).wait()
        if current_tip == new_tip:
            break
        num_trys -= 1

    assert num_trys > 0
    assert current_tip == new_tip


def test_backfilling_to_revision(clogger):
    clogger.disable_backfilling = False
    clogger.disable_tipfilling = True
    clogger.disable_deletion = True
    clogger.disable_maintenance = True

    num_trys = 50
    wait_time = 2
    num_to_go_back = 10

    oldest_revnum, oldest_rev = clogger.conn.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")

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
        new_ending = clogger.conn.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")[1]
        DEBUG and Log.note("{{data}}", data=(oldest_rev, new_old_rev, new_ending))
        if new_ending == new_old_rev:
            break
        else:
            Till(seconds=wait_time).wait()
            num_trys -= 1

    assert num_trys > 0
    assert new_old_rev == new_ending

    # Check that revnum's were properly handled
    expected_revnum = oldest_revnum + num_to_go_back
    with clogger.conn.transaction() as t:
        new_revnum = t.get_one("SELECT revnum FROM csetLog WHERE revision=?", (oldest_rev,))[0]
    assert expected_revnum == new_revnum


def test_backfilling_by_count(clogger):
    clogger.disable_backfilling = False
    clogger.disable_tipfilling = True
    clogger.disable_deletion = True
    clogger.disable_maintenance = True

    num_trys = 50
    wait_time = 2
    num_to_go_back = 10

    oldest_revnum, oldest_rev = clogger.conn.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")

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
            new_ending = t.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")[1]
            DEBUG and Log.note("{{data}}", data=(oldest_rev, new_old_rev, new_ending))
            if new_ending == new_old_rev:
                new_revnum = t.get_one("SELECT revnum FROM csetLog WHERE revision=?", (oldest_rev,))[0]
                break
        if new_ending != new_old_rev:
            Till(seconds=wait_time).wait()
            num_trys -= 1

    assert num_trys > 0
    assert new_old_rev == new_ending

    # Check that revnum's were properly handled
    expected_revnum = oldest_revnum + num_to_go_back
    assert expected_revnum == new_revnum


def test_maintenance_and_deletion(clogger):
    # IMPORTANT: Assumes that the max csets is 100
    clogger.disable_tipfilling = True

    # Temporarily disable these workers
    clogger.disable_maintenance = True
    clogger.disable_deletion = True

    max_revs = 100
    extra_to_add = 50
    num_trys = 50
    wait_time = 2
    with clogger.conn.transaction() as t:
        revnums_in_db = t.get_one("SELECT count(revnum) as revnum FROM csetLog")[0]

    num_csets_missing = max_revs - revnums_in_db
    if num_csets_missing > 0:
        extra_to_add += num_csets_missing

    with clogger.conn.transaction() as t:
        _, tail_cset = clogger.get_tail(t)

    clogger.csets_todo_backwards.add((extra_to_add, True))
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

        revnums_in_db = t.get_one("SELECT count(revnum) as revnum FROM csetLog")[0]
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
        revnums_in_db = clogger.conn.get_one("SELECT count(revnum) as revnum FROM csetLog")[0]
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

    min_permanent = 10
    num_trys = 50
    wait_time = 2
    new_timestamp = 1

    # Add extra non-permanent revisions if needed
    total_revs = clogger.conn.get_one("SELECT count(revnum) FROM csetLog")[0]
    if total_revs <= min_permanent:
        clogger.csets_todo_backwards.add((50, True))
        clogger.disable_backfilling = False

        tmp_num_trys = 0
        while tmp_num_trys < num_trys:
            Till(seconds=wait_time).wait()
            new_total_revs = clogger.conn.get_one("SELECT count(revnum) FROM csetLog")[0]
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
        t.execute(
            "INSERT OR REPLACE INTO csetLog (revnum, revision, timestamp) VALUES " +
            sql_iso(sql_list(map(quote_value, (tail_tipnum, tail_cset, new_timestamp))))
        )

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
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True

    num_trys = 50
    wait_time = 2
    prev_total_revs = clogger.conn.get_one("SELECT count(revnum) FROM csetLog")[0]
    with clogger.conn.transaction() as t:
        max_tip_num, _ = clogger.get_tip(t)
        t.execute(
            "DELETE FROM csetLog WHERE revnum >= " + str(max_tip_num) + " - 5"
        )

    with clogger.working_locker:
        clogger.recompute_table_revnums()

    clogger.disable_tipfilling = False
    tmp_num_trys = 0
    while tmp_num_trys < num_trys:
        Till(seconds=wait_time).wait()
        revnums_in_db = clogger.conn.get_one("SELECT count(revnum) as revnum FROM csetLog")[0]
        if revnums_in_db == prev_total_revs:
            break
        tmp_num_trys += 1
    assert tmp_num_trys < num_trys


def test_get_revnum_range_backfill(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True

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

    curr_revnum = -1
    for revnum, revision in revnums:
        assert revision
        assert revnum > curr_revnum
        curr_revnum = revnum


def test_get_revnum_range_tipfill(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True

    # Get the current tip, delete it, then request it's
    # revnum range up to a known revision
    with clogger.conn.transaction() as t:
        tip_num, tip_rev = clogger.get_tip(t)
        t.execute(
            "DELETE FROM csetLog WHERE revnum >= " + str(tip_num) + " - 5"
        )
        _, new_tip_rev = clogger.get_tip(t)

    assert tip_rev and new_tip_rev

    # Test out of order revision requests here also
    revnums = clogger.get_revnnums_from_range(new_tip_rev, tip_rev)

    assert len(revnums) == 7

    curr_revnum = -1
    for revnum, revision in revnums:
        assert revision
        assert revnum > curr_revnum
        curr_revnum = revnum


def test_get_revnum_range_tipnback(clogger):
    clogger.disable_tipfilling = True
    clogger.disable_backfilling = True
    clogger.disable_maintenance = True
    clogger.disable_deletion = True

    for ordering in range(2):
        # Used for testing output
        prev_total_revs = clogger.conn.get_one("SELECT count(revnum) FROM csetLog")[0]
        expected_total_revs = prev_total_revs + 10

        # Get the current tail, go 10 changesets back and request
        # the final one as the second revision.
        with clogger.conn.transaction() as t:
            rev1, oldest_rev = clogger.get_tail(t)

        # Get the current tip, delete it, then request it's revnum range
        # to a non-existent (backfill required) revision in the past.
        with clogger.conn.transaction() as t:
            tip_num, tip_rev = clogger.get_tip(t)
            t.execute(
                "DELETE FROM csetLog WHERE revnum >= " + str(tip_num) + " - 5"
            )
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

        curr_revnum = -1
        for revnum, revision in revnums:
            assert revision
            assert revnum > curr_revnum
            curr_revnum = revnum
