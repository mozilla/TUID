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
from tuid.service import Clogger
from tuid.service import MAXIMUM_NONPERMANENT_CSETS
from tuid import sql

_clogger = None
_conn = None

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
    assert clogger is not None


def test_tipfilling(clogger):
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
            with clogger.conn.transaction() as t:
                new_tip = t.get_one("SELECT max(revnum) AS revnum, revision FROM csetLog")[1]
            if new_tip:
                nothing_exists = False
            else:
                (Till(seconds=wait_time)).wait()
        if current_tip == new_tip:
            break
        num_trys -= 1

    assert num_trys > 0
    assert current_tip == new_tip


def test_backfilling_to_revision(clogger):
    num_trys = 50
    wait_time = 2
    num_to_go_back = 10

    oldest_rev = None
    oldest_revnum = 1
    with clogger.conn.transaction() as t:
        oldest_revnum, oldest_rev = t.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")

    new_old_rev = None
    clog_url = HG_URL + clogger.config.hg.branch + '/' + 'json-log/' + oldest_rev
    clog_obj_list = list(clogger._get_clog(clog_url)['changesets'])
    for count, clog_obj in enumerate(clog_obj_list[1:]):
        if count + 1 >= num_to_go_back:
            new_old_rev = clog_obj['node'][:12]
            break

    clogger.csets_todo_backwards.append((new_old_rev, True))

    new_ending = None
    while num_trys > 0:
        with clogger.conn.transaction() as t:
            new_ending = t.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")[1]
        print(oldest_rev)
        print(new_old_rev)
        print(new_ending)
        if new_ending == new_old_rev:
            break
        else:
            (Till(seconds=wait_time)).wait()
            num_trys -= 1

    assert num_trys > 0
    assert new_old_rev == new_ending

    # Check that revnum's were properly handled
    expected_revnum = oldest_revnum + num_to_go_back
    with clogger.conn.transaction() as t:
        new_revnum = t.get_one("SELECT revnum FROM csetLog WHERE revision=?", (oldest_rev,))[0]
    assert expected_revnum == new_revnum


def test_backfilling_by_count(clogger):
    num_trys = 50
    wait_time = 2
    num_to_go_back = 10

    oldest_rev = None
    oldest_revnum = 1
    with clogger.conn.transaction() as t:
        oldest_revnum, oldest_rev = t.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")

    new_old_rev = None
    clog_url = HG_URL + clogger.config.hg.branch + '/' + 'json-log/' + oldest_rev
    clog_obj_list = list(clogger._get_clog(clog_url)['changesets'])
    for count, clog_obj in enumerate(clog_obj_list[1:]):
        if count >= num_to_go_back - 1:
            new_old_rev = clog_obj['node'][:12]
            break

    clogger.csets_todo_backwards.append((num_to_go_back, True))

    new_ending = None
    while num_trys > 0:
        with clogger.conn.transaction() as t:
            new_ending = t.get_one("SELECT min(revnum) AS revnum, revision FROM csetLog")[1]
        print(oldest_rev)
        print(new_old_rev)
        print(new_ending)
        if new_ending == new_old_rev:
            break
        else:
            (Till(seconds=wait_time)).wait()
            num_trys -= 1

    assert num_trys > 0
    assert new_old_rev == new_ending

    # Check that revnum's were properly handled
    expected_revnum = oldest_revnum + num_to_go_back
    with clogger.conn.transaction() as t:
        new_revnum = t.get_one("SELECT revnum FROM csetLog WHERE revision=?", (oldest_rev,))[0]
    assert expected_revnum == new_revnum


def test_maintenance_and_deletion(clogger):
    # IMPORTANT: Assumes that the max csets is 100
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
        tail_cset = clogger.get_tail(t)[1]

    clogger.csets_todo_backwards.append((extra_to_add, True))
    new_tail = None
    tmp_num_trys = 0
    while tmp_num_trys < num_trys:
        (Till(seconds=wait_time)).wait()
        with clogger.conn.transaction() as t:
            new_tail = clogger.get_tail(t)[1]
        if new_tail != tail_cset:
            break
        tmp_num_trys += 1
    assert tmp_num_trys < num_trys

    inserts_list = [
        ('file1', new_tail),
        ('file2', new_tail)
    ]
    with clogger.conn.transaction() as t:
        t.execute(
            "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
            sql_list(
                sql_iso(sql_list(map(quote_value, i)))
                for i in inserts_list
            )
        )
        revnums_in_db = t.get_one("SELECT count(revnum) as revnum FROM csetLog")[0]
    if revnums_in_db <= max_revs:
        Log.note("Maintenance worker already ran.")
        assert True
        return

    wait_time = 10
    tmp_num_trys = 0
    while tmp_num_trys < num_trys:
        (Till(seconds=wait_time)).wait()
        with clogger.conn.transaction() as t:
            revnums_in_db = t.get_one("SELECT count(revnum) as revnum FROM csetLog")[0]
        if revnums_in_db <= max_revs:
            break
        tmp_num_trys += 1
    assert tmp_num_trys < num_trys


def test_partial_tipfilling(clogger):
    num_trys = 50
    wait_time = 2
    with clogger.conn.transaction() as t:
        max_tip_num = clogger.get_tail(t)
        total_revs = t.get_one("SELECT count(revnum) FROM csetLog")

    #if total_revs > max_tip_num:
    assert True