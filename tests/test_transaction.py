# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

import pytest

from mo_dots import Data
from mo_threads import Signal, Thread
from pyLibrary.sql.sqlite import Sqlite


def test_two_transactions():
    threads = Data()
    signals = Data()

    def work(name, db, please_stop):
        sigs = signals[name]
        try:
            sigs.please_insert.wait()
            with db.transaction() as t:
                t.execute("INSERT INTO my_table VALUES (?)", (name,))
                sigs.inserted.go()
                sigs.please_verify.wait()
                result = t.query("SELECT * FROM my_table")
                assert len(result.data) == 1
                assert result.data[0][0] == name
                sigs.verified.go()
                sigs.please_complete.wait()
        finally:
            # RELEASE ALL SIGNALS, THIS IS ENDING BADLY
            sigs.inserted.go()
            sigs.verified.go()

    db = Sqlite()
    db.execute("CREATE TABLE my_table AS (value TEXT)")

    for name in ["a", "b"]:
        signals[name] = {
            "please_insert": Signal(),
            "inserted": Signal(),
            "please_verify": Signal(),
            "verified": Signal(),
            "please_complete": Signal()
        }
        threads[name] = Thread.run(name, work, name)

    a, b = signals.a, signals.b

    a.please_insert.go()
    a.inserted.wait()
    b.please_insert.go()
    b.inserted.wait()
    a.please_verify.go()
    a.verified.wait()
    b.please_verify.go()
    b.verified.wait()
    # AT THIS POINT WE HAVE VERIFIED TWO INDEPENDENT TRANSACTIONS ON A SINGLE DB
    a.please_complete.go()
    b.please_complete.go()

    threads.a.join()
    threads.b.join()

    result = db.query("SELECT * FROM my_table ORDER BY value")
    assert len(result.data) == 2
    assert result.data[0][0] == 'a'
    assert result.data[1][0] == 'b'
