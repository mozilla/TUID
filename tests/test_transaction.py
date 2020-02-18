# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_sql import sql_iso

from jx_sqlite import sqlite
from mo_dots import Data
from mo_threads import Signal, Thread
from jx_sqlite.sqlite import Sqlite, quote_value, DOUBLE_TRANSACTION_ERROR

sqlite.DEBUG = True


def test_interleaved_transactions():
    db, threads, signals = _setup()
    a, b = signals.a, signals.b

    # INTERLEAVED TRANSACTION STEPS
    for i in range(3):
        _perform(a, i)
        _perform(b, i)

    _teardown(db, threads)


def test_transactionqueries():
    db = Sqlite()
    db.query("CREATE TABLE my_table (value TEXT)")

    with db.transaction() as t:
        t.execute("INSERT INTO my_table (value) VALUES ('a')")
        try:
            result1 = db.query("SELECT * FROM my_table")
            assert False
        except Exception as e:
            assert DOUBLE_TRANSACTION_ERROR in e
        result2 = t.query("SELECT * FROM my_table")

    assert result2.data[0][0] == "a"


def test_two_commands():
    db, threads, signals = _setup()
    a, b = signals.a, signals.b

    _perform(a, 0)
    _perform(a, 1)
    _perform(b, 0)
    _perform(b, 1)
    _perform(a, 2)
    _perform(b, 2)

    _teardown(db, threads)


def test_nested_transaction1():
    db = Sqlite()
    db.query("CREATE TABLE my_table (value TEXT)")

    with db.transaction() as t:
        t.execute("INSERT INTO my_table VALUES ('a')")

        result = t.query("SELECT * FROM my_table")
        assert len(result.data) == 1
        assert result.data[0][0] == "a"

        with db.transaction() as t2:
            t2.execute("INSERT INTO my_table VALUES ('b')")

    _teardown(db, {})


def test_nested_transaction2():
    db = Sqlite()
    db.query("CREATE TABLE my_table (value TEXT)")

    with db.transaction() as t:
        with db.transaction() as t2:
            t2.execute("INSERT INTO my_table VALUES ('b')")

            result = t2.query("SELECT * FROM my_table")
            assert len(result.data) == 1
            assert result.data[0][0] == "b"

        t.execute("INSERT INTO my_table VALUES ('a')")

    _teardown(db, {})


# # def test_all_combinations():
# #     # ALL 8bit NUMBERS WITH 4 ONES (AND 4 ZEROS), NOT INCLUDING PALINDROMES
# #     for sequence in SEQUENCE_COMBOS:
# #         db, threads, signals = _setup()
# #         sigs = {'0': signals.a, '1': signals.b}
# #         counter = {'0': 0, '1': 0}
# #         for s in sequence:
# #             _perform(sigs[s], counter[s])
# #             counter[s] += 1
# #
# #         _teardown(db, threads)
#
#
def _work(name, db, sigs, please_stop):
    try:
        sigs[0].begin.wait()
        with db.transaction() as t:
            sigs[0].done.go()
            sigs[1].begin.wait()
            t.execute("INSERT INTO my_table VALUES " + sql_iso(quote_value(name)))
            sigs[1].done.go()

            sigs[2].begin.wait()
            result = t.query("SELECT * FROM my_table WHERE value=" + quote_value(name))
            assert len(result.data) == 1
            assert result.data[0][0] == name
        sigs[2].done.go()
    finally:
        # RELEASE ALL SIGNALS, THIS IS ENDING BADLY
        for s in sigs:
            s.done.go()


def _setup():
    threads = Data()
    signals = Data()

    db = Sqlite()
    db.query("CREATE TABLE my_table (value TEXT)")

    for name in ["a", "b"]:
        signals[name] = [{"begin": Signal(), "done": Signal()} for _ in range(4)]
        threads[name] = Thread.run(name, _work, name, db, signals[name])

    return db, threads, signals


def _teardown(db, threads):
    for t in threads.values():
        t.join()
        t.join()

    result = db.query("SELECT * FROM my_table ORDER BY value")
    assert len(result.data) == 2
    assert result.data[0][0] == "a"
    assert result.data[1][0] == "b"


def _perform(c, i):
    c[i].begin.go()
    c[i].done.wait()


#
# SEQUENCE_COMBOS = [
#     # ALL 8bit NUMBERS WITH 4 ONES (AND 4 ZEROS), NOT INCLUDING BINARY NOT OF THE SAME
#     "00001111", "00010111", "00011011", "00011101", "00011110", "00100111",
#     "00101011", "00101101", "00101110", "00110011", "00110101", "00110110",
#     "00111001", "00111010", "00111100", "01000111", "01001011", "01001101",
#     "01001110", "01010011", "01010101", "01010110", "01011001", "01011010",
#     "01011100", "01100011", "01100101", "01100110", "01101001", "01101010",
#     "01101100", "01110001", "01110010", "01110100", "01111000"
# ]
