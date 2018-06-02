# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_dots import Data
from mo_threads import Signal, Thread
from pyLibrary.sql import sqlite, sql_iso
from pyLibrary.sql.sqlite import Sqlite, quote_value

# NONE OF THESE TESTS ARE GOOD, BUT THE CODE MY BE USEFUL FOR MAKING GOOD TESTS

# sqlite.DEBUG = True
#
# def test_interleaved_transactions():
#     db, threads, signals = _setup()
#     a, b = signals.a, signals.b
#
#     # INTERLEAVED TRANSACTION STEPS
#     for i in range(4):
#         _perform(a, i)
#         _perform(b, i)
#
#     _teardown(db, threads)
#
#
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
# def _work(name, db, sigs, please_stop):
#     try:
#         sigs[0].begin.wait()
#         with db.transaction() as t:
#             sigs[0].done.go()
#             sigs[1].begin.wait()
#             t.execute("INSERT INTO my_table VALUES "+sql_iso(quote_value(name)))
#             sigs[1].done.go()
#
#             sigs[2].begin.wait()
#             result = t.query("SELECT * FROM my_table")
#             assert len(result.data) == 1
#             assert result.data[0][0] == name
#             sigs[2].done.go()
#
#             sigs[3].begin.wait()
#         sigs[3].done.go()
#     finally:
#         # RELEASE ALL SIGNALS, THIS IS ENDING BADLY
#         for s in sigs:
#             s.done.go()
#
#
# def _setup():
#     threads = Data()
#     signals = Data()
#
#     db = Sqlite()
#     db.query("CREATE TABLE my_table (value TEXT)")
#
#     for name in ["a", "b"]:
#         signals[name] = [{"begin": Signal(), "done": Signal()} for _ in range(4)]
#         threads[name] = Thread.run(name, _work, name, db, signals[name])
#
#     return db, threads, signals
#
#
# def _teardown(db, threads):
#     threads.a.join()
#     threads.b.join()
#
#     result = db.query("SELECT * FROM my_table ORDER BY value")
#     assert len(result.data) == 2
#     assert result.data[0][0] == 'a'
#     assert result.data[1][0] == 'b'
#
#
# def _perform(c, i):
#     c[i].begin.go()
#     c[i].done.wait()
#
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


