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
import os
import pytest

from mo_dots import Null
from mo_logs import Log, startup, constants
from mo_threads import Thread, Till
from mo_times import Timer
from pyLibrary.env import http
from pyLibrary.sql import sql_list, sql_iso, quote_set
from pyLibrary.sql.sqlite import quote_value, DOUBLE_TRANSACTION_ERROR
from tuid.service import TUIDService
from tuid.util import map_to_array

service = None
GC_DEBUG = True

def test_annotation_memory():
    import psutil
    import os
    import time
    import objgraph.objgraph as objgraph
    import random
    import gc, pprint

    #gc.set_debug(gc.DEBUG_SAVEALL)

    with open('resources/stressfiles.json', 'r') as f:
        files = json.load(f)

    total_trials = 500
    total_files = 1
    files_to_get = files[:total_files]
    test_rev = "58eb13b394f4"

    all_end_mems = [None] * total_trials
    all_percents = [None] * total_trials
    process = psutil.Process(os.getpid())
    start_mem = -1
    initial_growth = {}
    for count_tmp, i in enumerate(range(total_trials)):

        # Randomize files
        #files_to_get = [random.choice(files) for _ in range(total_files)]

        with service.conn.transaction() as t:
            t.execute("DELETE FROM temporal WHERE file IN " + quote_set(files_to_get))
            t.execute("DELETE FROM annotations WHERE file IN " + quote_set(files_to_get))
            t.execute("DELETE FROM latestFileMod WHERE file IN " + quote_set(files_to_get))
        del t

        if start_mem == -1:
            start_mem = round(process.memory_info().rss / (1000 * 1000), 2)
        Log.note("\nBefore:")
        initial_growth = {}
        objgraph.growth(peak_stats={})
        objgraph.growth(peak_stats=initial_growth)
        #objgraph.show_growth(peak_stats=initial_growth)
        service.initial_growth = initial_growth
        res = service.get_tuids_from_files(files_to_get, test_rev)
        Log.note("\nAfter:")
        objgraph.show_growth(peak_stats=initial_growth)

        tmp = objgraph.by_type('Transaction')
        obj = objgraph.by_type('SQL')[-1]
        objgraph._find_dominator_graph(obj, max_depth=4)
        objgraph.show_refs(obj, filename=str(int(time.time())) + '_forwardrefs.dot')
        objgraph.show_backrefs(objgraph.by_type('SQL')[-1], filename=str(int(time.time())) + '_backwwardrefs.dot')
        objgraph.show_refs(objgraph.by_type('tuple')[-10], filename=str(int(time.time())) + '_allrefs_dict.dot')
        objgraph.show_refs([res], filename=str(int(time.time())) + '_res_var.dot')
        gc.collect()

        end_mem = round(process.memory_info().rss / (1000 * 1000), 2)
        pc_used = service.statsdaemon.get_used_memory_percent()

        Log.note("GC get_count: {{getc}}", getc=gc.get_count())
        Log.note("GC collect: {{getc}}", getc=gc.collect())

        Log.note(
            "Started with {{mem}}, finished with {{endmem}}. Percent currently used is {{pc}}",
            mem=start_mem,
            endmem=end_mem,
            pc=pc_used
        )
        Log.note("Used {{mem}} Mb since first get_tuids call.", mem=str(end_mem - start_mem))

        all_end_mems[i] = end_mem
        all_percents[i] = pc_used

    from matplotlib import pyplot as plt

    plt.figure()
    plt.plot(all_end_mems)
    plt.title("Memory usage over time.")
    plt.xlabel("Trial count")
    plt.ylabel("Memory usage (Mb)")

    plt.figure()
    plt.plot(all_percents)
    plt.title("Percent of memory used over time.")
    plt.xlabel("Trial count")
    plt.ylabel("Memory usage (%)")

    plt.show(block=True)

if __name__=="__main__":
    try:
        config = startup.read_settings(
            filename=os.environ.get('TUID_CONFIG')
        )
        constants.set(config.constants)
        Log.start(config.debug)

        service = TUIDService(config.tuid)
        Log.note("Started TUID Service")
    except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
        try:
            Log.error("Serious problem with TUID service construction!  Shutdown!", cause=e)
        finally:
            Log.stop()
    test_annotation_memory()