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
from pyLibrary.sql import sql_list, sql_iso, quote_set
from pyLibrary.sql.sqlite import quote_value, DOUBLE_TRANSACTION_ERROR
from tuid.service import TUIDService
from tuid.util import map_to_array

_service = None
GC_DEBUG = False

@pytest.fixture
def service(config, new_db):
    global _service
    if new_db == 'yes':
        return TUIDService(database=Null, start_workers=False, kwargs=config.tuid)
    elif new_db == 'no':
        if _service is None:
            _service = TUIDService(kwargs=config.tuid, start_workers=False)
        return _service
    else:
        Log.error("expecting 'yes' or 'no'")

@pytest.mark.skip("Used for local memory use testing.")
def test_annotation_memory(service):
    import psutil
    import os
    import gc, pprint

    gc.set_debug(gc.DEBUG_SAVEALL)

    with open('resources/stressfiles.json', 'r') as f:
        files = json.load(f)

    total_trials = 1000
    total_files = 1
    files_to_get = files[:total_files]
    test_rev = "58eb13b394f4"

    all_end_mems = [None] * total_trials
    all_percents = [None] * total_trials
    process = psutil.Process(os.getpid())
    start_mem = -1
    for i in range(total_trials):

        # Randomize files
        #files_to_get = [random.choice(files) for _ in range(total_files)]

        with service.conn.transaction() as t:
            filter = {"terms": {"file": files_to_get}}
            service.temporal.delete_record(filter)
            service.temporal.refresh()
            query = {"query": {"terms": {"revision": files_to_get}}}
            result = service.temporal.search(query)
            while len(result.hits.hits) != 0:
                Till(seconds=0.001).wait()
                result = service.temporal.search(query)
            service.temporal.delete_record(filter)
            t.execute("DELETE FROM annotations WHERE file IN " + quote_set(files_to_get))
            t.execute("DELETE FROM latestFileMod WHERE file IN " + quote_set(files_to_get))

        if start_mem == -1:
            start_mem = round(process.memory_info().rss / (1000 * 1000), 2)
        service.get_tuids(files_to_get, test_rev)
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

        if GC_DEBUG:
            Log.note("Uncollected garbage: ")
            pprint.pprint(gc.garbage)

            import time
            time.sleep(10)

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