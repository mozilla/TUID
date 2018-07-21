# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_logs import Log
from mo_threads import Till, Lock, Thread
from mo_times.durations import MINUTE

import linecache
import os
import tracemalloc
import memory_profiler
from datetime import datetime

DAEMON_WAIT_FOR_PC = 1 * MINUTE # Time until a percent complete log message is emitted.
DAEMON_WAIT_FOR_THREADS = 1 * MINUTE # Time until a percent complete log message is emitted.
DAEMON_MEMORY_LOG_INTERVAL = 10 * MINUTE # Time until the memory is logged.

class StatsLogger:

    def __init__(self):
        self.total_locker = Lock()
        self.total_files_requested = 0
        self.total_tuids_mapped = 0

        self.threads_locker = Lock()
        self.waiting = 0
        self.threads_waiting = 0

        Thread.run("pc-daemon", self.run_pc_daemon)
        Thread.run("threads-daemon", self.run_threads_daemon)
        Thread.run("memory-daemon", self.run_memory_daemon)


    def update_totals(self, num_files_req, num_tuids_mapped):
        with self.total_locker:
            self.total_files_requested += num_files_req
            self.total_tuids_mapped += num_tuids_mapped


    def reset_totals(self):
        with self.total_locker:
            self.total_files_requested = 0
            self.total_tuids_mapped = 0


    def run_pc_daemon(self, please_stop=None):
        while not please_stop:
            try:
                with self.total_locker:
                    requested = self.total_files_requested
                    if requested != 0:
                        mapped = self.total_tuids_mapped
                        Log.note(
                            "Percent complete {{mapped}}/{{requested}} = {{percent|percent(0)}}",
                            requested=requested,
                            mapped=mapped,
                            percent=mapped/requested
                        )
                (Till(seconds=DAEMON_WAIT_FOR_PC.seconds) | please_stop).wait()
            except Exception as e:
                Log.warning("Unexpected error in pc-daemon: {{cause}}", cause=e)


    def update_threads_waiting(self, val):
        with self.threads_locker:
            self.threads_waiting += val


    def update_anns_waiting(self, val):
        with self.threads_locker:
            self.waiting += val


    def run_threads_daemon(self, please_stop=None):
        while not please_stop:
            try:
                with self.threads_locker:
                    Log.note(
                        "Currently {{waiting}} waiting to get annotation, and {{threads}} waiting to be created.",
                        waiting=self.waiting,
                        threads=self.threads_waiting
                    )
                (Till(seconds=DAEMON_WAIT_FOR_THREADS.seconds) | please_stop).wait()
            except Exception as e:
                Log.warning("Unexpected error in pc-daemon: {{cause}}", cause=e)


    def display_top(self, snapshot, key_type='lineno', limit=20):
        snapshot = snapshot.filter_traces((
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        ))
        top_stats = snapshot.statistics(key_type)

        Log.note("Top {{num}} lines", num=limit)
        for index, stat in enumerate(top_stats[:limit], 1):
            frame = stat.traceback[0]
            # replace "/path/to/module/file.py" with "module/file.py"
            filename = os.sep.join(frame.filename.split(os.sep)[-2:])
            Log.note(
                "#{{num}}: {{fname}}:{{lineno}}: {{size}} KiB",
                num=index,
                fname=filename,
                lineno=frame.lineno,
                size=round(stat.size / 1024, 1)
            )

            line = linecache.getline(frame.filename, frame.lineno).strip()
            if line:
                Log.note("    {{line}}", line=line)

        other = top_stats[limit:]
        if other:
            size = sum(stat.size for stat in other)
            Log.note("{{others}} other: {{size}} KiB", others=len(other), size=round(size / 1024, 1))
        total = sum(stat.size for stat in top_stats)
        Log.note("Total allocated size: {{size}} KiB", size=round(total / 1024, 1))


    def run_memory_daemon(self, please_stop):
        tracemalloc.start()
        old_max = 0
        snapshot = None

        while not please_stop:
            try:
                (Till(seconds=DAEMON_MEMORY_LOG_INTERVAL.seconds) | please_stop).wait()
                max_rss = max(memory_profiler.memory_usage())
                if max_rss > old_max:
                    old_max = max_rss
                    snapshot = tracemalloc.take_snapshot()
                    Log.note(
                        "{{currtime}} max-RSS {{maxrss}}",
                        currtime=datetime.now(),
                        maxrss=max_rss
                    )

                if snapshot is not None:
                    Log.note("Displaying snapshot...")
                    self.display_top(snapshot)
            except Exception as e:
                Log.warning("Error encountered while trying to log memory: {{cause}}", cause=e)
