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
import psutil
from datetime import datetime

DAEMON_WAIT_FOR_PC = 1 * MINUTE # Time until a percent complete log message is emitted.
DAEMON_WAIT_FOR_THREADS = 1 * MINUTE # Time until a thread count log message is emitted.
DAEMON_MEMORY_LOG_INTERVAL = 2 * MINUTE # Time until the memory is logged.

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


    def get_free_memory(self):
        tmp = psutil.virtual_memory()
        return tmp.free


    def run_memory_daemon(self, please_stop):
        while not please_stop:
            try:
                (Till(seconds=DAEMON_MEMORY_LOG_INTERVAL.seconds) | please_stop).wait()
                mem = psutil.virtual_memory()
                Log.note(
                    "TUID Process - complete memory info: {{mem}}",
                    mem=str(mem)
                )
            except Exception as e:
                Log.warning("Error encountered while trying to log memory: {{cause}}", cause=e)
