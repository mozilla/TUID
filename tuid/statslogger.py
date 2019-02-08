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
from mo_threads.threads import ALL
from mo_times.durations import MINUTE

import gc
import os
import psutil

DAEMON_WAIT_FOR_PC = 1 * MINUTE # Time until a percent complete log message is emitted.
DAEMON_WAIT_FOR_THREADS = 1 * MINUTE # Time until a thread count log message is emitted.
DAEMON_MEMORY_LOG_INTERVAL = 2 * MINUTE # Time until the memory is logged.
DAEMON_REQUESTS_LOG_INTERVAL = 2 * MINUTE # Time until requests data is logged.

class StatsLogger:

    def __init__(self):
        self.out_of_memory_restart = False

        self.total_locker = Lock()
        self.total_files_requested = 0
        self.total_tuids_mapped = 0

        self.threads_locker = Lock()
        self.waiting = 0
        self.threads_waiting = 0

        self.requests_locker = Lock()
        self.requests_total = 0
        self.requests_complete = 0
        self.requests_incomplete = 0
        self.requests_passed = 0
        self.requests_failed = 0

        self.prev_mem = 0
        self.curr_mem = 0
        self.initial_growth = {}

        Thread.run("pc-daemon", self.run_pc_daemon)
        Thread.run("threads-daemon", self.run_threads_daemon)
        Thread.run("memory-daemon", self.run_memory_daemon)
        Thread.run("requests-daemon", self.run_requests_daemon)


    def get_percent_complete(self):
        with self.total_locker:
            if self.total_files_requested > 0:
                pc = round(self.total_tuids_mapped / self.total_files_requested, 4)
            else:
                pc = 1.0
        return 100 * pc


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


    def set_process(self, pid):
        self.processtolog = psutil.Process(os.getpid())


    def get_process_memory(self):
        return round(self.processtolog.memory_info().rss / (1000 * 1000), 2)


    def print_proc_memory_used(self, loc):
        self.prev_mem = self.curr_mem
        self.curr_mem = self.get_process_memory()
        Log.note(
            "Used memory since last call: {{mem}}",
            loc=loc,
            mem=self.curr_mem - self.prev_mem
        )


    def get_free_memory(self):
        tmp = psutil.virtual_memory()
        return tmp.available >> 20


    def get_used_memory(self):
        tmp = psutil.virtual_memory()
        return tmp.used >> 20


    def get_used_memory_percent(self):
        tmp = psutil.virtual_memory()
        return tmp.percent


    def run_memory_daemon(self, please_stop):
        while not please_stop:
            try:
                (Till(seconds=DAEMON_MEMORY_LOG_INTERVAL.seconds) | please_stop).wait()
                mem = psutil.virtual_memory()
                Log.note(
                    "TUID Process - complete memory info: {{mem}}",
                    mem=str(mem)
                )
                Log.note("\nOpen threads ({{num}}):", num=len(ALL))
                Log.note(
                    "{{data}}",
                    data=str({
                        i: ALL[i].name
                        for i in ALL
                    })
                )
            except Exception as e:
                Log.warning("Error encountered while trying to log memory: {{cause}}", cause=e)


    def update_requests(
            self,
            requests_total=0,
            requests_incomplete=0,
            requests_complete=0,
            requests_failed=0,
            requests_passed=0
        ):
        '''
        Updates and returns the current totals.
        :param requests_total:
        :param requests_incomplete: Service required more than 30 seconds, or is busy
        :param requests_complete: Service and app successfully fulfilled the request fully
                                  (including handling errors successfully)
        :param requests_failed: Count for unexpected behaviour
        :param requests_passed: Count for expected behaviour
        :return:
        '''
        with self.requests_locker:
            self.requests_total += requests_total
            self.requests_incomplete += requests_incomplete
            self.requests_complete += requests_complete
            self.requests_failed += requests_failed
            self.requests_passed += requests_passed


    def get_requests(self):
        return {
            'total': self.requests_total,
            'incomplete': self.requests_incomplete,
            'complete': self.requests_complete,
            'failed': self.requests_failed,
            'passed': self.requests_passed,
        }


    def run_requests_daemon(self, please_stop):
        while not please_stop:
            try:
                (Till(seconds=DAEMON_REQUESTS_LOG_INTERVAL.seconds) | please_stop).wait()
                request_stats = self.get_requests()
                if request_stats['incomplete'] == 0:
                    pc_complete = request_stats['complete']
                else:
                    pc_complete = request_stats['complete'] / request_stats['incomplete']
                Log.note(
                    "\nRequest stats \n"
                    "----------------\n"
                    "Requests ratio (complete/incomplete): {{comp}}/{{incomp}} = {{pc_comp}}\n"
                    "Passed: {{passed}}\n"
                    "Failed: {{failed}}\n",
                    comp=request_stats['complete'],
                    incomp=request_stats['incomplete'],
                    pc_comp=pc_complete,
                    passed=request_stats['passed'],
                    failed=request_stats['failed']
                )
            except Exception as e:
                Log.warning("Error encountered while trying to log requests: {{cause}}", cause=e)