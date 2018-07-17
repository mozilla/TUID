# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from mo_logs import Log
from mo_threads import Till, Lock, Thread
from mo_times.durations import MINUTE

DAEMON_WAIT_FOR_PC = 1 * MINUTE # Time until a percent complete log message is emitted.

class PercentCompleteLogger:

    def __init__(self):
        self.total_locker = Lock()
        self.total_files_requested = 0
        self.total_tuids_mapped = 0
        Thread.run("pc-daemon", self.run_daemon)


    def update_totals(self, num_files_req, num_tuids_mapped):
        with self.total_locker:
            self.total_files_requested += num_files_req
            self.total_tuids_mapped += num_tuids_mapped


    def reset_totals(self):
        with self.total_locker:
            self.total_files_requested = 0
            self.total_tuids_mapped = 0


    def run_daemon(self, please_stop=None):
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
