# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import shutil

from mo_dots import wrap
from mo_logs import startup, constants, Log
from mo_times import Timer, Date
from mo_threads import Process, Till

from mo_future import text_type
from pyLibrary.env import http

from tuid import sql
from tuid.client import TuidClient
from tuid.service import TUIDService, HG_URL

RETRY = {"times": 3, "sleep": 5}

# Run this test while running app.py, and
# sqs_consumer.py. A temporary database after
# initialization is stored in case any bugs are
# encountered during testing.

try:
    config = startup.read_settings()
    constants.set(config.constants)
    client = TuidClient(config.client)

    # This test requests TUIDs at a rate of request_rate.
    # Overtime the load increases until we reach a breaking
    # point.
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    queue_length_at_rate = []
    req_rate = 0.2  # Rate at reqs/sec
    step = 0.01  # Secs to increase per end
    init_req_pause = 1 / req_rate  # Time to wait between requests.
    tries_after_wait = 5  # Number of data points to get after seeing a non-empty queue
    rev_count = 100  # Number of revisions to request tuids for

    # Get the queue for monitoring it's length
    config = startup.read_settings()
    constants.set(config.constants)
    Log.start(config.debug)

    # Get the client to push to the queue
    queue = client.push_queue

    # Get the service to delete entries
    service = TUIDService(conn=sql.Sql("resources/tuid_app.db"), kwargs=config.tuid)

    # Get a list of 1000 files from stressfiles
    with open("resources/stressfiles.json", "r") as f:
        files = json.load(f)

    # Get rev_count revisions from changelogs
    csets = []
    final_rev = ""
    while len(csets) < rev_count:
        # Get a changelog
        clog_url = HG_URL / "mozilla-central" / "json-log" / final_rev
        try:
            Log.note("Searching through changelog {{url}}", url=clog_url)
            clog_obj = http.get_json(clog_url, retry=RETRY)
        except Exception as e:
            Log.error(
                "Unexpected error getting changset-log for {{url}}",
                url=clog_url,
                error=e,
            )

        cset = ""
        for clog_cset in clog_obj["changesets"]:
            cset = clog_cset["node"][:12]
            if len(csets) < rev_count:
                csets.append(cset)

        final_rev = cset

    # Oldest is now first
    csets.reverse()

    # Make the initial insertion (always slow)
    with Timer("Make initialization request for TUID", {"timestamp": Date.now()}):
        resp = service.get_tuids_from_files(files, csets[0])

    # Backup the database now
    shutil.copy("resources/tuid_app.db", "resources/tuid_app_tmp.db")

    # While we haven't hit the breaking point perform
    # the stress test
    while tries_after_wait >= 0:
        # Restore the database to before any testing
        # shutil.copy('resources/test_tmp.db', 'resources/test.db')

        for rev in csets[1:]:
            # Request each revision in order
            request = wrap(
                {
                    "from": "files",
                    "where": {
                        "and": [{"eq": {"revision": rev}}, {"in": {"path": files}}]
                    },
                    "meta": {"format": "list", "request_time": Date.now()},
                }
            )
            if client.push_queue is not None:
                Log.note(
                    "record tuid request to SQS: {{timestamp}}",
                    timestamp=request.meta.request_time,
                )
                client.push_queue.add(request)

            # Wait before sending the next request
            (Till(seconds=init_req_pause + 10)).wait()

        check_state = {"rate": req_rate, "qlength": len(queue)}
        queue_length_at_rate.append(check_state)
        req_rate += step
        init_req_pause = 1 / req_rate
        # Check and save length after all the requests
        if check_state["qlength"] > 1:
            tries_after_wait -= 1
            # Wait while there are still some requests left
            while len(queue) > 0:
                Log.note(
                    "Waiting 10 seconds...queue has items left to process: {{qlength}}",
                    qlength=len(queue),
                )
                (Till(seconds=10)).wait()

    Log.note(
        "Checked states: {{rates}}",
        rates=[(r["rate"], r["qlength"]) for r in queue_length_at_rate],
    )

except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
    try:
        Log.error("Problem with example client!", cause=e)
    finally:
        Log.stop()
