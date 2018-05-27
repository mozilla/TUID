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

from mo_future import text_type
from mo_kwargs import override
from mo_logs import startup, constants, Log
from mo_times import Timer

from mo_hg import hg_mozilla_org
from mo_threads import Thread, Signal, Till
from pyLibrary import aws
from pyLibrary.env import http
from tuid import service
from tuid.client import TuidClient

# REQUIRED TO PREVENT constants FROM COMPLAINING
_ = service
_ = hg_mozilla_org

PAUSE_ON_FAILURE = 30
DEBUG = True
RETRY = {"times": 3, "sleep": 5}

@override
def queue_consumer(client, pull_queue, please_stop=None, kwargs=None):
    queue = aws.Queue(pull_queue)
    client = TuidClient(client)
    try_revs = {}

    #while len(queue) > 0:
    #    request = queue.pop(till=please_stop)
    #    if request:
    #        Log.note("Popping request from {{time}}", time=request.meta.request_time)
    #        queue.commit()

    while not please_stop:
        request = queue.pop(till=please_stop)
        if please_stop:
            break
        if not request:
            Log.note("Nothing in queue, pausing for 5 seconds...")
            (please_stop | Till(seconds=5)).wait()
            continue
        Log.note("Found something in queue")

        and_op = request.where['and']

        revision = None
        files = None
        for a in and_op:
            if a.eq.revision:
                revision = a.eq.revision
            elif a['in'].path:
                files = a['in'].path
            elif a.eq.path:
                files = [a.eq.path]

        if len(files) <= 0:
            Log.warning("No files in the given request: {{request}}", request=request)
            continue

        if revision[:12] in try_revs:
            Log.warning(
                "Revision {{cset}} does not exist in the {{branch}} branch",
                cset=revision[:12], branch='mozilla-central'
            )
            queue.commit()
            continue

        clog_url = 'https://hg.mozilla.org/mozilla-central/json-log/' + revision[:12]
        clog_obj = http.get_json(clog_url, retry=RETRY)
        if isinstance(clog_obj, (text_type, str)):
            Log.warning(
                "Revision {{cset}} does not exist in the {{branch}} branch",
                cset=revision[:12], branch='mozilla-central'
            )
            try_revs[revision[:12]] = True
            queue.commit()
            continue
        else:
            Log.note("Revision {{cset}} exists on mozilla-central.", cset=revision[:12])

        with Timer("Make TUID request from {{timestamp|date}}", {"timestamp": request.meta.request_time}):
            client.enabled = True  # ENSURE THE REQUEST IS MADE
            result = http.post_json(
                        "http://localhost:5000/tuid",
                        json=request,
                        timeout=10000
                    )
            if not client.enabled:
                Log.note("pausing consumer for {{num}}sec", num=PAUSE_ON_FAILURE)
                Till(seconds=PAUSE_ON_FAILURE).wait()
            if result is None or len(result.data) != len(files):
                Log.warning("expecting response for every file requested")

        queue.commit()

if __name__ == '__main__':
    try:
        tmp_signal = Signal()
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)

        queue_consumer(kwargs=config, please_stop=tmp_signal)
        worker = Thread.run("sqs consumer", queue_consumer, kwargs=config)
        Thread.wait_for_shutdown_signal(allow_exit=True, please_stop=worker.stopped)
    except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
        try:
            Log.error("Serious problem with consumer construction! Shutdown!", cause=e)
        finally:
            Log.stop()
