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

from mo_hg import hg_mozilla_org
from mo_kwargs import override
from mo_logs import startup, constants, Log
from mo_threads import Thread, Signal, Till, MAIN_THREAD
from mo_times import Timer, Date
from pyLibrary import aws
from pyLibrary.env import http
from tuid import service

# REQUIRED TO PREVENT constants FROM COMPLAINING

_ = service
_ = hg_mozilla_org


def one_request(request, please_stop):
    and_op = request.where['and']

    files = []
    for a in and_op:
        if a['in'].path:
            files = a['in'].path
        elif a.eq.path:
            files = [a.eq.path]

    with Timer("Make TUID request from {{timestamp|date}}", {"timestamp": request.meta.request_time}):
        try:
            result = http.post_json(
                "http://localhost:5000/tuid",
                json=request,
                timeout=30
            )
            if result is None or len(result.data) != len(files):
                Log.note("incomplete response for {{thread}}", thread=Thread.current().name)
        except Exception as e:
            Log.warning("Request failure", cause=e)

@override
def queue_consumer(pull_queue, please_stop=None):
    queue = aws.Queue(pull_queue)
    time_offset = None
    request_count = 0

    while not please_stop:
        request = queue.pop(till=please_stop)
        if please_stop:
            break
        if not request:
            Log.note("Nothing in queue, pausing for 5 seconds...")
            (please_stop | Till(seconds=5)).wait()
            continue

        now = Date.now().unix
        if time_offset is None:
            time_offset = now - request.meta.request_time

        next_request = request.meta.request_time + time_offset
        if next_request > now:
            Till(till=next_request).wait()

        Thread.run("request "+text_type(request_count), one_request, request)
        request_count += 1
        queue.commit()


if __name__ == '__main__':
    try:
        tmp_signal = Signal()
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)

        queue_consumer(kwargs=config, please_stop=tmp_signal)
        worker = Thread.run("sqs consumer", queue_consumer, kwargs=config)
        MAIN_THREAD.wait_for_shutdown_signal(allow_exit=True, please_stop=worker.stopped)
    except BaseException as e:
        Log.error("Serious problem with consumer construction! Shutdown!", cause=e)
