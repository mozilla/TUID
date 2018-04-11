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

from mo_logs import startup, Log, constants
from tuid.client import TuidClient

try:
    config = startup.read_settings()
    constants.set(config.constants)
    client = TuidClient(config.client)

    response = client.get_tuid("29dcc9cb77c3", "/js/src/builtin/TypedObject.h")
    Log.note("response={{response|json}}", response=response)

except BaseException as e:  # MUST CATCH BaseException BECAUSE argparse LIKES TO EXIT THAT WAY, AND gunicorn WILL NOT REPORT
    try:
        Log.error("Problem with example client!", cause=e)
    finally:
        Log.stop()
