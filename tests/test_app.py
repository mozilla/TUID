# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import pytest
from mo_files import File
from mo_json import json2value
from mo_logs.strings import utf82unicode

from mo_future import text_type
from pyLibrary.env import http

app_process = None

@pytest.fixture(scope="session")
def app():
    global app_process
    if not app_process:
        app_process = Process(
            "TUID app",
            ["python", "tuid/app.py"],
            env={str("PYTHONPATH"): str(".;vendor")},
            debug=True
        )
    Till(seconds=1).wait()
    return app_process


def test_default(config, app):
    url = "http://localhost:"+text_type(config.flask.port)
    response = http.get(url)
    expected =File("tuid/public/index.html").read_bytes()
    assert response.content == expected


def test_query_error(config, app):
    url = "http://localhost:"+text_type(config.flask.port)+"/query"
    response = http.get(url, json={"from": "files"})
    error = json2value(utf82unicode(response.content))
    assert response.status_code == 400
    assert "expecting a simple where clause with following structure" in error.template
