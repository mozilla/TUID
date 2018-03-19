# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os
import pytest
from mo_dots import wrap
from mo_files import File
from mo_json import json2value
from mo_logs.strings import utf82unicode
from mo_threads import Process, Till

from mo_future import text_type
from pyLibrary.env import http

app_process = None


@pytest.fixture(scope="session")
def app():
    global app_process

    pythonpath = str("." + os.pathsep + "vendor")
    if not app_process:
        app_process = Process(
            "TUID app",
            ["python", "tuid/app.py"],
            env={str("PYTHONPATH"): pythonpath},
            debug=True
        )
        Till(seconds=5).wait()  # Time to warm up
    yield
    app_process.please_stop.go()
    app_process.join(raise_on_error=False)


def test_default(config, app):
    url = "http://localhost:" + text_type(config.flask.port)
    response = http.get(url)
    expected = File("tuid/public/index.html").read_bytes()
    assert response.content == expected


def test_query_error(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.get(url, json={"from": "files"})
    error = json2value(utf82unicode(response.content))
    assert response.status_code == 400
    assert "expecting a simple where clause with following structure" in error.template


def test_single_file(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.post_json(url, json={
        "from": "files",
        "where": {"and": [
            {"eq": {"revision": "29dcc9cb77c372c97681a47496488ec6c623915d"}},
            {"in": {"path": ["gfx/thebes/gfxFontVariations.h"]}}
        ]}
    })

    list_response = wrap([
        {h: v for h, v in zip(response.header, r)}
        for r in response.data
    ])
    tuids = list_response[0].tuids

    assert len(tuids) == 41  # 41 lines expected
    assert len(set(tuids)) == 41  # tuids much be unique
