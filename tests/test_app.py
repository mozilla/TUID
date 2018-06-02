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

from mo_dots import wrap, Data
from mo_future import text_type
from mo_json import json2value
from mo_logs.strings import utf82unicode
from mo_threads import Process
from pyLibrary.env import http

app_process = True



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
        for line in app_process.stderr:
            if line.startswith(b' * Running on '):
                break
    yield
    app_process.stop()
    app_process.join(raise_on_error=False)


def test_empty_query(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.get(url)
    assert response.status_code == 400
    assert response.content == b"expecting query"


@pytest.mark.skip("can not send request on windows, I do not know why")
def test_query_too_big(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    name = "a"*10000000
    response = http.get(url, json={"from": name})
    assert response.status_code == 400
    assert response.content == b"request too large"


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
