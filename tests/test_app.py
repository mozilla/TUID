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

from mo_dots import wrap, Null
from mo_future import text_type, PY2
from mo_json import json2value
from mo_logs.strings import utf82unicode
from mo_threads import Process
from pyLibrary.env import http
from tuid.app import EXPECTING_QUERY

from tuid.client import TuidClient

app_process = None


@pytest.fixture(scope="session")
def app():
    global app_process

    pythonpath = str("." + os.pathsep + "vendor")
    if PY2:
        app_process = Null
    elif not app_process:
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


@pytest.mark.first_run
@pytest.mark.skipif(PY2, reason="interprocess communication problem")
def test_empty_query(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.get(url)
    assert response.status_code == 400
    assert response.content == EXPECTING_QUERY


@pytest.mark.first_run
@pytest.mark.skipif(os.name == 'nt', reason="can not send request on windows, I do not know why")
def test_query_too_big(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid/"
    name = "a"*10000000
    response = http.get(url, json={"from": name})
    assert response.status_code == 400
    assert response.content == b"request too large"


@pytest.mark.first_run
@pytest.mark.skipif(PY2, reason="interprocess communication problem")
def test_query_error(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.get(url, json={"from": "files"})
    error = json2value(utf82unicode(response.content))
    assert response.status_code == 400
    assert "expecting a simple where clause with following structure" in error.template


@pytest.mark.first_run
@pytest.mark.skipif(PY2, reason="interprocess communication problem")
def test_zero_files(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.post_json(url, json={
        "from": "files",
        "where": {"and": [
            {"eq": {"revision": "29dcc9cb77c372c97681a47496488ec6c623915d"}},
            {"in": {"path": []}},
            {"eq": {"branch": "mozilla-central"}}
        ]}
    })

    assert len(response.data) == 0


@pytest.mark.first_run
@pytest.mark.skipif(PY2, reason="interprocess communication problem")
def test_single_file(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.post_json(url, json={
        "from": "files",
        "where": {"and": [
            {"eq": {"revision": "29dcc9cb77c372c97681a47496488ec6c623915d"}},
            {"in": {"path": ["gfx/thebes/gfxFontVariations.h"]}},
            {"eq": {"branch": "mozilla-central"}}
        ]}
    })

    list_response = wrap([
        {h: v for h, v in zip(response.header, r)}
        for r in response.data
    ])
    tuids = list_response[0].tuids

    assert len(tuids) == 41  # 41 lines expected
    assert len(set(tuids)) == 41  # tuids much be unique

@pytest.mark.first_run
@pytest.mark.skipif(PY2, reason="interprocess communication problem")
def test_single_file_list(config, app):
    url = "http://localhost:" + text_type(config.flask.port) + "/tuid"
    response = http.post_json(url, json={
        "meta": {"format": "list"},
        "from": "files",
        "where": {"and": [
            {"eq": {"revision": "29dcc9cb77c372c97681a47496488ec6c623915d"}},
            {"in": {"path": ["gfx/thebes/gfxFontVariations.h"]}},
            {"eq": {"branch": "mozilla-central"}}
        ]}
    })

    list_response = response.data
    tuids = list_response[0].tuids

    assert len(tuids) == 41  # 41 lines expected
    assert len(set(tuids)) == 41  # tuids much be unique


@pytest.mark.first_run
@pytest.mark.skipif(PY2, reason="interprocess communication problem")
def test_client(config, app):
    client = TuidClient(config.client)
    client.get_tuid(
        revision="29dcc9cb77c372c97681a47496488ec6c623915d",
        file="gfx/thebes/gfxFontVariations.h",
        branch="mozilla-central"
    )


@pytest.mark.first_run
@pytest.mark.skipif(PY2, reason="interprocess communication problem")
def test_client_w_try(config, app):
    client = TuidClient(config.client)
    client.get_tuid(
        revision="0f4946791ddb",
        file="dom/base/nsWrapperCache.cpp",
        branch="try"
    )

