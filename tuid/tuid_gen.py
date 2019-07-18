from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import gc
import os

import flask
from flask import Flask, Response, request

import objgraph
from mo_dots import listwrap, coalesce, unwraplist
from mo_json import value2json, json2value
from mo_logs import Log, constants, startup, Except
from mo_logs.strings import utf82unicode, unicode2utf8
from mo_threads.threads import RegisterThread
from mo_times import Timer
from pyLibrary.env import http
from pyLibrary.env.flask_wrappers import cors_wrapper
from tuid.service import TUIDService
from tuid.util import map_to_array

OVERVIEW = None
QUERY_SIZE_LIMIT = 10 * 1000 * 1000
EXPECTING_QUERY = b"expecting query\r\n"
TOO_BUSY = 10
TOO_MANY_THREADS = 4


def _get_one_tuid(service):
    # Returns a single TUID if it exists else None
    query = {
        "_source": {"includes": ["revision", "line", "file", "tuid"]},
        "query": {"bool": {"must": [{"term": {"tuid": 0}}]}},
        "size": 1,
    }
    temp = service.temporal.search(query).hits.hits[0]._source
    return temp


config = startup.read_settings(filename="/home/ajupazhamayil/TUID/tests/travis/config.json")
constants.set(config.constants)
Log.start(config.debug)

service = TUIDService(config.tuid)

temp = _get_one_tuid(service)
# {'file': 'gfx/thebes/GLContextProviderGLX.cpp', 'line': 1, 'tuid': 1, 'revision': '0ec22e77aefc'}
print(temp)
