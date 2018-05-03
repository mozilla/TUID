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

from mo_logs import Log, constants, startup

config = None

def pytest_addoption(parser):
    parser.addoption(
        "--new-db",
        action="store",
        default="no",
        help="`yes` or `no` to use a new database"
    )

@pytest.fixture
def new_db(request):
    return request.config.getoption("new_db")


@pytest.fixture(scope="session")
def config():
    config = startup.read_settings(filename=os.environ.get('TUID_CONFIG'))
    constants.set(config.constants)
    Log.start(config.debug)
    return config

