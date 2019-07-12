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

from mo_dots import Null
from mo_logs import Log
from mo_threads import Thread, Till
from tuid.service import TUIDService
from mo_dots import Null, wrap

_service = None


@pytest.fixture
def service(config, new_db):
    global _service
    if new_db == "yes":
        return TUIDService(database=Null, start_workers=False, kwargs=config.tuid)
    elif new_db == "no":
        if _service is None:
            _service = TUIDService(kwargs=config.tuid, start_workers=False)
        return _service
    else:
        Log.error("expecting 'yes' or 'no'")


def test_caching(service):
    # File changed revisions
    rev1_file_changed = "1aa26cb6f9d6"
    rev2_file_changed = "aa0394eb1c57"
    # File not changed revisions
    rev2_file_not_changed = "c0200f9fc1ab"
    # Oldest revision where file is not changed
    rev1_file_not_changed = "5ea694074089"
    service.clogger.initialize_to_range(rev1_file_not_changed, rev2_file_not_changed)

    test_files = [["gfx/gl/GLContextProviderGLX.cpp"]]

    for i, elem in enumerate(test_files):
        result1 = service.get_tuids_from_files(elem, rev1_file_not_changed)
        result2 = service.get_tuids_from_files(elem, rev2_file_not_changed)
        # It should be not equal because in between
        # these two revisions file has been changed.
        assert result1[0][0][1] != result2[0][0][1]

    # _get_annotation function should give a non None result because
    # rev1_file_changed and rev2_file_changed are in between the above
    # requested revisions, so it should have cached.
    result1_changed = service._get_annotation(rev1_file_changed, test_files[0][0])
    assert result1_changed
    assert result1_changed != service.stringify_tuids(result2[0][0][1])

    result2_changed = service._get_annotation(rev2_file_changed, test_files[0][0])
    assert result2_changed
    assert result2_changed == service.stringify_tuids(result2[0][0][1])
