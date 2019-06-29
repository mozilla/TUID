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
    # Partition is to make sure threads are taken care by the caller itself
    service.clogger._fill_in_range(5, 'd63ed14ed622')

    query = {
        "_source": {"includes": ["revnum", "revision"]},
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "done"
                    }
                }
            }
        },
        "sort": [{"revnum": {"order": "desc"}}],
        "size":100
    }
    result = service.clogger.csetlog.search(query)
    for r in result.hits.hits:
        revision = r._source.revision
        revnum = r._source.revnum
        branch = service.config.hg.branch

        old_revision = "d63ed14ed622"
        new_revision = "c0200f9fc1ab"

        test_files = [
            ["/dom/html/HTMLCanvasElement.cpp"],
            ["/gfx/layers/ipc/CompositorBridgeChild.cpp"]
            ]


        result1 = service.get_tuids_from_files(test_files[1],old_revision)

        result2 = service.get_tuids_from_files(test_files[1], new_revision)

        # Update done = true
        updated_record = service.clogger._make_record_csetlog(revnum, revision, -1)
        updated_record["value"].update({"done": "true"})
        service.clogger.csetlog.add(updated_record)

    assert True