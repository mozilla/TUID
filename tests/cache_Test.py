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

    old_revision = "aa0394eb1c57"
    new_revision = "c0200f9fc1ab"
    service.clogger.initialize_to_range(old_revision, new_revision)

    test_files = [
        ["/dom/html/HTMLCanvasElement.cpp"],
        ["/gfx/layers/ipc/CompositorBridgeChild.cpp"],
        ["/gfx/layers/wr/WebRenderCommandBuilder.h"],
        ["/gfx/layers/wr/WebRenderUserData.cpp"],
        ["/gfx/layers/wr/WebRenderUserData.h"],
        ["/layout/generic/nsFrame.cpp"],
        ["/layout/generic/nsIFrame.h"],
        ["/layout/generic/nsImageFrame.cpp"],
        ["/layout/painting/FrameLayerBuilder.cpp"],
        ["/widget/cocoa/nsNativeThemeCocoa.mm"]
    ]
    result1 = None
    for temp, elem in enumerate(test_files[9:]):
        result1 = service.get_tuids_from_files(elem,old_revision, going_forward=True)

    result2 = service.get_tuids_from_files(test_files[9], new_revision, going_forward=True)

    assert True