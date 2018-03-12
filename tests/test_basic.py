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

import sql
from tidservice import TIDService

config = None


@pytest.fixture
def service(new_db):
    if new_db == 'yes':
        return TIDService(conn=sql.Sql(":memory:"))
    elif new_db == 'no':
        return TIDService(conn=sql.Sql("resources/test.db"))


def test_new_then_old(service):
    # delete database then run this test
    old = service.get_tids("/testing/geckodriver/CONTRIBUTING.md", "6162f89a4838")
    new = service.get_tids("/testing/geckodriver/CONTRIBUTING.md", "06b1a22c5e62")
    assert len(old) == len(new)
    for i in range(0, len(old)):
        assert old[i] == new[i]


def test_tids_on_changed_file(service):
    # https://hg.mozilla.org/integration/mozilla-inbound/file/a6fdd6eae583/taskcluster/ci/test/tests.yml
    old_lines = service.get_tids(  # 2205 lines
        "/taskcluster/ci/test/tests.yml", "a6fdd6eae583"
    )

    # THE FILE HAS NOT CHANGED, SO WE EXPECT THE SAME SET OF TIDs AND LINES TO BE RETURNED
    # https://hg.mozilla.org/integration/mozilla-inbound/file/a0bd70eac827/taskcluster/ci/test/tests.yml
    same_lines = service.get_tids(  # 2201 lines

        "/taskcluster/ci/test/tests.yml", "a0bd70eac827"
    )

    # assertAlmostEqual PERFORMS A STRUCURAL COMPARISION
    assert same_lines == old_lines

def test_removed_lines(service):
    # THE FILE HAS FOUR LINES REMOVED
    # https://hg.mozilla.org/integration/mozilla-inbound/rev/c8dece9996b7
    # https://hg.mozilla.org/integration/mozilla-inbound/file/c8dece9996b7/taskcluster/ci/test/tests.yml
    old_lines = service.get_tids(     # 2205 lines
        "/taskcluster/ci/test/tests.yml", "a6fdd6eae583"
    )
    new_lines = service.get_tids(     # 2201 lines
        "/taskcluster/ci/test/tests.yml", "c8dece9996b7"
    )

    # EXPECTING
    assert len(new_lines) == len(old_lines) - 4

def test_remove_file(service):
    entries = service.get_tids("/third_party/speedometer/InteractiveRunner.html", "e3f24e165618")
    assert 1 == len(entries)
    assert entries[0][0] == -1 and entries[0][1] == 0


def test_generic_1(service):
    old = service.get_tids("/gfx/ipc/GPUParent.cpp", "a5a2ae162869")
    new = service.get_tids("/gfx/ipc/GPUParent.cpp", "3acb30b37718")
    assert len(old) == 467
    assert len(new) == 476
    for i in range(1, 207):
        assert old[i] == new[i]


def test_file_with_line_replacement(service):
    new = service.get_tids("/python/mozbuild/mozbuild/action/test_archive.py", "e3f24e165618")
    old = service.get_tids("/python/mozbuild/mozbuild/action/test_archive.py", "c730f942ce30")
    assert 653 == len(new)
    assert 653 == len(old)
    for i in range(0, 600):
        if i == 374 or i == 376:
            assert old[i] != new[i]
        else:
            assert old[i] == new[i]


def test_distant_rev(service):
    old = service.get_tids("/python/mozbuild/mozbuild/action/test_archive.py", "e3f24e165618")
    new = service.get_tids("/python/mozbuild/mozbuild/action/test_archive.py", "0d1e55d87931")
    assert len(old) == 653
    assert len(new) == 653
    for i in range(0, 653):
        assert new[i] == old[i]


def test_new_file(service):
    rev = service.get_tids("/media/audioipc/server/src/lib.rs", "a39241b3e7b1")
    assert len(rev) == 636
#'''

def test_bad_date_file(service):
    # The following changeset is dated February 14, 2018 but was pushed to mozilla-central
    # on March 8, 2018. It modifies the file: dom/media/MediaManager.cpp
    # https://hg.mozilla.org/mozilla-central/rev/07fad8b0b417d9ae8580f23d697172a3735b546b
    change_one = service.get_tids("dom/media/MediaManager.cpp", "07fad8b0b417d9ae8580f23d697172a3735b546b")

    # Insert a change in between these dates to throw us off.
    # https://hg.mozilla.org/mozilla-central/rev/0451fe123f5b
    change_two = service.get_tids("dom/media/MediaManager.cpp", "0451fe123f5b")

    # Add the file just before these changes.
    # https://hg.mozilla.org/mozilla-central/rev/42c6ec43f782
    change_prev = service.get_tids("dom/media/MediaManager.cpp", "42c6ec43f782")

    # First revision (07fad8b0b417d9ae8580f23d697172a3735b546b) should be equal to the
    # tuids for it's child dated March 6.
    # https://hg.mozilla.org/mozilla-central/rev/7a6bc227dc03
    earliest_rev = service.get_tids("dom/media/MediaManager.cpp", "7a6bc227dc03")

    assert len(change_one) == len(earliest_rev)
    for i in range(0, len(change_one)):
        assert change_one[i] == earliest_rev[i]

def test_multi_parent_child_changes(service):
    # For this file: toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp
    # Multi-parent, multi-child change: https://hg.mozilla.org/mozilla-central/log/0ef34a9ec4fbfccd03ee0cfb26b182c03e28133a
    earliest_rev = service.get_tids("toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp", "0ef34a9ec4fbfccd03ee0cfb26b182c03e28133a")

    # A past revision: https://hg.mozilla.org/mozilla-central/rev/bb6db24a20dd
    past_rev =  service.get_tids("toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp", "bb6db24a20dd")

    # Check it on the child which doesn't modify it: https://hg.mozilla.org/mozilla-central/rev/39717163c6c9
    next_rev = service.get_tids("toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp", "39717163c6c9")

    assert len(earliest_rev) == len(next_rev)
    for i in range(0, len(earliest_rev)):
        assert next_rev[i] == earliest_rev[i]

def test_get_tids_from_revision(service):
    tids = service.get_tids_from_revision("a6fdd6eae583")
    assert tids != None