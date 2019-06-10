# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json

import pytest

from mo_dots import Null
from mo_logs import Log, Except
from mo_threads import Thread, Till
from mo_times import Timer
from pyLibrary.env import http
from pyLibrary.sql import sql_list, quote_set
from pyLibrary.sql.sqlite import quote_value, DOUBLE_TRANSACTION_ERROR, quote_list
from tuid.service import TUIDService
from tuid.util import map_to_array

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


def test_transactions(service):
    # This should pass
    old = service.get_tuids(
        "/testing/geckodriver/CONTRIBUTING.md", "6162f89a4838", commit=False
    )
    new = service.get_tuids(
        "/testing/geckodriver/CONTRIBUTING.md", "06b1a22c5e62", commit=False
    )

    assert len(old) == len(new)

    # listed_inserts = [None] * 100
    listed_inserts = [
        ("test" + str(count), str(count)) for count, entry in enumerate(range(100))
    ]
    listed_inserts.append("hello world")  # This should cause a transaction failure

    try:
        with service.conn.transaction() as t:
            count = 0
            while count < len(listed_inserts):
                tmp_inserts = listed_inserts[count : count + 50]
                count += 50
                t.execute(
                    "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES "
                    + sql_list(quote_list(i) for i in tmp_inserts)
                )
        assert False  # SHOULD NOT GET HERE
    except Exception as e:
        e = Except.wrap(e)
        assert "11 values for 2 columns" in e

    # Check that the transaction was undone
    latestTestMods = service.conn.get_one(
        "SELECT revision FROM latestFileMod WHERE file=?", ("test1",)
    )

    assert not latestTestMods


# @pytest.mark.skipif(True, reason="Broken transaction test.")
def test_transactions2(service):
    inserting = [("testing_transaction2_1", "1"), ("testing_transaction2_2", "2")]

    with service.conn.transaction() as t:
        # Make a change
        t.execute(
            "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES "
            + sql_list(quote_list(i) for i in inserting)
        )

        try:
            # Query for one change
            query_res1 = service.conn.get(
                "SELECT revision FROM latestFileMod WHERE file=?",
                ("testing_transaction2_1",),
            )
            assert False
        except Exception as e:
            assert DOUBLE_TRANSACTION_ERROR in e

        # Query for the other change
        query_res2 = t.get(
            "SELECT revision FROM latestFileMod WHERE file=?",
            ("testing_transaction2_2",),
        )

    assert query_res2[0][0] == "2"


@pytest.mark.first_run
def test_duplicate_ann_node_entries(service):
    # This test ensures that we can handle duplicate annotation
    # node entries.

    # After the first call with the following
    # file we should have no duplicate tuids.
    rev = "8eab40c27903"
    files = ["browser/base/content/browser.xul"]
    file, tuids = service.get_tuids(files, rev)[0]
    tuids_arr = map_to_array(tuids)
    known_duplicate_lines = [[650, 709], [651, 710]]
    for first_duped_line, second_duped_line in known_duplicate_lines:
        assert tuids_arr[first_duped_line - 1] != tuids_arr[second_duped_line - 1]

    # Second call on a future _unknown_ annotation will give us
    # duplicate entries.
    future_rev = "e02ce918e160"
    file, tuids = service.get_tuids(files, future_rev)[0]
    tuids_arr = map_to_array(tuids)
    for first_duped_line, second_duped_line in known_duplicate_lines:
        assert tuids_arr[first_duped_line - 1] == tuids_arr[second_duped_line - 1]


def test_tryrepo_tuids(service):
    test_file = [
        "dom/base/nsWrapperCache.cpp",
        "testing/mochitest/baselinecoverage/browser_chrome/browser.ini",
    ]
    test_revision = "0f4946791ddb"

    found_file = False
    result, _ = service.get_tuids_from_files(test_file, test_revision, repo="try")
    for file, tuids in result:
        if file == "testing/mochitest/baselinecoverage/browser_chrome/browser.ini":
            found_file = True
            assert len(tuids) == 3
    assert found_file


def test_multithread_tuid_uniqueness(service):
    timeout_seconds = 60
    revision = "d63ed14ed622"
    revision2 = "14dc6342ec50"
    #https://hg.mozilla.org/mozilla-central/rev/c0200f9fc1abf1e34a0bb1acb5a9f57d38ca677b

    test_files = [
        ["/gfx/layers/wr/WebRenderUserData.cpp"],
        ["/gfx/layers/wr/WebRenderUserData.h"],
        ["/layout/generic/nsFrame.cpp"],
        ["/layout/generic/nsIFrame.h"],
        ["/layout/generic/nsImageFrame.cpp"],
        ["/layout/painting/FrameLayerBuilder.cpp"],
        ["/layout/painting/nsDisplayList.h"],
        ["/layout/style/ImageLoader.cpp"],
        ["/layout/style/ServoStyleSet.cpp"],
        ["widget/cocoa/nsNativeThemeCocoa.mm"]
    ]

    test_files = [
        ["/browser/components/extensions/test/browser/browser_ext_webNavigation_onCreatedNavigationTarget_contextmenu.js"],
        ["/browser/components/extensions/test/browser/browser_ext_popup_corners.js"],
        ["/browser/components/extensions/test/browser/browser_ext_webNavigation_onCreatedNavigationTarget.js"],
        ["/browser/modules/test/browser/formValidation/browser_form_validation.js"],
        ["/devtools/.eslintrc.js"],
        ["/browser/base/content/test/general/browser_e10s_about_page_triggeringprincipal.js"],
        ["/browser/base/content/test/general/browser_keywordSearch_postData.js"],
        ["/browser/base/content/test/siteIdentity/browser_bug906190.js"],
        ["/browser/components/preferences/in-content/tests/browser_applications_selection.js"],
        ["/browser/components/preferences/in-content/tests/browser_basic_rebuild_fonts_test.js"]
    ]

    num_tests = len(test_files)
    # Call service on multiple threads at once
    tuided_files = [None] * num_tests
    threads = [
        Thread.run(
            str(i),
            service.mthread_testing_get_tuids_from_files,
            test_files[i],
            revision,
            tuided_files,
            i,
        )
        for i, a in enumerate(tuided_files)
    ]
    too_long = Till(seconds=timeout_seconds)
    for t in threads:
        t.join(till=too_long)
    assert not too_long

    #checks for uniqueness of tuids in different files
    tuidlist = [
        tm.tuid
        for ft in tuided_files
        for path, tuidmaps in ft
        for tm in tuidmaps
    ]
    # ensure no duplicates
    assert len(tuidlist) == len(set(tuidlist))

    tuided_files = [None] * num_tests
    threads2 = [
        Thread.run(
            str(i),
            service.mthread_testing_get_tuids_from_files,
            test_files[i],
            revision2,
            tuided_files,
            i,
        )
        for i, a in enumerate(tuided_files)
    ]
    too_long = Till(seconds=timeout_seconds*2)
    for t2 in threads2:
        t2.join(till=too_long)
    assert not too_long

    #checks for uniqueness of tuids in different files
    tuidlist2 = [
        tm.tuid
        for ft in tuided_files
        for path, tuidmaps in ft
        for tm in tuidmaps
    ]

    assert len(tuidlist2) == len(set(tuidlist2))


def test_multithread_tuid_uniqueness(service):
    timeout_seconds = 60
    old_revision = "d63ed14ed622"
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

    num_tests = len(test_files)
    # Call service on multiple threads at once
    tuided_files = [None] * num_tests
    threads = [
        Thread.run(
            str(i),
            service.mthread_testing_get_tuids_from_files,
            test_files[i],
            old_revision,
            tuided_files,
            i,
            going_forward=True
        )
        for i, a in enumerate(tuided_files)
    ]
    too_long = Till(seconds=timeout_seconds*4)
    for t in threads:
        t.join(till=too_long)
    assert not too_long

    #checks for uniqueness of tuids in different files
    tuidlist = [
        tm.tuid
        for ft in tuided_files
        for path, tuidmaps in ft
        for tm in tuidmaps
    ]
    # ensure no duplicates
    assert len(tuidlist) == len(set(tuidlist))

    #Checks for the TUID uniqueness after updating the file frontier
    tuided_files = [None] * num_tests
    threads = [
        Thread.run(
            str(i),
            service.mthread_testing_get_tuids_from_files,
            test_files[i],
            new_revision,
            tuided_files,
            i,
            going_forward=True
        )
        for i, a in enumerate(tuided_files)
    ]
    too_long = Till(seconds=timeout_seconds*4)
    for t in threads:
        t.join(till=too_long)
    assert not too_long

    #checks for uniqueness of tuids in different files
    tuidlist = [
        tm.tuid
        for ft in tuided_files
        for path, tuidmaps in ft
        for tm in tuidmaps
    ]
    # ensure no duplicates
    assert len(tuidlist) == len(set(tuidlist))


def test_multithread_service(service):
    num_tests = 10
    timeout_seconds = 60
    revision = "d63ed14ed622"
    test_file = ["devtools/server/tests/browser/browser_markers-docloading-03.js"]

    # Call service on multiple threads at once
    tuided_files = [None] * num_tests
    threads = [
        Thread.run(
            str(i),
            service.mthread_testing_get_tuids_from_files,
            test_file,
            revision,
            tuided_files,
            i,
        )
        for i, a in enumerate(tuided_files)
    ]
    too_long = Till(seconds=timeout_seconds)
    for t in threads:
        t.join(till=too_long)
    assert not too_long

    # All returned results should be the same.
    expected_filename, expected_tuids = tuided_files[0][0]
    for result in tuided_files[1:]:
        assert len(result) == len(
            test_file
        )  # get_tuid returns a list of (file, tuids) tuples
        filename, tuids = result[0]
        assert filename == expected_filename
        assert set(tuids) == set(expected_tuids)

    # Check that we can get the same result after these
    # calls.
    tuids, _ = service.get_tuids_from_files(test_file, revision, use_thread=False)
    assert len(tuids[0][1]) == 41

    for tuid_count, mapping in enumerate(tuids[0][1]):
        if mapping.tuid != tuided_files[0][0][1][tuid_count].tuid:  # Use first result
            # All lines should have a mapping
            assert False

    # Double check to make sure we have no None values.
    for mapping in tuids[0][1]:
        if mapping.tuid is None:  # Use first result
            # All lines should have a mapping
            assert False


def test_new_then_old(service):
    # delete database then run this test
    old = service.get_tuids("/testing/geckodriver/CONTRIBUTING.md", "6162f89a4838")
    new = service.get_tuids("/testing/geckodriver/CONTRIBUTING.md", "06b1a22c5e62")

    assert len(old) == len(new)
    for i in range(0, len(old)):
        assert old[i] == new[i]


def test_tuids_on_changed_file(service):
    # https://hg.mozilla.org/integration/mozilla-inbound/file/a6fdd6eae583/taskcluster/ci/test/tests.yml
    old_lines = service.get_tuids(  # 2205 lines
        "/taskcluster/ci/test/tests.yml", "a6fdd6eae583"
    )

    # THE FILE HAS NOT CHANGED, SO WE EXPECT THE SAME SET OF TUIDs AND LINES TO BE RETURNED
    # https://hg.mozilla.org/integration/mozilla-inbound/file/a0bd70eac827/taskcluster/ci/test/tests.yml
    same_lines = service.get_tuids(  # 2201 lines
        "/taskcluster/ci/test/tests.yml", "a0bd70eac827"
    )

    # assertAlmostEqual PERFORMS A STRUCURAL COMPARISION
    assert same_lines == old_lines


def test_removed_lines(service):
    # THE FILE HAS FOUR LINES REMOVED
    # https://hg.mozilla.org/integration/mozilla-inbound/rev/c8dece9996b7
    # https://hg.mozilla.org/integration/mozilla-inbound/file/c8dece9996b7/taskcluster/ci/test/tests.yml
    old_lines = service.get_tuids(  # 2205 lines
        "/taskcluster/ci/test/tests.yml", "a6fdd6eae583"
    )
    new_lines = service.get_tuids(  # 2201 lines
        "/taskcluster/ci/test/tests.yml", "c8dece9996b7"
    )

    # EXPECTING
    assert len(new_lines[0][1]) == len(old_lines[0][1]) - 4


def test_remove_file(service):
    entries = service.get_tuids(
        "/third_party/speedometer/InteractiveRunner.html", "e3f24e165618"
    )
    assert 0 == len(entries[0][1])


def test_generic_1(service):
    old = service.get_tuids("/gfx/ipc/GPUParent.cpp", "a5a2ae162869")[0][1]
    new = service.get_tuids("/gfx/ipc/GPUParent.cpp", "3acb30b37718")[0][1]
    assert len(old) == 467
    assert len(new) == 476
    for i in range(1, 207):
        assert old[i] == new[i]


def test_parallel_get_tuids(service):
    with open("resources/stressfiles.json", "r") as f:
        files = json.load(f)
    old = service.get_tuids(files[:15], "a5a2ae162869")

    assert old is not None


def test_500_file(service):
    # This file is non existent and should not have tuids
    tuids = service.get_tuids("/browser/garbage.garbage", "d3ed36f4fb7a")
    assert len(tuids[0][1]) == 0


def test_file_with_line_replacement(service):
    new = service.get_tuids(
        "/python/mozbuild/mozbuild/action/test_archive.py", "e3f24e165618"
    )
    old = service.get_tuids(
        "/python/mozbuild/mozbuild/action/test_archive.py", "c730f942ce30"
    )
    new = new[0][1]
    old = old[0][1]
    assert 653 == len(new)
    assert 653 == len(old)
    for i in range(0, 600):
        if i == 374 or i == 376:
            assert old[i] != new[i]
        else:
            assert old[i] == new[i]


def test_distant_rev(service):
    old = service.get_tuids(
        "/python/mozbuild/mozbuild/action/test_archive.py", "e3f24e165618"
    )
    new = service.get_tuids(
        "/python/mozbuild/mozbuild/action/test_archive.py", "0d1e55d87931"
    )
    new = new[0][1]
    old = old[0][1]
    assert len(old) == 653
    assert len(new) == 653
    for i in range(0, 653):
        assert new[i] == old[i]


def test_new_file(service):
    rev = service.get_tuids("/media/audioipc/server/src/lib.rs", "a39241b3e7b1")
    assert len(rev[0][1]) == 636


def test_bad_date_file(service):
    # The following changeset is dated February 14, 2018 but was pushed to mozilla-central
    # on March 8, 2018. It modifies the file: dom/media/MediaManager.cpp
    # https://hg.mozilla.org/mozilla-central/rev/07fad8b0b417d9ae8580f23d697172a3735b546b
    change_one = service.get_tuids(
        "dom/media/MediaManager.cpp", "07fad8b0b417d9ae8580f23d697172a3735b546b"
    )[0][1]

    # Insert a change in between these dates to throw us off.
    # https://hg.mozilla.org/mozilla-central/rev/0451fe123f5b
    change_two = service.get_tuids("dom/media/MediaManager.cpp", "0451fe123f5b")[0][1]

    # Add the file just before these changes.
    # https://hg.mozilla.org/mozilla-central/rev/42c6ec43f782
    change_prev = service.get_tuids("dom/media/MediaManager.cpp", "42c6ec43f782")[0][1]

    # First revision (07fad8b0b417d9ae8580f23d697172a3735b546b) should be equal to the
    # tuids for it's child dated March 6.
    # https://hg.mozilla.org/mozilla-central/rev/7a6bc227dc03
    earliest_rev = service.get_tuids("dom/media/MediaManager.cpp", "7a6bc227dc03")[0][1]

    assert len(change_one) == len(earliest_rev)
    for i in range(0, len(change_one)):
        assert change_one[i] == earliest_rev[i]


def test_multi_parent_child_changes(service):
    # For this file: toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp
    # Multi-parent, multi-child change: https://hg.mozilla.org/mozilla-central/log/0ef34a9ec4fbfccd03ee0cfb26b182c03e28133a
    earliest_rev = service.get_tuids(
        "toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp",
        "0ef34a9ec4fbfccd03ee0cfb26b182c03e28133a",
    )[0][1]

    # A past revision: https://hg.mozilla.org/mozilla-central/rev/bb6db24a20dd
    past_rev = service.get_tuids(
        "toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp",
        "bb6db24a20dd",
    )[0][1]

    # Check it on the child which doesn't modify it: https://hg.mozilla.org/mozilla-central/rev/39717163c6c9
    next_rev = service.get_tuids(
        "toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp",
        "39717163c6c9",
    )[0][1]

    assert len(earliest_rev) == len(next_rev)
    for i in range(0, len(earliest_rev)):
        assert next_rev[i] == earliest_rev[i]


def test_get_tuids_from_revision(service):
    tuids = service.get_tuids_from_revision("a6fdd6eae583")
    assert tuids != None


def test_many_files_one_revision(service):
    with open("resources/stressfiles.json", "r") as f:
        files = json.load(f)
    test_file_init = ["widget/cocoa/nsCocoaWindow.mm"]
    dir = ""
    tmp = [dir + f for f in files][:10]

    test_file = test_file_init + tmp
    first_front = "739c536d2cd6"
    test_rev = "159e1105bdc7"

    service.clogger.csetlog.refresh()
    service.clogger.disable_all()
    service.clogger.initialize_to_range(first_front, test_rev)
    service.clogger.disable_backfilling = False
    service.clogger.start_backfilling()

    with service.conn.transaction() as t:
        t.execute("DELETE FROM latestFileMod WHERE file IN " + quote_set(test_file))
        t.execute("DELETE FROM annotations WHERE file IN " + quote_set(test_file))

    Log.note("Total files: {{total}}", total=str(len(test_file)))

    old, _ = service.get_tuids_from_files(test_file, first_front, use_thread=False)
    print("old:")
    for el in old:
        print(el[0])
        print("     " + el[0] + ":" + str(len(el[1])))

    new, _ = service.get_tuids_from_files(test_file, test_rev, use_thread=False)
    print("new:")
    for el in new:
        print("     " + el[0] + ":" + str(len(el[1])))


def test_one_addition_many_files(service):
    old_rev = "159e1105bdc7"
    test_rev = "58eb13b394f4"  # 11 Lines added, 1 removed

    with open("resources/stressfiles.json", "r") as f:
        files = json.load(f)
    test_file_change = ["widget/cocoa/nsCocoaWindow.mm"]
    dir = ""
    tmp = [dir + f for f in files][:1]  # TEST WITH SOME OTHER NUMBER OF FILES
    test_file = test_file_change + tmp

    service.clogger.csetlog.refresh()
    service.clogger.disable_all()
    service.clogger.initialize_to_range(old_rev, test_rev)
    service.clogger.disable_backfilling = False
    service.clogger.start_backfilling()

    with service.conn.transaction() as t:
        t.execute("DELETE FROM latestFileMod WHERE file IN " + quote_set(test_file))
        t.execute("DELETE FROM annotations WHERE file IN " + quote_set(test_file))

    # Get current annotation
    result, _ = service.get_tuids_from_files(test_file_change, old_rev)

    _, curr_tuids = result[0]
    curr_tuid_array = map_to_array(curr_tuids)

    # remove line 2148, add eleven lines
    expected_tuid_array = curr_tuid_array[:2147] + ([-1] * 11) + curr_tuid_array[2148:]

    Log.note("Total files: {{total}}", total=str(len(test_file)))

    tuid_response, _ = service.get_tuids_from_files(
        test_file, test_rev, use_thread=False
    )
    print("new:")
    for filename, tuids in tuid_response:
        print("     " + filename + ":" + str(len(tuids)))
        if filename != test_file_change[0]:
            continue
        new_tuid_array = map_to_array(tuids)

        assert len(new_tuid_array) == len(expected_tuid_array)
        for new_tuid, curr_tuid in zip(new_tuid_array, expected_tuid_array):
            if curr_tuid == -1:
                continue
            assert new_tuid == curr_tuid


def test_one_http_call_required(service):
    files = [
        "/browser/base/content/test/general/browser_bug423833.js",
        "/browser/base/content/test/general/browser_bug575561.js",
        "/browser/base/content/test/general/browser_bug678392.js",
        "/browser/base/content/test/general/browser_bug767836_perwindowpb.js",
        "/browser/base/content/test/general/browser_e10s_about_page_triggeringprincipal.js",
        "/browser/base/content/test/general/browser_keywordSearch_postData.js",
        "/browser/base/content/test/general/browser_tabfocus.js",
        "/browser/base/content/test/pageinfo/browser_pageinfo_image_info.js",
        "/browser/base/content/test/popupNotifications/browser_popupNotification_keyboard.js",
        "/browser/base/content/test/sidebar/browser_bug409481.js",
        "/browser/base/content/test/siteIdentity/browser_bug906190.js",
        "/browser/base/content/test/tabcrashed/browser_autoSubmitRequest.js",
        "/browser/base/content/test/tabcrashed/browser_clearEmail.js",
        "/browser/base/content/test/tabcrashed/browser_showForm.js",
        "/browser/base/content/test/tabcrashed/browser_shown.js",
        "/browser/base/content/test/urlbar/browser_urlbarKeepStateAcrossTabSwitches.js",
        "/browser/base/content/test/webextensions/browser_extension_sideloading.js",
        "/browser/components/contextualidentity/test/browser/browser_usercontext.js",
        "/browser/components/customizableui/test/browser_947914_button_addons.js",
        "/browser/components/enterprisepolicies/tests/browser/browser_policies_notice_in_aboutpreferences.js",
        "/browser/components/enterprisepolicies/tests/browser/browser_policy_disable_masterpassword.js",
        "/browser/components/extensions/test/browser/browser_ext_browserAction_popup_resize.js",
        "/browser/components/extensions/test/browser/browser_ext_find.js",
        "/browser/components/extensions/test/browser/browser_ext_omnibox.js",
        "/browser/components/extensions/test/browser/browser_ext_pageAction_popup_resize.js",
        "/browser/components/extensions/test/browser/browser_ext_popup_background.js",
        "/browser/components/extensions/test/browser/browser_ext_popup_corners.js",
        "/browser/components/extensions/test/browser/browser_ext_webNavigation_onCreatedNavigationTarget.js",
        "/browser/components/extensions/test/browser/browser_ext_webNavigation_onCreatedNavigationTarget_contextmenu.js",
        "/browser/components/preferences/in-content/tests/browser_advanced_update.js",
        "/browser/components/preferences/in-content/tests/browser_applications_selection.js",
        "/browser/components/preferences/in-content/tests/browser_basic_rebuild_fonts_test.js",
        "/browser/components/preferences/in-content/tests/browser_bug1018066_resetScrollPosition.js",
        "/browser/components/preferences/in-content/tests/browser_bug1020245_openPreferences_to_paneContent.js",
        "/browser/components/preferences/in-content/tests/browser_bug1184989_prevent_scrolling_when_preferences_flipped.js",
        "/browser/components/preferences/in-content/tests/browser_bug410900.js",
        "/browser/components/preferences/in-content/tests/browser_change_app_handler.js",
        "/browser/components/preferences/in-content/tests/browser_checkspelling.js",
        "/browser/components/preferences/in-content/tests/browser_cookies_exceptions.js",
        "/browser/components/preferences/in-content/tests/browser_engines.js",
        "/browser/components/preferences/in-content/tests/browser_extension_controlled.js",
        "/browser/components/preferences/in-content/tests/browser_fluent.js",
        "/browser/components/preferences/in-content/tests/browser_homepages_filter_aboutpreferences.js",
        "/browser/components/preferences/in-content/tests/browser_languages_subdialog.js",
        "/browser/components/preferences/in-content/tests/browser_layersacceleration.js",
        "/browser/components/preferences/in-content/tests/browser_masterpassword.js",
        "/browser/components/preferences/in-content/tests/browser_notifications_do_not_disturb.js",
        "/browser/components/preferences/in-content/tests/browser_password_management.js",
        "/browser/components/preferences/in-content/tests/browser_performance.js",
        "/browser/components/preferences/in-content/tests/browser_performance_e10srollout.js",
        "/browser/components/preferences/in-content/tests/browser_performance_non_e10s.js",
        "/browser/components/preferences/in-content/tests/browser_permissions_urlFieldHidden.js",
        "/browser/components/preferences/in-content/tests/browser_privacypane.js",
        "/browser/components/preferences/in-content/tests/browser_sanitizeOnShutdown_prefLocked.js",
        "/browser/components/preferences/in-content/tests/browser_search_within_preferences_1.js",
        "/browser/components/preferences/in-content/tests/browser_search_within_preferences_2.js",
        "/browser/components/preferences/in-content/tests/browser_search_within_preferences_command.js",
        "/browser/components/preferences/in-content/tests/browser_security-1.js",
        "/browser/components/preferences/in-content/tests/browser_security-2.js",
        "/browser/components/preferences/in-content/tests/browser_site_login_exceptions.js",
        "/browser/components/preferences/in-content/tests/browser_spotlight.js",
        "/browser/components/preferences/in-content/tests/browser_subdialogs.js",
        "/browser/components/preferences/in-content/tests/siteData/browser_siteData.js",
        "/browser/components/preferences/in-content/tests/siteData/browser_siteData2.js",
        "/browser/components/preferences/in-content/tests/siteData/browser_siteData3.js",
        "/browser/components/search/test/browser_aboutSearchReset.js",
        "/browser/components/search/test/browser_abouthome_behavior.js",
        "/browser/components/sessionstore/test/browser_480893.js",
        "/browser/components/sessionstore/test/browser_590563.js",
        "/browser/components/sessionstore/test/browser_705597.js",
        "/browser/components/sessionstore/test/browser_707862.js",
        "/browser/components/sessionstore/test/browser_aboutSessionRestore.js",
        "/browser/components/sessionstore/test/browser_crashedTabs.js",
        "/browser/components/sessionstore/test/browser_swapDocShells.js",
        "/browser/components/shell/test/browser_1119088.js",
        "/browser/extensions/onboarding/test/browser/browser_onboarding_skip_tour.js",
        "/browser/extensions/onboarding/test/browser/browser_onboarding_tourset.js",
        "/browser/modules/test/browser/formValidation/browser_form_validation.js",
        "/devtools/.eslintrc.js",
        "/devtools/client/debugger/new/test/mochitest/head.js",
        "/devtools/client/netmonitor/test/browser_net_resend_cors.js",
        "/devtools/client/responsive.html/test/browser/browser_page_state.js",
        "/devtools/client/shared/test/browser_toolbar_webconsole_errors_count.js",
        "/devtools/client/sourceeditor/test/browser_css_autocompletion.js",
        "/devtools/client/sourceeditor/test/browser_css_getInfo.js",
        "/devtools/client/sourceeditor/test/browser_css_statemachine.js",
        "/devtools/server/tests/browser/browser_markers-docloading-01.js",
        "/devtools/server/tests/browser/browser_markers-docloading-02.js",
        "/devtools/server/tests/browser/browser_markers-docloading-03.js",
        "/devtools/server/tests/browser/browser_storage_dynamic_windows.js",
        "/devtools/server/tests/browser/browser_storage_updates.js",
        "/toolkit/components/narrate/test/browser_narrate.js",
        "/toolkit/components/narrate/test/browser_narrate_language.js",
        "/toolkit/components/narrate/test/browser_voiceselect.js",
        "/toolkit/components/narrate/test/browser_word_highlight.js",
        "/toolkit/components/normandy/test/browser/browser_about_preferences.js",
        "/toolkit/components/normandy/test/browser/browser_about_studies.js",
        "/toolkit/components/payments/test/browser/browser_host_name.js",
        "/toolkit/components/payments/test/browser/browser_profile_storage.js",
        "/toolkit/components/payments/test/browser/browser_request_serialization.js",
        "/toolkit/components/payments/test/browser/browser_request_summary.js",
        "/toolkit/components/payments/test/browser/browser_total.js",
        "/toolkit/components/reader/test/browser_readerMode_with_anchor.js",
        "/toolkit/content/tests/browser/browser_datetime_datepicker.js",
        "/toolkit/content/tests/browser/browser_saveImageURL.js",
        "/toolkit/content/tests/browser/browser_save_resend_postdata.js",
        "/toolkit/mozapps/extensions/test/browser/browser_bug562797.js",
        "/toolkit/mozapps/extensions/test/browser/browser_discovery.js",
        "/toolkit/mozapps/extensions/test/browser/browser_discovery_install.js",
        "/tools/lint/docs/linters/eslint-plugin-mozilla.rst",
        "/tools/lint/eslint/eslint-plugin-mozilla/lib/configs/browser-test.js",
        "/tools/lint/eslint/eslint-plugin-mozilla/lib/index.js",
        "/tools/lint/eslint/eslint-plugin-mozilla/lib/rules/no-cpows-in-tests.js",  # This file is removed in the newer revision
        "/tools/lint/eslint/eslint-plugin-mozilla/tests/no-cpows-in-tests.js",  # This file is removed in the newer revision
        "/testing/geckodriver/CONTRIBUTING.md",  # Unchannged file
        "/dom/media/MediaManager.cpp",  # Unchanged file
    ]

    non_existent = {
        "tools/lint/eslint/eslint-plugin-mozilla/lib/rules/no-cpows-in-tests.js": 1,
        "tools/lint/eslint/eslint-plugin-mozilla/tests/no-cpows-in-tests.js": 1,
    }

    # TODO: Check files with added/changed lines.
    # TODO: Check files which should not have changed.
    changed_files = {
        "browser/components/search/test/browser_aboutSearchReset.js": {
            "changes": {"removed": [67, 86, 119], "added": []}
        },
        "toolkit/components/narrate/test/browser_voiceselect.js": {
            "changes": {"removed": [7, 8], "added": []}
        },
        "toolkit/components/narrate/test/browser_word_highlight.js": {
            "changes": {"removed": [7, 8], "added": []}
        },
    }

    files_not_changed = {
        "testing/geckodriver/CONTRIBUTING.md": 1,
        "dom/media/MediaManager.cpp": 1,
    }

    service.clogger.csetlog.refresh()
    service.clogger.disable_all()
    service.clogger.initialize_to_range("d63ed14ed622", "14dc6342ec50")
    service.clogger.csetlog.refresh()
    service.clogger.disable_backfilling = False
    service.clogger.start_backfilling()
    service.clogger.csetlog.refresh()

    # SETUP
    proc_files = (
        ["/dom/base/Link.cpp"] + files[-10:] + [k for k in changed_files]
    )  # Useful in testing

    with service.conn.transaction() as t:
        t.execute("DELETE FROM latestFileMod WHERE file IN " + quote_set(proc_files))
        t.execute("DELETE FROM annotations WHERE file IN " + quote_set(proc_files))

    Log.note("Number of files to process: {{flen}}", flen=len(files))
    first_f_n_tuids, _ = service.get_tuids_from_files(
        proc_files, "d63ed14ed622", use_thread=False
    )

    # THIS NEXT CALL SHOULD BE FAST, DESPITE THE LACK OF LOCAL CACHE
    http.DEBUG = True
    Log.alert("start http request count")
    start = http.request_count
    timer = Timer("get next revision")
    with timer:
        f_n_tuids, _ = service.get_tuids_from_files(
            proc_files, "14dc6342ec50", use_thread=False
        )
    num_http_calls = http.request_count - start
    http.DEBUG = False

    # assert num_http_calls <= 3  # 2 DIFFS FROM ES, AND ONE CALL TO hg.mo
    assert timer.duration.seconds < 30

    assert len(proc_files) == len(f_n_tuids)

    # Check removed files
    for (fname, tuids) in f_n_tuids:
        if fname in non_existent:
            assert len(tuids) == 0

    # Check removed lines
    for (fname, tuids) in first_f_n_tuids:
        if fname in changed_files:
            rmed = changed_files[fname]["changes"]["removed"]
            tmp_ts = {}
            for tmap in tuids:
                if tmap.line in rmed:
                    tmp_ts[str(tmap.line)] = tmap.tuid

            for (fname2, tuids) in f_n_tuids:
                if fname == fname2:
                    for tmap in tuids:
                        if str(tmap.line) in tmp_ts:
                            assert tmap.tuid != tmp_ts[str(tmap.line)]

    # Check unchanged files
    for (fname1, tuids1) in first_f_n_tuids:
        if fname1 not in files_not_changed:
            continue

        for (fname2, tuids2) in f_n_tuids:
            if fname2 != fname1:
                continue

            for count, tmap1 in enumerate(tuids1):
                assert tmap1.tuid == tuids2[count].tuid


def test_long_file(service):
    timer = Timer("test", silent=True)

    with timer:
        service.get_tuids(
            files="gfx/angle/checkout/src/libANGLE/formatutils.cpp",
            revision="29dcc9cb77c3",
        )

    assert timer.duration.seconds < 30


def test_out_of_order_get_tuids_from_files(service):
    rev_initial = "3eccd139667d"
    rev_latest = "4e9446f9e8f0"
    rev_middle = "9b7db28b360d"
    test_file = ["dom/base/nsWrapperCache.cpp"]
    service.clogger.csetlog.refresh()
    service.clogger.disable_all()
    service.clogger.initialize_to_range(rev_initial, rev_latest)
    test_file = ["dom/base/nsWrapperCache.cpp"]
    with service.conn.transaction() as t:
        t.execute("DELETE FROM latestFileMod WHERE file=" + quote_value(test_file[0]))
        t.execute("DELETE FROM annotations WHERE file=" + quote_value(test_file[0]))

    check_lines = [41]

    result1, _ = service.get_tuids_from_files(test_file, rev_initial, use_thread=False)
    result2, _ = service.get_tuids_from_files(test_file, rev_latest, use_thread=False)
    test_result, _ = service.get_tuids_from_files(
        test_file, rev_middle, use_thread=False
    )
    # Check that test_result's tuids at line 41 is different from
    # result 2.
    entered = False
    for (fname, tuids2) in result2:
        if fname not in test_file:
            # If we find another file, this test fails
            assert fname == test_file[0]

        for (fname_test, tuids_test) in test_result:
            # Check that check_line entries are different
            for count, tmap in enumerate(tuids2):
                entered = True
                if tmap.line not in check_lines:
                    assert tmap.tuid == tuids_test[count].tuid
                else:
                    assert tmap.tuid != tuids_test[count].tuid
    assert entered


def test_out_of_order_going_forward_get_tuids_from_files(service):
    rev_initial = "3eccd139667d"
    rev_latest = "4e9446f9e8f0"
    rev_latest2 = "9dfb7673f106393b79226"
    rev_middle = "9b7db28b360d"

    service.clogger.csetlog.refresh()
    service.clogger.disable_all()
    service.clogger.initialize_to_range(rev_initial, rev_latest2)
    test_file = ["dom/base/nsWrapperCache.cpp"]
    with service.conn.transaction() as t:
        t.execute("DELETE FROM latestFileMod WHERE file=" + quote_value(test_file[0]))
        t.execute("DELETE FROM annotations WHERE file=" + quote_value(test_file[0]))

    check_lines = [41]

    result1, _ = service.get_tuids_from_files(
        test_file, rev_initial, going_forward=True, use_thread=False
    )
    result2, _ = service.get_tuids_from_files(
        test_file, rev_latest, going_forward=True, use_thread=False
    )
    test_result, _ = service.get_tuids_from_files(
        test_file, rev_middle, going_forward=True, use_thread=False
    )
    result2, _ = service.get_tuids_from_files(
        test_file, rev_latest2, going_forward=True, use_thread=False
    )

    # Check that test_result's tuids at line 41 is different from
    # result 2.
    entered = False
    for (fname, tuids2) in result2:
        if fname not in test_file:
            # If we find another file, this test fails
            assert fname == test_file[0]

        for (fname_test, tuids_test) in test_result:
            # Check that check_line entries are different
            for count, tmap in enumerate(tuids2):
                entered = True
                if tmap.line not in check_lines:
                    assert tmap.tuid == tuids_test[count].tuid
                else:
                    assert tmap.tuid != tuids_test[count].tuid
    assert entered


@pytest.mark.first_run
def test_threaded_service_call(service):
    # Will fail on second runs using the same dataset as it's
    # checking threading capabilities.
    timeout_seconds = 1
    mc_revision = "04cc917f68c5"
    test_file = [
        "browser/components/payments/test/browser/browser_host_name.js",
        "/browser/components/extensions/test/browser/browser_ext_omnibox.js",
        "/browser/components/extensions/test/browser/browser_ext_pageAction_popup_resize.js",
        "/browser/components/extensions/test/browser/browser_ext_popup_background.js",
        "/browser/components/extensions/test/browser/browser_ext_popup_corners.js",
        "/toolkit/components/reader/test/browser_readerMode_with_anchor.js",
    ]

    res, completed = service.get_tuids_from_files(
        test_file, mc_revision, going_forward=True
    )
    assert not completed
    assert all([len(tuids) == 0 for file, tuids in res])

    while not completed:
        # Wait a bit to let them process
        Till(seconds=timeout_seconds).wait()

        # Try getting them again
        res, completed = service.get_tuids_from_files(
            test_file, mc_revision, going_forward=True
        )

    assert completed
    assert all([len(tuids) > 0 for file, tuids in res])


def test_try_rev_then_mc(service):
    try_revision = "4a0e5e6c2b73"
    mc_revision = "04cc917f68c5"
    test_file = ["browser/components/payments/test/browser/browser_host_name.js"]
    file_length = 34

    res1, _ = service.get_tuids_from_files(
        test_file, try_revision, repo="try", going_forward=True, use_thread=False
    )
    assert len(res1[0][1]) == 0

    res2, _ = service.get_tuids_from_files(
        test_file,
        mc_revision,
        repo="mozilla-central",
        going_forward=True,
        use_thread=False,
    )
    assert len(res2[0][1]) == file_length

    for tuid_map in res2[0][1]:
        if tuid_map.tuid is None:
            assert False


def test_merged_changes(service):
    old_rev = "316e5fab18f1"
    new_rev = "06d10d09e6ee"
    test_files = ["js/src/wasm/WasmTextToBinary.cpp"]

    service.clogger.csetlog.refresh()
    service.clogger.disable_all()
    service.clogger.initialize_to_range(old_rev, new_rev)
    with service.conn.transaction() as t:
        t.execute("DELETE FROM latestFileMod WHERE file=" + quote_value(test_files[0]))
        t.execute("DELETE FROM annotations WHERE file=" + quote_value(test_files[0]))

    old_tuids, _ = service.get_tuids_from_files(test_files, old_rev, use_thread=False)
    new_tuids, _ = service.get_tuids_from_files(test_files, new_rev, use_thread=False)

    lines_added = {"js/src/wasm/WasmTextToBinary.cpp": [1668, 1669, 1670, 1671]}
    completed = 0
    for file, old_file_tuids in old_tuids:
        if file in lines_added:
            assert len(old_file_tuids) == 5461

            for new_file, tmp_tuids in new_tuids:
                new_file_tuids = []
                if new_file == file:
                    for tuid_map in tmp_tuids:
                        if tuid_map.line in lines_added[file]:
                            new_file_tuids.append(tuid_map.tuid)

                    assert len(tmp_tuids) == 5461

                    # No tuids from the new should be in the old
                    # so this intersection should always be empty.
                    assert (
                        len(set(new_file_tuids) & set([t.tuid for t in old_file_tuids]))
                        <= 0
                    )
                    completed += 1

                    break
    assert completed == len(lines_added.keys())


@pytest.mark.skip(
    reason="Very long to get diffs. It tests across multiple merges to ensure TUIDs are stable."
)
def test_very_distant_files(service):
    new_rev = "6e8e861540e6"
    old_rev = "1e2c9151a09e"
    test_files = ["docshell/base/nsDocShell.cpp"]

    service.clogger.csetlog.refresh()
    service.clogger.disable_all()
    service.clogger.initialize_to_range(old_rev, new_rev)

    with service.conn.transaction() as t:
        t.execute("DELETE FROM annotations WHERE revision = " + quote_value(new_rev))
        for file in test_files:
            t.execute(
                "UPDATE latestFileMod SET revision = "
                + quote_value(old_rev)
                + " WHERE file = "
                + quote_value(file)
            )

    old_tuids, _ = service.get_tuids_from_files(
        test_files, old_rev, use_thread=False, max_csets_proc=10000
    )
    new_tuids, _ = service.get_tuids_from_files(
        test_files, new_rev, use_thread=False, max_csets_proc=10000
    )

    lines_moved = {"docshell/base/nsDocShell.cpp": {1028: 1026, 1097: 1029}}
    lines_added = {"docshell/base/nsDocShell.cpp": [2770]}

    Log.note("Check output manually for any abnormalities as well.")

    completed = 0
    for file, old_file_tuids in old_tuids:
        if file in lines_moved:
            old_moved_tuids = {}
            print("OLD:")
            for tuid_map in old_file_tuids:
                print(str(tuid_map.line) + ":" + str(tuid_map.tuid))
                if tuid_map.line in lines_moved[file].keys():
                    old_moved_tuids[tuid_map.line] = tuid_map.tuid

            assert len(old_moved_tuids) == len(lines_moved[file].keys())

            print("\n\nNEW:")
            new_moved_tuids = {}
            for new_file, tmp_tuids in new_tuids:
                if new_file == file:
                    tmp_lines = [lines_moved[file][line] for line in lines_moved[file]]
                    for tuid_map in tmp_tuids:
                        print(str(tuid_map.line) + ":" + str(tuid_map.tuid))
                        if tuid_map.line in tmp_lines:
                            new_moved_tuids[tuid_map.line] = tuid_map.tuid
                    break

            assert len(new_moved_tuids) == len(old_moved_tuids)
            for line_moved in old_moved_tuids:
                old_tuid = old_moved_tuids[line_moved]
                new_line = lines_moved[file][line_moved]
                assert new_line in new_moved_tuids
                assert old_tuid == new_moved_tuids[new_line]
            completed += 1

        if file in lines_added:
            for new_file, tmp_tuids in new_tuids:
                new_file_tuids = []
                if new_file == file:
                    for tuid_map in tmp_tuids:
                        if tuid_map.line in lines_added[file]:
                            new_file_tuids.append(tuid_map.tuid)

                    # No tuids from the new should be in the old
                    # so this intersection should always be empty.
                    assert (
                        len(set(new_file_tuids) & set([t.tuid for t in old_file_tuids]))
                        <= 0
                    )
                    completed += 1

                    break
    assert completed == len(lines_moved.keys()) + len(lines_added.keys())


@pytest.mark.skip(reason="Never completes")
def test_daemon(service):
    from mo_threads import Signal

    temp_signal = Signal()

    # Run the daemon indefinitely to see if
    # we can update all known files to the latest
    # revisions. This can take a while though.
    service._daemon(temp_signal)
