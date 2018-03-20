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
import json
import os
from mo_logs import Log
from mo_times import Timer

from pyLibrary.env import http

from tuid import sql
from tuid.service import TUIDService


@pytest.fixture
def service(config, new_db):
    if new_db == 'yes':
        return TUIDService(conn=sql.Sql(":memory:"), kwargs=config.tuid)
    elif new_db == 'no':
        return TUIDService(conn=sql.Sql("resources/test.db"), kwargs=config.tuid)
    else:
        Log.error("expecting 'yes' or 'no'")


def test_new_then_old(service):
    # delete database then run this test
    old = service.get_tuids("/testing/geckodriver/CONTRIBUTING.md", "6162f89a4838")
    new = service.get_tuids("/testing/geckodriver/CONTRIBUTING.md", "06b1a22c5e62")
    print(old)
    print(new)
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
    old_lines = service.get_tuids(     # 2205 lines
        "/taskcluster/ci/test/tests.yml", "a6fdd6eae583"
    )
    new_lines = service.get_tuids(     # 2201 lines
        "/taskcluster/ci/test/tests.yml", "c8dece9996b7"
    )

    # EXPECTING
    assert len(new_lines) == len(old_lines) - 4


def test_remove_file(service):
    entries = service.get_tuids("/third_party/speedometer/InteractiveRunner.html", "e3f24e165618")
    assert 0 == len(entries)


def test_generic_1(service):
    old = service.get_tuids("/gfx/ipc/GPUParent.cpp", "a5a2ae162869")
    new = service.get_tuids("/gfx/ipc/GPUParent.cpp", "3acb30b37718")
    assert len(old) == 467
    assert len(new) == 476
    for i in range(1, 207):
        assert old[i] == new[i]


def test_500_file(service):
    # this file has no history (nore should it have tuids)
    # calling hg will return a 500 error
    tuids = service.get_tuids("/browser/tools/mozscreenshots/mozscreenshots/extension/lib/robot_upperleft.png", "d3ed36f4fb7a")
    assert len(tuids) == 0


def test_file_with_line_replacement(service):
    new = service.get_tuids("/python/mozbuild/mozbuild/action/test_archive.py", "e3f24e165618")
    old = service.get_tuids("/python/mozbuild/mozbuild/action/test_archive.py", "c730f942ce30")
    assert 653 == len(new)
    assert 653 == len(old)
    for i in range(0, 600):
        if i == 374 or i == 376:
            assert old[i] != new[i]
        else:
            assert old[i] == new[i]


def test_distant_rev(service):
    old = service.get_tuids("/python/mozbuild/mozbuild/action/test_archive.py", "e3f24e165618")
    new = service.get_tuids("/python/mozbuild/mozbuild/action/test_archive.py", "0d1e55d87931")
    assert len(old) == 653
    assert len(new) == 653
    for i in range(0, 653):
        assert new[i] == old[i]


def test_new_file(service):
    rev = service.get_tuids("/media/audioipc/server/src/lib.rs", "a39241b3e7b1")
    assert len(rev) == 636

def test_bad_date_file(service):
    # The following changeset is dated February 14, 2018 but was pushed to mozilla-central
    # on March 8, 2018. It modifies the file: dom/media/MediaManager.cpp
    # https://hg.mozilla.org/mozilla-central/rev/07fad8b0b417d9ae8580f23d697172a3735b546b
    change_one = service.get_tuids("dom/media/MediaManager.cpp", "07fad8b0b417d9ae8580f23d697172a3735b546b")

    # Insert a change in between these dates to throw us off.
    # https://hg.mozilla.org/mozilla-central/rev/0451fe123f5b
    change_two = service.get_tuids("dom/media/MediaManager.cpp", "0451fe123f5b")

    # Add the file just before these changes.
    # https://hg.mozilla.org/mozilla-central/rev/42c6ec43f782
    change_prev = service.get_tuids("dom/media/MediaManager.cpp", "42c6ec43f782")

    # First revision (07fad8b0b417d9ae8580f23d697172a3735b546b) should be equal to the
    # tuids for it's child dated March 6.
    # https://hg.mozilla.org/mozilla-central/rev/7a6bc227dc03
    earliest_rev = service.get_tuids("dom/media/MediaManager.cpp", "7a6bc227dc03")

    assert len(change_one) == len(earliest_rev)
    for i in range(0, len(change_one)):
        assert change_one[i] == earliest_rev[i]

def test_multi_parent_child_changes(service):
    # For this file: toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp
    # Multi-parent, multi-child change: https://hg.mozilla.org/mozilla-central/log/0ef34a9ec4fbfccd03ee0cfb26b182c03e28133a
    earliest_rev = service.get_tuids("toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp", "0ef34a9ec4fbfccd03ee0cfb26b182c03e28133a")

    # A past revision: https://hg.mozilla.org/mozilla-central/rev/bb6db24a20dd
    past_rev =  service.get_tuids("toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp", "bb6db24a20dd")

    # Check it on the child which doesn't modify it: https://hg.mozilla.org/mozilla-central/rev/39717163c6c9
    next_rev = service.get_tuids("toolkit/components/printingui/ipc/PrintProgressDialogParent.cpp", "39717163c6c9")

    assert len(earliest_rev) == len(next_rev)
    for i in range(0, len(earliest_rev)):
        assert next_rev[i] == earliest_rev[i]

def test_get_tuids_from_revision(service):
    tuids = service.get_tuids_from_revision("a6fdd6eae583")
    assert tuids != None

@pytest.mark.skipif(os.environ.get('TRAVIS'), reason="Too expensive on travis.")
def test_many_files_one_revision(service):
    with open('resources/stressfiles.json', 'r') as f:
        files = json.load(f)
    test_file = ["widget/cocoa/nsCocoaWindow.mm"]
    first_front = "739c536d2cd6"
    test_rev = "159e1105bdc7"
    dir = "/dom/base/"
    tmp = [dir + f for f in files]
    Log.note("Total files: {{total}}", total=str(len(test_file)))

    test_file.extend(tmp[1:10])
    old = service.get_tuids_from_files(test_file,first_front)
    print("old:")
    for el in old:
        print(el[0])
        print("     "+el[0]+":"+str(len(el[1])))

    new = service.get_tuids_from_files(test_file,test_rev)
    print("new:")
    for el in new:
        print("     "+el[0]+":"+str(len(el[1])))


@pytest.mark.skipif(os.environ.get('TRAVIS'), reason="Too expensive on travis.")
def test_one_addition_many_files(service):
    with open('resources/stressfiles.json', 'r') as f:
        files = json.load(f)
    test_file = ["widget/cocoa/nsCocoaWindow.mm"]
    test_rev = "58eb13b394f4"
    dir = "/dom/base/"
    tmp = [dir + f for f in files]
    Log.note("Total files: {{total}}", total=str(len(test_file)))

    test_file.extend(tmp[1:10])
    new = service.get_tuids_from_files(test_file,test_rev)
    print("new:")
    for el in new:
        print("     "+el[0]+":"+str(len(el[1])))


@pytest.mark.skipif(os.environ.get('TRAVIS'), reason="Too expensive on travis.")
def test_one_http_call_required(service):
    files =[
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
        "/tools/lint/eslint/eslint-plugin-mozilla/lib/rules/no-cpows-in-tests.js",  # DOES NOT EXIST IN NEWER REVISION
        "/tools/lint/eslint/eslint-plugin-mozilla/tests/no-cpows-in-tests.js"  # DOES NOT EXIST IN NEWER REVISION
    ]

    # SETUP
    Log.note("Number of files to process: {{flen}}", flen=len(files))
    service.get_tuids_from_files(['/dom/base/Link.cpp']+files, "d63ed14ed622")

    # THIS NEXT CALL SHOULD BE FAST, DESPITE THE LACK OF LOCAL CACHE
    start = http.request_count
    timer = Timer("get next revision")
    with timer:
        service.get_tuids_from_files(['/dom/base/Link.cpp']+files, "14dc6342ec50")
    num_http_calls = http.request_count - start

    assert num_http_calls <= 2
    assert timer.duration.seconds < 30
    # TODO: ALSO VERIFY THE TUIDS ARE MATCH AS EXPECTED (AND NOW-MISSING FILES HAVE ZERO TUIDS)


def test_long_file(service):
    timer = Timer("test", silent=True)

    with timer:
        service.get_tuids(
            file="gfx/angle/checkout/src/libANGLE/formatutils.cpp",
            revision="29dcc9cb77c3"
        )

    assert timer.duration.seconds < 30
