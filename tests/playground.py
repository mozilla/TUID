# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from tidservice import TIDService
import unittest
config = None


class TestgrabTIDs(unittest.TestCase):

    def setUp(self):
        # MAKE AN INSTANCE OF THE SERVICE
        self.service = TIDService()

    # def test_grabTIDs(self):
    #    cursor = self.service.grab_tids("/taskcluster/ci/test/tests.yml","d1d44405008e")
    #    self.assertEqual(len(cursor),2291)


    # def test_applyChangesetsToRev(self):
    #     cursor = self.service._apply_changesets_to_rev("/taskcluster/ci/test/tests.yml","d1d44405008e","61340c7debf6")
    #     self.assertEqual(len(cursor),2291)
    #
    # def test_grabChangesets(self):
    #     cursor = self.service._grab_changeset("/taskcluster/ci/test/tests.yml","ad80a6d082c4")
    #     self.assertEqual(len(cursor),3)

    # def test_changesetsBetween(self):
    #     csets = self.service._changesets_between("/taskcluster/ci/test/tests.yml","ad80a6d082c4","ed32591c2394")
    #     self.assertEqual(len(csets),3)
    #
    # def test_addChangsetToRev(self):
    #     self.service._make_tids_from_changeset("/devtools/client/inspector/fonts/fonts.js", 'bb6f23916cb1')
    #     revision = self.service._grab_revision("/devtools/client/inspector/fonts/fonts.js","2559f86f67f6")
    #     file="/devtools/client/inspector/fonts/fonts.js"
    #     rev="bb6f23916cb1"
    #     cursor = self.service.conn.execute(self.service._grabTIDQuery,(file,rev,))
    #     cset=cursor.fetchall()
    #     result = self.service._add_changeset_to_rev(revision,cset)
    #     self.assertEqual(len(result),190)
    #
    #
    # def test_grabRevision(self):
    #     cursor = self.service._grab_revision("/taskcluster/ci/test/tests.yml", "c8dece9996b7")
    #     self.assertEqual(len(cursor),2201)



    # def test_grabTIDs(self):
    #     old = self.service.grab_tids("/testing/geckodriver/CONTRIBUTING.md","5ee7725a416c")
    #     new = self.service.grab_tids("/testing/geckodriver/CONTRIBUTING.md","65e2ad9a6e30")
    #
    #     print("old:",len(old))
    #     print("new:",len(new))
    #     self.assertEqual(len(old),97)
    #     self.assertEqual(len(new),232)

    # def test_makeTIDsFromRevision(self):
    #     self.service._make_tids_from_revision("/testing/geckodriver/CONTRIBUTING.md","5ee7725a416c")


    # def test_new_then_old(self):
    #     #delete database then run this test
    #     old = self.service.grab_tids("/testing/geckodriver/CONTRIBUTING.md", "6162f89a4838")
    #     new = self.service.grab_tids("/testing/geckodriver/CONTRIBUTING.md", "06b1a22c5e62")
    #     self.assertEqual(len(new),len(old))
    #     for i in range(0,len(old)):
    #         self.assertEqual(old[i],new[i])


    def test_tids_on_changed_file(self):
        # https://hg.mozilla.org/integration/mozilla-inbound/rev/a6fdd6eae583/taskcluster/ci/test/tests.yml
        old_lines = self.service.grab_tids( # 2205 lines
            "/taskcluster/ci/test/tests.yml","a6fdd6eae583"
        )

        # THE FILE HAS NOT CHANGED, SO WE EXPECT THE SAME SET OF TIDs AND LINES TO BE RETURNED
        # https://hg.mozilla.org/integration/mozilla-inbound/file/a0bd70eac827/taskcluster/ci/test/tests.yml
        same_lines = self.service.grab_tids( # 2201 lines

            "/taskcluster/ci/test/tests.yml","c8dece9996b7"
        )

        # assertAlmostEqual PERFORMS A STRUCURAL COMPARISION
        self.assertEqual(len(old_lines)-4,len(same_lines))


        # THE FILE HAS FOUR LINES REMOVED
        # https://hg.mozilla.org/integration/mozilla-inbound/rev/c8dece9996b7
        # https://hg.mozilla.org/integration/mozilla-inbound/file/c8dece9996b7/taskcluster/ci/test/tests.yml
        new_lines = self.service.grab_tids(
            "/taskcluster/ci/test/tests.yml","c8dece9996b7"
        )

        # EXPECTING
        self.assertEqual(len(new_lines), len(old_lines)-4)


    # def test_file_with_line_replacement(self):
    # https://hg.mozilla.org/mozilla-central/diff/e3f24e165618/python/mozbuild/mozbuild/action/test_archive.py
    #
    # def test_file_removal(self):
    # https://hg.mozilla.org/mozilla-central/diff/e3f24e165618/third_party/speedometer/InteractiveRunner.html
    #
    #
    # def test_multiple_files_changed(self):  #add some lines, remove some lines
    # https://hg.mozilla.org/mozilla-central/rev/84cb594525ad
    #
    # def test_create_files(self):  # plus make changes to existing, very good test!!
    # https://hg.mozilla.org/mozilla-central/rev/53967c00d476
    #
    # def test_distant_revisions(self):
    # before:  https://hg.mozilla.org/mozilla-central/file/c730f942ce30/python/mozbuild/mozbuild/action/test_archive.py
    # change:  https://hg.mozilla.org/mozilla-central/rev/e3f24e165618/python/mozbuild/mozbuild/action/test_archive.py
    # after:   https://hg.mozilla.org/mozilla-central/file/0d1e55d87931/python/mozbuild/mozbuild/action/test_archive.py

if __name__ == '__main__':
    unittest.main()
