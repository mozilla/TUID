from tidservice import TIDService
import unittest
config = None


class TestGetTIDS(unittest.TestCase):


    def setUp(self):
        # MAKE AN INSTANCE OF THE SERVICE
        self.service = TIDService()

    def test_makeTIDsFromChangeset(self):
        self.service._makeTIDsFromChangeset("/browser/extensions/formautofill/test/browser/head.js", '19b32a138d08')


    def test_grabTIDs(self):
        cursor = self.service.grabTIDs("/js/examples/jorendb.js", "e54a3d60d9fd5f48154f1013f8f63ecea1ebf06f")
        self.assertEqual(len(cursor),891)



    # def test_tids_on_changed_file(self):
    #     # https://hg.mozilla.org/integration/mozilla-inbound/rev/a6fdd6eae583/taskcluster/ci/test/tests.yml
    #     old_lines = self.service.get_tid(
    #         rev="a6fdd6eae583",
    #         file="taskcluster/ci/test/tests.yml"
    #     )
    #
    #     # THE FILE HAS NOT CHANGED, SO WE EXPECT THE SAME SET OF TIDs AND LINES TO BE RETURNED
    #     # https://hg.mozilla.org/integration/mozilla-inbound/file/a0bd70eac827/taskcluster/ci/test/tests.yml
    #     same_lines = self.service.get_tid(
    #         rev="c8dece9996b7",
    #         file="taskcluster/ci/test/tests.yml"
    #     )
    #
    #     # assertAlmostEqual PERFORMS A STRUCURAL COMPARISION
    #     self.assertAlmostEqual(old_lines, same_lines)
    #     self.assertAlmostEqual(same_lines, old_lines)
    #
    #
    #     # THE FILE HAS FOUR LINES REMOVED
    #     # https://hg.mozilla.org/integration/mozilla-inbound/rev/c8dece9996b7
    #     # https://hg.mozilla.org/integration/mozilla-inbound/file/c8dece9996b7/taskcluster/ci/test/tests.yml
    #     new_lines = self.service.get_tid(
    #         rev="c8dece9996b7",
    #         file="taskcluster/ci/test/tests.yml"
    #     )
    #
    #     # EXPECTING
    #     self.assertEqual(len(new_lines), len(old_lines)-4)

if __name__ == '__main__':
    unittest.main()