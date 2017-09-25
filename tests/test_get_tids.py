import unittest

from mo_logs import constants, startup, Log
from mo_testing.fuzzytestcase import FuzzyTestCase

config = None


class TestGetTIDS(FuzzyTestCase):


    def setUp(self):
        # MAKE AN INSTANCE OF THE SERVICE
        self.service = TIDService(config)

    def test_tids_on_changed_file(self):
        # https://hg.mozilla.org/integration/mozilla-inbound/rev/a6fdd6eae583/taskcluster/ci/test/tests.yml
        old_lines = self.service.get_tid(
            rev="a6fdd6eae583",
            file="taskcluster/ci/test/tests.yml"
        )

        # THE FILE HAS NOT CHANGED, SO WE EXPECT THE SAME SET OF TIDs AND LINES TO BE RETURNED
        # https://hg.mozilla.org/integration/mozilla-inbound/file/a0bd70eac827/taskcluster/ci/test/tests.yml
        same_lines = self.service.get_tid(
            rev="c8dece9996b7",
            file="taskcluster/ci/test/tests.yml"
        )

        # assertAlmostEqual PERFORMS A STRUCURAL COMPARISION
        self.assertAlmostEqual(old_lines, same_lines)
        self.assertAlmostEqual(same_lines, old_lines)


        # THE FILE HAS FOUR LINES REMOVED
        # https://hg.mozilla.org/integration/mozilla-inbound/rev/c8dece9996b7
        # https://hg.mozilla.org/integration/mozilla-inbound/file/c8dece9996b7/taskcluster/ci/test/tests.yml
        new_lines = self.service.get_tid(
            rev="c8dece9996b7",
            file="taskcluster/ci/test/tests.yml"
        )

        # EXPECTING
        self.assertEqual(len(new_lines), len(old_lines)-4)


try:
    # WILL READ config.json WHICH WILL BE USED TO CALL THE TID SERVICE CONSTRUCTOR
    config = startup.read_settings()
    constants.set(config.constants)
    Log.start(config.debug)

except Exception as e:
    Log.error("Problem with etl", e)
