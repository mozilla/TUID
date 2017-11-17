from tidservice import TIDService
import pytest

config = None


class TestgrabTIDs:
    @classmethod
    def setup_class(self):
        self.service = TIDService()

    def test_new_then_old(self):
        #delete database then run this test
        old = self.service.grab_tids("/testing/geckodriver/CONTRIBUTING.md", "6162f89a4838")
        new = self.service.grab_tids("/testing/geckodriver/CONTRIBUTING.md", "06b1a22c5e62")
        assert len(new)==len(old)
        for i in range(0,len(old)):
            assert old[i]==new[i]

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
        assert len(old_lines)-4==len(same_lines)


        # THE FILE HAS FOUR LINES REMOVED
        # https://hg.mozilla.org/integration/mozilla-inbound/rev/c8dece9996b7
        # https://hg.mozilla.org/integration/mozilla-inbound/file/c8dece9996b7/taskcluster/ci/test/tests.yml
        new_lines = self.service.grab_tids(
            "/taskcluster/ci/test/tests.yml","c8dece9996b7"
        )

        # EXPECTING
        assert len(new_lines)== len(old_lines)-4

    def test_remove_file(self):
        assert 0==len(self.service.grab_tids("/third_party/speedometer/InteractiveRunner.html","e3f24e165618"))

    def test_generic_1(self):
        old = self.service.grab_tids("/gfx/ipc/GPUParent.cpp","a5a2ae162869")
        new = self.service.grab_tids("/gfx/ipc/GPUParent.cpp","3acb30b37718")
        assert len(old)==467
        assert len(new)==476
        for i in range(1,207):
            assert old[i]==new[i]

    def test_file_with_line_replacement(self):
        new = self.service.grab_tids("/python/mozbuild/mozbuild/action/test_archive.py","e3f24e165618")
        old = self.service.grab_tids("/python/mozbuild/mozbuild/action/test_archive.py","c730f942ce30")
        assert 653==len(new)
        assert 653==len(old)
        for i in range(0,600):
            if i==374 or i==376:
                assert old[i] != new[i]
            else:
                assert old[i]==new[i]

    def test_distant_rev(self):
        old = self.service.grab_tids("/python/mozbuild/mozbuild/action/test_archive.py","e3f24e165618")
        new = self.service.grab_tids("/python/mozbuild/mozbuild/action/test_archive.py","0d1e55d87931")
        assert len(old)==653
        assert len(new)==653
        for i in range(0,653):
            assert new[i]==old[i]

    def test_new_file(self):
        rev = self.service.grab_tids("/media/audioipc/server/src/lib.rs","a39241b3e7b1")
        assert len(rev)==636
