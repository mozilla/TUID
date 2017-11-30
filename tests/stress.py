from tidservice import TIDService
import pytest
import sql
import json

@pytest.fixture
def service():
    return TIDService(conn=sql.Sql("resources/stress.db"))


def test_huge_file(service):
    files = []
    with open('resources/stressfiles.json', 'r') as f:
        files = json.load(f)
    count = 0
    total = len(files)
    old = service.grab_tids_from_files("/dom/base/",files,"6159e19a7c0f")
    new = service.grab_tids_from_files("/dom/base/",files,"698d4d2ed8c1")
    print("old:")
    for el in old:
        print("     "+el[0]+":"+str(len(el[1])))
    print("new:")
    for el in new:
        print("     "+el[0]+":"+str(len(el[1])))
