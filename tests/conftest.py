import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--new-db",
        action="store",
        default="no",
        help="`yes` or `no` to use a new database"
    )

@pytest.fixture
def new_db(request):
    return request.config.getoption("new_db")
