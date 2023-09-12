import pytest

from conftest import RECORD, XML
from udata_csw.csw_client import CswClient

TEST_URL = 'http://www.example.com/csw'

@pytest.fixture
def client():
    return CswClient(TEST_URL, skip_caps=True)

@pytest.fixture
def data(n=2):
    return [RECORD() for r in range(n)]

def test_get_ids(client, data, rmock):
    rmock.post(TEST_URL, status_code=200, text=XML(data))

    ids = list(client.get_ids())

    history = rmock.request_history
    assert len(ids) == len(data)
    assert ids == [d.id for d in data]
