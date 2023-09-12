import pytest

from udata.utils import faker

from udata_csw.csw_client import CswClient

from factories import CswRecordFactory
from util import to_xml

@pytest.fixture
def client():
    return CswClient(faker.url(), skip_caps=True)

@pytest.fixture
def data(n=2):
    return CswRecordFactory.create_batch(n)

def test_get_ids(client, data, rmock):
    rmock.post(client.url, status_code=200, text=to_xml(data))

    ids = list(client.get_ids())

    history = rmock.request_history
    assert len(ids) == len(data)
    assert ids == [d.id for d in data]
