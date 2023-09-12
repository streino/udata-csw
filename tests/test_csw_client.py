import pytest

from udata.utils import faker

from udata_csw.csw_client import CswClient

from factories import CswRecordFactory
from util import to_xml

MAX_RECORDS = 5

@pytest.fixture
def client():
    return CswClient(faker.url(), skip_caps=True)

@pytest.fixture(params=range(0,MAX_RECORDS))
def data(request):
    return CswRecordFactory.create_batch(request.param)


def test_get_ids(client, data, rmock):
    rmock.post(client.url, status_code=200, text=to_xml(data))

    ids = list(client.get_ids())

    history = rmock.request_history
    assert len(ids) == len(data)
    assert ids == [d.id for d in data]

@pytest.mark.parametrize("page_size", range(1,MAX_RECORDS+1))
def test_get_ids_pagination(client, data, rmock, page_size):
    rmock.post(client.url, status_code=200, text=to_xml(data))

    ids = list(client.get_ids(page_size=page_size))

    assert len(ids) == len(data)
    assert ids == [d.id for d in data]
