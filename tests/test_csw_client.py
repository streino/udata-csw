import pytest

from udata.utils import faker

from udata_csw.csw_client import CswClient

from factories import CswRecordFactory
from util import batched, csw_dc


# must be smaller than default page_size to avoid pagination in default tests
MAX_RECORDS = 5


@pytest.fixture
def client():
    return CswClient(faker.url(), skip_caps=True)


@pytest.fixture(params=range(0,MAX_RECORDS))
def data(request):
    return CswRecordFactory.create_batch(request.param)


def test_get_ids(client, data, rmock):
    rmock.post(client.url, status_code=200, text=csw_dc(data))

    ids = list(client.get_ids())

    assert len(ids) == len(data)
    assert ids == [d.id for d in data]


@pytest.mark.parametrize("page_size", [1, MAX_RECORDS-1, MAX_RECORDS, MAX_RECORDS+1,
                                       MAX_RECORDS*3-1, MAX_RECORDS*3, MAX_RECORDS*3+1])
def test_get_ids_pagination(client, data, rmock, page_size):
    matches = len(data)
    batches = list(batched(data, page_size)) if matches > 0 else [[]]  # at least one (empty) response
    rmock.post(client.url, [{'status_code': 200, 'text': csw_dc(d, matches)} for d in batches])

    ids = list(client.get_ids(page_size=page_size))

    assert len(rmock.request_history) == len(batches)
    assert len(ids) == len(data)
    assert ids == [d.id for d in data]


@pytest.mark.parametrize("limit", [1, MAX_RECORDS-1, MAX_RECORDS, MAX_RECORDS+1])
def test_get_ids_limit(client, data, rmock, limit):
    rmock.post(client.url, status_code=200, text=csw_dc(data[:limit], len(data)))

    ids = list(client.get_ids(limit=limit))

    assert len(ids) == min(len(data), limit)
    assert ids == [d.id for d in data[:limit]]
