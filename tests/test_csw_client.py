import pytest

from lxml import etree
from udata.utils import faker

from udata_csw.csw_client import CswClient
from udata_csw.ows_util import ns, nsmap

from csw_util import csw_dc, csw_gmd
from factories import CswRecordFactory
from util import batched


MAX_RECORDS = 5  # less than default pagination - 1
RECORDS_VALUES = range(0, MAX_RECORDS)
PAGINATION_VALUES = list(range(1, MAX_RECORDS+2)) + [MAX_RECORDS*3-1, MAX_RECORDS*3, MAX_RECORDS*3+1]
LIMIT_VALUES = [0, 1, MAX_RECORDS-1, MAX_RECORDS, MAX_RECORDS+1]


@pytest.fixture
def client():
    return CswClient(faker.url(), skip_caps=True)


@pytest.fixture
def record(request):
    return CswRecordFactory()


@pytest.fixture(params=RECORDS_VALUES)
def records(request):
    return CswRecordFactory.create_batch(request.param)


def test_get_ids_request(client, rmock):
    rmock.post(client.url, text=csw_dc())

    list(client.get_ids())

    assert len(rmock.request_history) == 1
    r = etree.fromstring(rmock.request_history[0].text)
    assert r.xpath('/csw:GetRecords/@outputSchema', namespaces=nsmap('csw'))[0] == ns('csw')
    assert r.xpath('/csw:GetRecords/@resultType', namespaces=nsmap('csw'))[0] == 'results'
    assert r.xpath('/csw:GetRecords/@maxRecords', namespaces=nsmap('csw'))[0] == '10'
    assert r.xpath('/csw:GetRecords/csw:Query/@typeNames', namespaces=nsmap('csw'))[0] == 'csw:Record'
    assert r.xpath('/csw:GetRecords/csw:Query/csw:ElementSetName/text()', namespaces=nsmap('csw'))[0] == 'brief'


def test_get_ids_request_pagination(client, rmock):
    rmock.post(client.url, text=csw_dc())

    list(client.get_ids(page_size=5))

    assert len(rmock.request_history) == 1
    r = etree.fromstring(rmock.request_history[0].text)
    assert r.xpath('/csw:GetRecords/@maxRecords', namespaces=nsmap('csw'))[0] == '5'


def test_get_ids_request_limit(client, rmock):
    rmock.post(client.url, text=csw_dc())

    list(client.get_ids(limit=3, page_size=10))
    list(client.get_ids(limit=10, page_size=7))

    # limit should only affect maxRecords when limit < page_size
    assert len(rmock.request_history) == 2
    r0 = etree.fromstring(rmock.request_history[0].text)
    assert r0.xpath('/csw:GetRecords/@maxRecords', namespaces=nsmap('csw'))[0] == '3'
    r1 = etree.fromstring(rmock.request_history[1].text)
    assert r1.xpath('/csw:GetRecords/@maxRecords', namespaces=nsmap('csw'))[0] == '7'


@pytest.mark.parametrize("limit", LIMIT_VALUES)
@pytest.mark.parametrize("page_size", PAGINATION_VALUES)
def test_get_ids(rmock, client, records, page_size, limit):
    if limit:
        records = records[:limit]
    matches = len(records)
    batches = list(batched(records, page_size)) if matches > 0 else [[]]  # at least one (empty) response

    rmock.post(client.url, [{'text': csw_dc(b, matches)} for b in batches])

    ids = list(client.get_ids(page_size=page_size, limit=limit))

    assert len(rmock.request_history) == len(batches)
    for i, b in enumerate(batches):
        r = etree.fromstring(rmock.request_history[i].text)
        start_pos = r.xpath('/csw:GetRecords/@startPosition', namespaces=nsmap('csw'))
        max_records = r.xpath('/csw:GetRecords/@maxRecords', namespaces=nsmap('csw'))

        if i == 0:
            assert len(start_pos) == 0  # no attribute when startPosition=0
            if limit > 0:
                assert int(max_records[0]) == min(limit, page_size)
            else:
                assert int(max_records[0]) == page_size
        else:
            assert int(start_pos[0]) == i * page_size
            if i < len(batches)-1:
                assert int(max_records[0]) == page_size
            else:
                assert int(max_records[0]) == len(b)

    assert len(ids) == len(records)
    assert ids == [r.id for r in records]


def test_get_record(rmock, client, record):
    rmock.get(client.url, text=csw_gmd(record))

    r = client.get_record(record.id)

    assert r.identifier == record.id
    assert r.identification[0].title == record.title
