import pytest

from lxml import etree
from lxml.etree import QName
from os.path import join, dirname
from udata.core.organization.factories import OrganizationFactory
from udata.harvest import actions
from udata.harvest.tests.factories import HarvestSourceFactory
from udata.models import Dataset
from udata.utils import faker

from udata_csw.csw_client import ns
from factories import CswRecordFactory
from csw_util import csw_dc, nsmap

DATA_DIR = join(dirname(__file__), 'data')
OGC_FILTER_XPATH = '/csw:GetRecords/csw:Query/csw:Constraint/ogc:Filter'

pytestmark = pytest.mark.usefixtures('clean_db')


def from_file(filename):
    f = join(DATA_DIR, f"{filename}.xml")
    return open(f).read()

def record_from_file(catalog_id, record_id):
    return from_file(f"{catalog_id}--{record_id}")

def ogc_qname(name):
    return QName(ns('ogc'), name)

def qname(tag):
    namespace, name = tag.split(':')
    return QName(ns(namespace), name)


@pytest.fixture
def url():
    return faker.url()


@pytest.fixture(autouse=True)
def get_capabilities(rmock, url):
    u = url.rstrip('/')
    xml = from_file('_get_capabilities').replace('{{SERVICE_URL}}', u)
    rmock.get(u + '?service=csw&version=2.0.2&request=getcapabilities', text=xml)


def test_simple(rmock, url):
    catalog_id = 'sextant'
    record_id = '7b032e0a-515d-4685-b546-73995bea6287'

    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org)

    rmock.post(source.url, text=csw_dc(CswRecordFactory(id=record_id)))
    rmock.get(source.url, text=record_from_file(catalog_id, record_id))

    actions.run(source.slug)
    source.reload()

    job = source.get_last_job()
    assert len(job.items) == 1
    assert job.items[0].remote_id == record_id

    datasets = {d.harvest.remote_id: d for d in Dataset.objects}
    assert len(datasets) == 1

    d: Dataset = datasets[record_id]
    assert d.harvest.remote_id == record_id
    assert d.title == 'The GEBCO_2020 Grid - A continuous terrain model of the global oceans and land'

    # TODO: test resulting datasets, mappings, conditions...


def test_simple_filter(rmock, url):
    config = {
        'filters': [
            {
                'key': 'ogc_filters',
                'value': 'PropertyIsEqualTo("dc:type", "dataset")'
            }
        ]
    }

    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org, config=config)

    rmock.post(source.url, text=csw_dc())

    actions.run(source.slug)

    f = (
        etree.fromstring(rmock.last_request.text)
        .xpath(OGC_FILTER_XPATH, namespaces=nsmap('csw', 'ogc'))[0]
    )
    assert len(f) == 1
    e = f[0]
    assert e.tag == qname('ogc:PropertyIsEqualTo')
    assert len(e) == 2
    assert e[0].tag == qname('ogc:PropertyName')
    assert e[0].text == 'dc:type'
    assert e[1].tag == qname('ogc:Literal')
    assert e[1].text == 'dataset'


def test_complex_filter(rmock, url):
    config = {
        'filters': [
            {
                'key': 'ogc_filters',
                'value': 'And([PropertyIsEqualTo("dc:type", "dataset"), PropertyIsLike("OrganisationName", "%DDTM%56%")])'
            }
        ]
    }

    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org, config=config)

    rmock.post(source.url, text=csw_dc())

    actions.run(source.slug)

    f = (
        etree.fromstring(rmock.last_request.text)
        .xpath(OGC_FILTER_XPATH, namespaces=nsmap('csw', 'ogc'))[0]
    )
    assert len(f) == 1
    e = f[0]
    assert e.tag == qname('ogc:And')
    assert len(e) == 2
    e0 = e[0]
    assert e0.tag == qname('ogc:PropertyIsEqualTo')
    assert len(e0) == 2
    assert e0[0].tag == qname('ogc:PropertyName')
    assert e0[0].text == 'dc:type'
    assert e0[1].tag == qname('ogc:Literal')
    assert e0[1].text == 'dataset'
    e1 = e[1]
    assert e1.tag == qname('ogc:PropertyIsLike')
    assert len(e1) == 2
    assert e1[0].tag == qname('ogc:PropertyName')
    assert e1[0].text == 'OrganisationName'
    assert e1[1].tag == qname('ogc:Literal')
    assert e1[1].text == '%DDTM%56%'


def test_multiple_filters(rmock, url):
    config = {
        'filters': [
            {
                'key': 'ogc_filters',
                'value': 'PropertyIsEqualTo("dc:type", "dataset")'
            },
            {
                'key': 'ogc_filters',
                'value': 'PropertyIsLike("OrganisationName", "%DDTM%56%")'
            }
        ]
    }

    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org, config=config)

    rmock.post(source.url, text=csw_dc())

    actions.run(source.slug)

    f = (
        etree.fromstring(rmock.last_request.text)
        .xpath(OGC_FILTER_XPATH, namespaces=nsmap('csw', 'ogc'))[0]
    )
    assert len(f) == 1
    e = f[0]
    assert e.tag == qname('ogc:Or')  # multiple constraints are OR'ed
    assert len(e) == 2
    e0 = e[0]
    assert e0.tag == qname('ogc:PropertyIsEqualTo')
    assert len(e0) == 2
    assert e0[0].tag == qname('ogc:PropertyName')
    assert e0[0].text == 'dc:type'
    assert e0[1].tag == qname('ogc:Literal')
    assert e0[1].text == 'dataset'
    e1 = e[1]
    assert e1.tag == qname('ogc:PropertyIsLike')
    assert len(e1) == 2
    assert e1[0].tag == qname('ogc:PropertyName')
    assert e1[0].text == 'OrganisationName'
    assert e1[1].tag == qname('ogc:Literal')
    assert e1[1].text == '%DDTM%56%'
