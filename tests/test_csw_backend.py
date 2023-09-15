import pytest

from collections.abc import Iterable
from datetime import datetime
from lxml import etree
from os.path import join, dirname
from udata.core.organization.factories import OrganizationFactory
from udata.harvest import actions
from udata.harvest.tests.factories import HarvestSourceFactory
from udata.models import Dataset
from udata.utils import faker
from udata_csw.ows_util import qname, ns, nsmap

from factories import CswRecordFactory
from csw_util import csw_dc

pytestmark = [
    pytest.mark.options(PLUGINS=['csw']),
    pytest.mark.usefixtures('clean_db')
]

DATA_DIR = join(dirname(__file__), 'data')
OGC_FILTER_XPATH = '/csw:GetRecords/csw:Query/csw:Constraint/ogc:Filter'


def from_file(filename):
    f = join(DATA_DIR, f"{filename}.xml")
    return open(f).read()

def record_from_file(catalog_id, record_id):
    return from_file(f"{catalog_id}--{record_id}")

def map_datasets() -> dict[str, Dataset]:
    return {d.harvest.remote_id: d for d in Dataset.objects}


@pytest.fixture
def url():
    return faker.url()


@pytest.fixture(autouse=True)
def get_capabilities(rmock, url):
    u = url.rstrip('/')
    xml = from_file('_get_capabilities').replace('{{SERVICE_URL}}', u)
    rmock.get(u + '?service=csw&version=2.0.2&request=getcapabilities', text=xml)


@pytest.fixture(autouse=True)
def mock_harvest(request, rmock, url):
    harvest = request.node.get_closest_marker('harvest')
    if not harvest:
        return
    catalog = request.node.get_closest_marker('catalog')
    if not catalog:
        raise RuntimeError('Found pytest.mark.harvest but no pytest.mark.catalog')
    # mock client.get_ids request
    records = [CswRecordFactory(id=id) for id in harvest.args]
    rmock.post(url, text=csw_dc(records))
    # mock client.get_record request(s)
    for id in harvest.args:
        rmock.get(url + f"?request=GetRecordById&Id={id}", text=record_from_file(catalog.args[0], id))


@pytest.mark.catalog('sextant')
@pytest.mark.harvest('5b956140-1351-4ae6-8b95-bb424eab96ce', '7b032e0a-515d-4685-b546-73995bea6287')
def test_harvest(rmock, url):
    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org)

    actions.run(source.slug)
    source.reload()

    job = source.get_last_job()
    assert job.status == 'done'

    datasets = map_datasets()
    assert len(datasets) == 2

    d = datasets['5b956140-1351-4ae6-8b95-bb424eab96ce']
    assert d.title == '0-Distribution des espèces invertébrés benthiques observées par les campagnes halieutiques en Atlantique'
    assert d.description.startswith('Distribution des espèces invertébrés benthiques en Atlantique')
    assert d.description.endswith('NURSE (2000-2013), ORHAGO (2011-2015)')
    # assert set(d.tags) == set()  TODO
    assert d.harvest.created_at == datetime(2016, 9, 27)
    assert d.harvest.modified_at == None
    assert len(d.resources) == 8
    assert d.resources[0].url == 'http://atlasbenthal.ifremer.fr'
    assert d.resources[0].title == 'Atlas Benthal'
    assert d.resources[0].description == '''Accès à l'Atlas Benthal'''
    assert d.resources[1].url == 'http://doi.org/10.12770/5b956140-1351-4ae6-8b95-bb424eab96ce'
    assert d.resources[1].title == 'DOI du jeu de donnée'
    assert d.resources[1].description == 'DOI du jeu de donnée'
    # ...
    assert d.resources[7].url == 'https://sextant.ifremer.fr/services/wms/atlas_benthal'
    assert d.resources[7].title == 'IFR-ATLAS-BENTHAL-NumSysCPerm38401270-3241_HYALTUB_ATL'
    assert d.resources[7].description == 'Hyalinoecia tubicola'

    d: Dataset = datasets['7b032e0a-515d-4685-b546-73995bea6287']
    assert d.title == 'The GEBCO_2020 Grid - A continuous terrain model of the global oceans and land'
    assert d.description.startswith('The GEBCO_2020 Grid was released in May 2020')
    assert d.description.endswith('(https://www.gebco.net/data_and_products/gridded_bathymetry_data/gebco_2020/#compilations).')
    # assert set(d.tags) == set()  TODO
    assert d.harvest.created_at == datetime(2020, 1, 1)
    assert d.harvest.modified_at == None
    assert len(d.resources) == 7
    assert d.resources[0].url == 'https://seabed2030.org/'
    assert d.resources[0].title == 'SeaBed 2030 website'
    assert d.resources[0].description == None
    assert d.resources[1].url == 'https://www.gebco.net/data_and_products/historical_data_sets/#gebco_2020'
    assert d.resources[1].title == 'Download link on gebco website'
    assert d.resources[1].description == None
    # ...
    assert d.resources[6].url == 'https://emodnet.ec.europa.eu/geoviewer'
    assert d.resources[6].title == 'EMODnet viewer'
    assert d.resources[6].description == None


@pytest.mark.catalog('fake')
@pytest.mark.harvest('title-title-only', 'title-alternate-only', 'title-both')
def test_harvest_title(rmock, url):
    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org)

    actions.run(source.slug)
    source.reload()

    job = source.get_last_job()
    assert job.status == 'done'

    datasets = map_datasets()
    assert len(datasets) == 3

    assert datasets['title-title-only'].title == 'Title'
    assert datasets['title-alternate-only'].title == 'Alternate title'
    assert datasets['title-both'].title == 'Title'


@pytest.mark.catalog('fake')
@pytest.mark.harvest('keywords')
def test_harvest_keywords(rmock, url):
    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org)

    actions.run(source.slug)
    source.reload()

    job = source.get_last_job()
    assert job.status == 'done'

    datasets = map_datasets()
    assert len(datasets) == 1

    d = datasets['keywords']
    assert set(d.tags) == set([
        'keyword-a-1', 'keyword-a-2', 'keyword-b-1', 'keyword-b-2',
        'keyword-c-1', 'keyword-c-2', 'keyword-d-1', 'keyword-d-2'])
    assert set(d.extras['iso:keywords']) == set([
        'Keyword A 1', 'Keyword A 2', 'Keyword B 1', 'Keyword B 2'])
    assert set(d.extras['iso:keywords:theme']) == set([
        'Keyword C 1', 'Keyword C 2', 'Keyword D 1', 'Keyword D 2'])


@pytest.mark.catalog('fake')
@pytest.mark.harvest('dates-creation-only', 'dates-publication-only',
    'dates-creation-publication', 'dates-revision', 'dates-all')
def test_harvest_dates(rmock, url):
    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw', url=url, organization=org)

    actions.run(source.slug)
    source.reload()

    job = source.get_last_job()
    assert job.status == 'done'

    datasets = map_datasets()
    assert len(datasets) == 5

    assert datasets['dates-creation-only'].harvest.created_at == datetime(2020, 2, 3, 11, 12)
    assert datasets['dates-creation-only'].harvest.modified_at == None

    assert datasets['dates-publication-only'].harvest.created_at == datetime(2021, 4, 5, 13, 14)
    assert datasets['dates-publication-only'].harvest.modified_at == None

    assert datasets['dates-creation-publication'].harvest.created_at == datetime(2020, 2, 3, 11, 12)
    assert datasets['dates-creation-publication'].harvest.modified_at == None

    assert datasets['dates-revision'].harvest.created_at == None
    assert datasets['dates-revision'].harvest.modified_at == datetime(2022, 6, 7, 15, 16)

    assert datasets['dates-all'].harvest.created_at == datetime(2020, 2, 3, 11, 12)
    assert datasets['dates-all'].harvest.modified_at == datetime(2022, 6, 7, 15, 16)


def test_filter_simple(rmock, url):
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


def test_filter_complex(rmock, url):
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


def test_filter_multiple(rmock, url):
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
