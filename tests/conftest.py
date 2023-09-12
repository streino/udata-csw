import pytest

from dataclasses import dataclass
from lxml import etree
from lxml.etree import QName
from lxml.builder import ElementMaker
from owslib.namespaces import Namespaces

from udata import settings
from udata.app import create_app
from udata.utils import faker

class CswSettings(settings.Testing):
    PLUGINS = ['csw']

@pytest.fixture
def app():
    app = create_app(settings.Defaults, override=CswSettings)
    return app

@dataclass
class Bbox:
    lower: str
    upper: str
    crs: str

@dataclass
class Record:
    id: str
    title: str
    type: str
    bbox: Bbox

def RECORD():
    return Record(
        id = faker.uuid4(),
        title = faker.sentence(),
        type = 'dataset',
        bbox = Bbox(
            lower = f"{faker.numerify('##.###')} {faker.numerify('##.###')}",
            upper = f"{faker.numerify('##.###')} {faker.numerify('##.###')}",
            crs = 'urn:ogc:def:crs:EPSG:6.6:4326'
        )
    )

def XML(data, start=None, end=None):
    ns = Namespaces().get_namespaces()

    rsp = ElementMaker(namespace=ns['csw'], nsmap={x: ns[x] for x in ['csw','xsi']})
    rec = ElementMaker(namespace=ns['csw'], nsmap={x: ns[x] for x in ['ows','dc']}) # 'geonet': 'http://www.fao.org/geonetwork'
    dc = ElementMaker(namespace=ns['dc'])
    ows = ElementMaker(namespace=ns['ows'])

    records = [
        rec.BriefRecord(
            dc.identifier(d.id),
            dc.title(d.title),
            dc.type(d.type),
            ows.BoundingBox(
                ows.LowerCorner(d.bbox.lower),
                ows.UpperCorner(d.bbox.upper),
                {'crs': d.bbox.crs}
            )
        )
        for d in data[start:end]
    ]

    tree = rsp.GetRecordsResponse(
        rsp.SearchStatus(timestamp=faker.date()),
        rsp.SearchResults(
            *records,
            {'numberOfRecordsMatched': str(len(records)),
             'numberOfRecordsReturned': str(len(records)),
             'elementSet': 'brief'}
        ),
        {QName(ns['xsi'], 'schemaLocation'): f"{ns['csw']} http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd"}
    )

    xml = etree.tostring(tree, encoding='UTF-8', xml_declaration=True, pretty_print=True).decode('utf-8')
    return xml