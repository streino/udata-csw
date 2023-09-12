from lxml import etree
from lxml.builder import ElementMaker
from lxml.etree import QName
from itertools import islice
from owslib.namespaces import Namespaces
from udata.utils import faker


# From https://docs.python.org/3/library/itertools.html#itertools-recipes
def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


_NS = Namespaces().get_namespaces()


def csw_dc(data, matches=None):
    rsp = ElementMaker(namespace=_NS['csw'], nsmap={x: _NS[x] for x in ['csw','xsi']})
    rec = ElementMaker(namespace=_NS['csw'], nsmap={x: _NS[x] for x in ['ows','dc']}) # 'geonet': 'http://www.fao.org/geonetwork'
    dc = ElementMaker(namespace=_NS['dc'])
    ows = ElementMaker(namespace=_NS['ows'])

    records = [
        rec.BriefRecord(
            dc.identifier(d.id),
            dc.title(d.title),
            dc.type(d.type),
            ows.BoundingBox(
                ows.LowerCorner('{0:.2f} {1:.2f}'.format(*d.bbox.lower)),
                ows.UpperCorner('{0:.2f} {1:.2f}'.format(*d.bbox.upper)),
                {'crs': d.bbox.crs}
            )
        )
        for d in data
    ]

    tree = rsp.GetRecordsResponse(
        rsp.SearchStatus(timestamp=faker.date()),
        rsp.SearchResults(
            *records,
            {'numberOfRecordsMatched': str(matches if matches else len(data)),
             'numberOfRecordsReturned': str(len(records)),
             'elementSet': 'brief'}
        ),
        {QName(_NS['xsi'], 'schemaLocation'): f"{_NS['csw']} http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd"}
    )

    xml = etree.tostring(tree, encoding='UTF-8', xml_declaration=True, pretty_print=True).decode('utf-8')
    return xml
