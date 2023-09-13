from lxml import etree
from lxml.builder import ElementMaker
from lxml.etree import QName
from udata.utils import faker

from udata_csw.csw_client import ns


def nsmap(*namespaces):
    return {x: ns(x) for x in namespaces}


def csw_dc(records=[], matches=None):
    rsp = ElementMaker(namespace=ns('csw'), nsmap=nsmap('csw','xsi'))
    rec = ElementMaker(namespace=ns('csw'), nsmap=nsmap('ows','dc')) # 'geonet': 'http://www.fao.org/geonetwork'
    dc = ElementMaker(namespace=ns('dc'))
    ows = ElementMaker(namespace=ns('ows'))

    recs = [
        rec.BriefRecord(
            dc.identifier(r.id),
            dc.title(r.title),
            dc.type(r.type),
            ows.BoundingBox(
                ows.LowerCorner('{0:.2f} {1:.2f}'.format(*r.bbox.lower)),
                ows.UpperCorner('{0:.2f} {1:.2f}'.format(*r.bbox.upper)),
                {'crs': r.bbox.crs}
            )
        )
        for r in records
    ]

    tree = rsp.GetRecordsResponse(
        rsp.SearchStatus(timestamp=faker.date()),
        rsp.SearchResults(
            *recs,
            {'numberOfRecordsMatched': str(matches if matches else len(records)),
             'numberOfRecordsReturned': str(len(recs)),
             'elementSet': 'brief'}
        ),
        {QName(ns('xsi'), 'schemaLocation'): f"{ns('csw')} http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd"}
    )

    return to_xml(tree)


def csw_gmd(record):
    rsp = ElementMaker(namespace=ns('csw'), nsmap=nsmap('csw'))
    gmd = ElementMaker(namespace=ns('gmd'), nsmap=nsmap(
        'gmd', 'gco', 'srv', 'gmx', 'gts', 'gsr', 'gmi', 'gml32', 'xlink', 'geonet', 'xsi'))
    gco = ElementMaker(namespace=ns('gco'))

    tree = rsp.GetRecordByIdResponse(
        gmd.MD_Metadata(
            gmd.fileIdentifier(
                gco.CharacterString(record.id)
            ),
            gmd.hierarchyLevel(
                gmd.MD_ScopeCode(
                    codeList='http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode',
                    codeListValue=record.type
                )
            ),
            gmd.identificationInfo(
                gmd.MD_DataIdentification(
                    gmd.citation(
                        gmd.CI_Citation(
                            gmd.title(
                                gco.CharacterString(record.title)
                            )
                        )
                    )
                )
            )
        ),
        {QName(ns('xsi'), 'schemaLocation'): f"{ns('gmd')} http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd"}
    )

    return to_xml(tree)


def to_xml(tree):
    return etree.tostring(tree, encoding='UTF-8', xml_declaration=True, pretty_print=True).decode('utf-8')