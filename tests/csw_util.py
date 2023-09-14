from collections.abc import Iterable
from lxml import etree
from lxml.builder import ElementMaker
from lxml.etree import Element, QName
from udata.utils import faker
from udata_csw.ows_util import qname, ns, nsmap
from typing import Union

from factories import CswRecord
from util import as_iterable


def csw_dc(records: Union[CswRecord, Iterable[CswRecord]] = [], matches: int = None) -> str:
    records = as_iterable(records)

    rsp = ElementMaker(namespace=ns('csw'), nsmap=nsmap('csw','xsi'))
    rec = ElementMaker(namespace=ns('csw'), nsmap=nsmap('ows','dc')) # 'geonet': 'http://www.fao.org/geonetwork'
    dc = ElementMaker(namespace=ns('dc'))

    recs = [
        rec.BriefRecord(
            dc.identifier(r.id),
            dc.type(r.type)
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
        {qname('xsi:schemaLocation'): f"{ns('csw')} http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd"}
    )

    return to_xml(tree)


def csw_gmd(record: CswRecord) -> str:
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
        {qname('xsi:schemaLocation'): f"{ns('gmd')} http://schemas.opengis.net/csw/2.0.2/profiles/apiso/1.0.0/apiso.xsd"}
    )

    return to_xml(tree)


def to_xml(tree: Element) -> str:
    return (
        etree
        .tostring(tree, encoding='UTF-8', xml_declaration=True, pretty_print=True)
        .decode('utf-8')
    )
