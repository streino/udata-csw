from lxml.etree import QName
from owslib.namespaces import Namespaces


_NS = {
    **Namespaces().get_namespaces(),
    'geonet': 'http://www.fao.org/geonetwork',
    'gsr': 'http://www.isotc211.org/2005/gsr'
}


def ns(namespace: str) -> str:
    return _NS[namespace]


def nsmap(*namespaces: str) -> dict[str, str]:
    return {x: ns(x) for x in namespaces}


def qname(tag):
    namespace, name = tag.split(':')
    return QName(ns(namespace), name)