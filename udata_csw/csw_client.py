import logging
import sys

from owslib.csw import CatalogueServiceWeb
from owslib.namespaces import Namespaces

log = logging.getLogger(__name__)


_NS = {
    **Namespaces().get_namespaces(),
    'geonet': 'http://www.fao.org/geonetwork',
    'gsr': 'http://www.isotc211.org/2005/gsr'
}


def ns(namespace):
    return _NS[namespace]


class CswClient(object):

    def __init__(self, url, **kwargs):
        self._url = url
        self._csw = CatalogueServiceWeb(url, **kwargs)

    @property
    def url(self):
        return self._url

    def get_ids(self, page_size=10, limit=None):
        limit = limit or sys.maxsize
        fetched = 0

        while True:
            maxrecords = min(page_size, limit - fetched)

            log.debug(f"Fetching {maxrecords} ids from position {fetched}")
            self._csw.getrecords2(startposition=fetched, maxrecords=maxrecords, esn='brief', outputschema=ns('csw'))

            if self._csw.results['returned'] == 0:
                break

            limit = min(limit, self._csw.results['matches'])

            ids = list(self._csw.records.keys())
            for i in ids:
                yield i

            fetched += len(ids)
            if fetched >= limit:
                break

    def get_record(self, id):
        self._csw.getrecordbyid(id=[id], esn='full', outputschema=ns('gmd'))
        return self._csw.records[id]
