import logging
from owslib.csw import CatalogueServiceWeb

log = logging.getLogger(__name__)


class CswClient(object):

    def __init__(self, url, **kwargs):
        self._url = url
        self._csw = CatalogueServiceWeb(url, **kwargs)

    @property
    def url(self):
        return self._url

    def get_ids(self, limit=0, page_size=10):
        fetched = 0
        while True:
            maxrecords = min(limit - fetched, page_size) if limit else page_size

            log.debug(f"Fetching {maxrecords} ids from position {fetched}")
            self._csw.getrecords2(startposition=fetched, maxrecords=maxrecords, esn='brief')

            if self._csw.results['returned'] == 0:
                break

            limit = min(limit, self._csw.results['matches'])

            # getrecords2 doesn't respect maxrecords param
            ids = list(self._csw.records.keys())[:maxrecords]
            for i in ids:
                yield i

            fetched += len(ids)
            if fetched >= limit:
                break

    def get_record(self, id):
        self._csw.getrecordbyid([id])
        return self._csw.records[id]
