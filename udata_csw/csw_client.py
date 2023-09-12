import logging
from owslib.csw import CatalogueServiceWeb

log = logging.getLogger(__name__)


class CswClient(object):

    def __init__(self, url, **kwargs):
        self.csw = CatalogueServiceWeb(url, **kwargs)

    def get_ids(self, limit=0, page_size=10):
        fetched = 0
        while True:
            maxrecords = min(limit - fetched, page_size) if limit else page_size

            log.debug(f"Fetching {maxrecords} ids from position {fetched}")
            self.csw.getrecords2(startposition=fetched, maxrecords=maxrecords, esn='brief')

            ids = list(self.csw.records.keys())
            if len(ids) == 0:
                break

            for i in ids:
                yield i

            fetched += len(ids)
            if fetched >= limit:
                break

    def get_record(self, id):
        self.csw.getrecordbyid([id])
        return self.csw.records[id]
