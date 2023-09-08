from owslib.csw import CatalogueServiceWeb


class CswClient(object):

    def __init__(self, url):
        self.csw = CatalogueServiceWeb(url)

    def get_ids(self, page=10):
        self.csw.getrecords2(maxrecords=page)
        return list(self.csw.records.keys())

    def get_record(self, id):
        self.csw.getrecordbyid([id])
        return self.csw.records[id]
