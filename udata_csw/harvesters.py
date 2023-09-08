import logging
from owslib.csw import CatalogueServiceWeb

from udata.harvest.backends.base import BaseBackend
from udata.models import Resource
from udata.utils import faker

log = logging.getLogger(__name__)


class CswBackend(BaseBackend):
    display_name = 'csw'

    def initialize(self):
        self.csw = CatalogueServiceWeb(self.source.url)
        self.csw.getrecords2(maxrecords=10)
        for identifier in self.csw.records.keys():
            self.add_item(identifier)

    def process(self, item):
        dataset = self.get_dataset(item.remote_id)

        # Here you comes your implementation. You should :
        # - fetch the remote dataset (if necessary)
        # - validate the fetched payload
        # - map its content to the dataset fields
        # - store extra significant data in the `extra` attribute
        # - map resources data

        self.csw.getrecordbyid(id=[item.remote_id])
        r = self.csw.records[item.remote_id]

        dataset.title = r.title
        dataset.description = r.abstract

        # Needs at least one resource to be indexed
        dataset.resources.clear()
        dataset.resources.append(Resource(
            title=faker.sentence(),
            description=faker.text(),
            url=faker.url(),
            filetype='remote',
            mime=faker.mime_type(category='text'),
            format=faker.file_extension(category='text'),
            filesize=faker.pyint()
        ))

        return dataset
