import logging

from udata.harvest.backends.base import BaseBackend
from udata.harvest.models import HarvestItem
from udata.models import Dataset, Resource
from udata.utils import faker

from .csw_client import CswClient

log = logging.getLogger(__name__)


MAX_RECORDS = 25  # FIXME: testing only


class CswBackend(BaseBackend):
    display_name = 'csw'

    def initialize(self):
        self.csw = CswClient(self.source.url)
        ids = self.csw.get_ids(limit=MAX_RECORDS)
        for id in ids:
            self.add_item(id)

    def process(self, item: HarvestItem):
        d: Dataset = self.get_dataset(item.remote_id)

        # Here you comes your implementation. You should :
        # - fetch the remote dataset (if necessary)
        # - validate the fetched payload
        # - map its content to the dataset fields
        # - store extra significant data in the `extra` attribute
        # - map resources data

        r = self.csw.get_record(item.remote_id)

        d.title = r.identification[0].title
        d.description = r.identification[0].abstract
        # TODO: complete

        # FIXME: needed?
        d.resources.clear()

        # Needs at least one resource to be indexed
        for rs in r.distribution.online:
            if not rs.url:  # FIXME
                continue
            d.resources.append(Resource(
                title = rs.name,
                description = rs.description,
                url = rs.url
                # TODO: complete
            ))

        return d
