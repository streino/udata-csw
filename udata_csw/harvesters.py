import logging

from owslib.fes import OgcExpression
from udata.harvest.backends.base import BaseBackend, HarvestFilter
from udata.harvest.models import HarvestItem
from udata.i18n import lazy_gettext as _
from udata.models import Dataset, Resource
from udata.utils import faker

from .csw_client import CswClient

_SUPPORTED_OGC_EXPRESSIONS = [
    'And', 'Or', 'Not',
    'PropertyIsEqualTo', 'PropertyIsNotEqualTo',
    'PropertyIsLike', 'PropertyIsNull',
    'PropertyIsLessThan', 'PropertyIsLessThanOrEqualTo',
    'PropertyIsGreaterThan', 'PropertyIsGreaterThanOrEqualTo',
    'PropertyIsBetween'
]
_mod = __import__('owslib.fes', fromlist=_SUPPORTED_OGC_EXPRESSIONS)
_OGC_EXPRESSIONS = {
    name: getattr(_mod, name) for name in _SUPPORTED_OGC_EXPRESSIONS
}

MAX_RECORDS = 25  # FIXME: testing only

log = logging.getLogger(__name__)

class CswBackend(BaseBackend):
    display_name = 'csw'

    filters = [
        HarvestFilter(_('OGC filter'), 'ogc_filters', str, _('OGC filter'))
    ]

    def initialize(self):
        self.csw = CswClient(self.source.url)
        ids = self.csw.get_ids(constraints=self._get_constraints(), limit=MAX_RECORDS)
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

        d.resources.clear()  # FIXME: needed?
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

    def _get_constraints(self):
        return [
            self._parse_ogc_filter(f['value'])
            for f in self.get_filters()
            if f['key'] == 'ogc_filters' and f.get('type') != 'exclude'
        ]

    def _parse_ogc_filter(self, expr: str) -> OgcExpression:
        # FIXME: unsafe !!!
        return eval(expr, None, _OGC_EXPRESSIONS)
