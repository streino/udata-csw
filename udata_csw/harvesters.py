import logging

from collections import defaultdict
from datetime import datetime
from dateutil.parser import parse as parse_date
from itertools import chain
from owslib.iso import CI_Date, MD_Keywords
from owslib.fes import OgcExpression
from typing import Optional
from udata.core.dataset.models import HarvestDatasetMetadata, HarvestResourceMetadata
from udata.harvest.backends.base import BaseBackend, HarvestFilter
from udata.harvest.models import HarvestItem
from udata.i18n import lazy_gettext as _
from udata.models import Dataset, License, Resource
from udata.utils import faker, get_by

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

    # TODO: add validation
    def process(self, item: HarvestItem):
        record = self.csw.get_record(item.remote_id)
        d: Dataset = self.get_dataset(item.remote_id)

        # TODO: initialize/complete d.harvest properly
        if not d.harvest:
            d.harvest = HarvestDatasetMetadata()

        ident = record.identification[0]

        # private = db.BooleanField(default=False)

        # acronym = db.StringField(max_length=128)
        d.title = ident.title or ident.alternatetitle
        d.description = ident.abstract

        keywords = CswBackend._get_keywords(*ident.keywords)
        d.tags = list(set(chain(*keywords.values())))

        # FIXME: also ident.accessconstraints/useconstraints ?
        # d.license = CswBackend._get_license(*ident.uselimitation)

        if created_at := CswBackend._get_date(*ident.date, type=['creation', 'publication']):
            d.harvest.created_at = created_at
        if modified_at := CswBackend._get_date(*ident.date, type=['revision']):
            d.harvest.modified_at = modified_at

        # frequency = db.StringField(choices=list(UPDATE_FREQUENCIES.keys()))
        # frequency_date = db.DateTimeField(verbose_name=_('Future date of update'))
        # - ident.[temporalextent_start, temporalextent_end]
        # temporal_coverage = db.EmbeddedDocumentField(db.DateRange)

        # - ident.spatialrepresentationtype
        # - ident.extent
        # - ident.bbox
        # - record.referencesystem.[code, codeSpace, version]
        # spatial = db.EmbeddedDocumentField(SpatialCoverage)

        # FIXME: what is this?
        # ext = db.MapField(db.GenericEmbeddedDocumentField())

        for k, v in keywords.items():
            label = 'iso:keywords' + (f":{k}" if k else '')
            d.extras[label] = list(v)
        if ident.contact:
            # TODO: separate record.contact[], ident.publisher[], ident.contributor[] ?
            d.extras['iso:contact'] = [f"{c.name} ({c.role}) - {c.organization} <{c.email}>" for c in ident.contact]
        if record.stdname:
            d.extras['iso.std'] = record.stdname + (f":{record.stdver}" if record.stdver else '')
        if ident.topiccategory:
            d.extras['iso:topiccategory'] = ident.topiccategory
        if ident.uricode:
            d.extras['iso:uricode'] = ident.uricode
        if ident.status:
            d.extras['iso:status'] = ident.status
        if record.dataquality and record.dataquality.lineage:
            d.extras['iso:lineage'] = record.dataquality.lineage

        for res in record.distribution.online:
            if not res.url:
                continue

            r: Resource = get_by(d.resources, 'url', res.url)
            if not r:
                r = Resource(url=res.url)

            # TODO: initialize/complete r.harvest properly
            if not r.harvest:
                r.harvest = HarvestResourceMetadata()

            r.title = res.name
            r.description = res.description

            # type = db.StringField(choices=list(RESOURCE_TYPES), default='main', required=True)
            # filetype = db.StringField(choices=list(RESOURCE_FILETYPES), default='file', required=True)
            # urlhash = db.StringField()
            # checksum = db.EmbeddedDocumentField(Checksum)
            # format = db.StringField()
            # mime = db.StringField()
            # filesize = db.IntField()  # `size` is a reserved keyword for mongoengine.
            # fs_filename = db.StringField()
            # schema = db.DictField()

            # extras = db.ExtrasField()

            # rs.harvest.created_at/modified_at

            d.resources.append(r)

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

    @staticmethod
    def _get_keywords(*ows_keywords: MD_Keywords) -> dict[str, set[str]]:
        keywords = defaultdict(set)
        for k in ows_keywords:
            # k.type can be None -> empty namespace
            keywords[k.type] |= {kw.name for kw in k.keywords if kw.name}
        return keywords

    @staticmethod
    def _get_license(*ows_licenses: str, default : License = License.default()) -> License:
        for l in ows_licenses:
            if license := License.guess_one(l):
                return license
        return default

    @staticmethod
    def _get_date(*ows_date: CI_Date, type: list[str] = [], default: Optional[datetime] = None) -> datetime:
        for t in type:
            if date_str := next((d.date for d in ows_date if d.type == t), None):
                try:
                    return parse_date(date_str)
                except:
                    log.warn(f"Failed parsing {d.type} date: {date_str}")
        return default