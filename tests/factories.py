import factory

from dataclasses import dataclass
from decimal import Decimal
from udata.utils import faker

@dataclass
class Bbox:
    lower: tuple[Decimal]
    upper: tuple[Decimal]
    crs: str

class BboxFactory(factory.Factory):
    class Meta:
        model = Bbox

    lower = factory.Faker('latlng')
    upper = factory.Faker('latlng')
    crs = 'urn:ogc:def:crs:EPSG:6.6:4326'

@dataclass
class CswRecord:
    id: str
    title: str
    type: str
    bbox: Bbox

class CswRecordFactory(factory.Factory):
    class Meta:
        model = CswRecord

    id = factory.Faker('uuid4')
    title = factory.Faker('sentence')
    type = 'dataset'
    bbox = factory.SubFactory(BboxFactory)
