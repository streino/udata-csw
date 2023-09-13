import factory

from dataclasses import dataclass
from udata.utils import faker

@dataclass
class CswRecord:
    id: str
    title: str
    type: str

class CswRecordFactory(factory.Factory):
    class Meta:
        model = CswRecord

    id = factory.Faker('uuid4')
    title = factory.Faker('sentence')
    type = 'dataset'
