import pytest

from os.path import join, dirname
from udata.core.organization.factories import OrganizationFactory
from udata.harvest import actions
from udata.harvest.tests.factories import HarvestSourceFactory
from udata.models import Dataset
from udata.utils import faker

from factories import CswRecordFactory
from csw_util import csw_dc

DATA_DIR = join(dirname(__file__), 'data')

pytestmark = pytest.mark.usefixtures('clean_db')


def record_from_file(catalog_id, record_id):
    filename = join(DATA_DIR, f"{catalog_id}--{record_id}.xml")
    return open(filename).read()


@pytest.mark.httpretty
def test_simple(rmock):
    catalog_id = 'sextant'
    record_id = '7b032e0a-515d-4685-b546-73995bea6287'

    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw',
                                  url=faker.url(),
                                  organization=org)

    rmock.post(source.url, text=csw_dc(CswRecordFactory(id=record_id)))
    rmock.get(source.url, text=record_from_file(catalog_id, record_id))

    actions.run(source.slug)

    source.reload()

    job = source.get_last_job()
    assert len(job.items) == 1
    assert job.items[0].remote_id == record_id

    datasets = {d.harvest.remote_id: d for d in Dataset.objects}
    assert len(datasets) == 1

    d: Dataset = datasets[record_id]
    assert d.harvest.remote_id == record_id
    assert d.title == 'The GEBCO_2020 Grid - A continuous terrain model of the global oceans and land'

    # TODO: test resulting datasets, mappings, conditions...
