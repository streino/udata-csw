from os.path import join, dirname

import pytest

from udata.core.organization.factories import OrganizationFactory
from udata.harvest import actions
from udata.harvest.tests.factories import HarvestSourceFactory
from udata.utils import faker

DATA_DIR = join(dirname(__file__), 'data')

pytestmark = pytest.mark.usefixtures('clean_db')


@pytest.mark.httpretty
def test_simple():
    org = OrganizationFactory()
    source = HarvestSourceFactory(backend='csw',
                                  url=faker.url(),
                                  organization=org)

    # TODO: mock remote endpoints responses

    actions.run(source.slug)

    source.reload()

    job = source.get_last_job()
    assert len(job.items) > 0

    # TODO: test resulting datasets, mappings, conditions...
