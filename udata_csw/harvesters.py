from udata.harvest.backends.base import BaseBackend
from udata.models import Resource
from udata.utils import faker


class CswBackend(BaseBackend):
    display_name = 'csw'

    def initialize(self):
        '''Generate a list of fake identifiers to harvest'''
        # Here you comes your implementation.
        # You should iter over a remote endpoint to list identifiers
        # to harvest and optionnaly store extra data
        for _ in range(faker.pyint()):
            self.add_item(faker.uuid4())  # Accept kwargs to store data

    def process(self, item):
        '''Generate a random dataset from a fake identifier'''
        # Get or create a harvested dataset with this identifier.
        # Harvest metadata are already filled on creation.
        dataset = self.get_dataset(item.remote_id)

        # Here you comes your implementation. You should :
        # - fetch the remote dataset (if necessary)
        # - validate the fetched payload
        # - map its content to the dataset fields
        # - store extra significant data in the `extra` attribute
        # - map resources data

        dataset.title = faker.sentence()
        dataset.description = faker.text()
        dataset.tags = list(set(faker.words(nb=faker.pyint())))

        # Resources
        for i in range(faker.pyint()):
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
