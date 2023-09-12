import pytest

from udata import settings
from udata.app import create_app

class CswSettings(settings.Testing):
    PLUGINS = ['csw']

@pytest.fixture
def app():
    app = create_app(settings.Defaults, override=CswSettings)
    return app
