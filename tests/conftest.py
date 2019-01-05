import pytest
from swaptacular_debtor import create_app


@pytest.fixture
def app(request):
    return create_app()
