import pytest
from business_data_api.api import initialise_flask_api

@pytest.fixture()
def app():
    app = initialise_flask_api(testing=True)
    yield app

@pytest.fixture()
def client(app):
    return app.test_client()

def test_health_check_krs_api(client):
    response = client.get('/krs-api/health')
    assert response.status_code == 200

def test_health_check_krs_df(client):
    response = client.get('/krs-df/health')
    assert response.status_code == 200

def test_health_check_root(client):
    response = client.get('/health')
    assert response.status_code == 200
