import pytest
from business_data_api.api import initialise_flask_api

@pytest.fixture()
def app():
    app = initialise_flask_api(testing=True)
    yield app

@pytest.fixture()
def client(app):
    return app.test_client()

def test_krs_api_health_check(client):
    response = client.get('/krs-api/health')
    assert response.status_code == 200

def test_krs_df_health_check(client):
    response = client.get('/krs-df/health')
    assert response.status_code == 200

def test_root_health_check(client):
    response = client.get('/health')
    assert response.status_code == 200

def test_krs_api_get_docs(client):
    response = client.get('/krs-api/docs')
    assert response.status_code == 200
    assert 'application/json' in response.content_type
    assert response.json['title'] == 'KRS API Documentation'

def test_krs_api_get_valid_krs_odpis_aktualny(client):
    response = client.get('/krs-api/get-odpis?krs=0000057814&rejestr=P')
    assert response.status_code == 200
    assert response.json['data']['odpis']['rodzaj'] == 'Aktualny'
