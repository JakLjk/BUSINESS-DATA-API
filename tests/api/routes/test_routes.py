import pytest
from fastapi.testclient import TestClient
from business_data_api.api import create_app

@pytest.fixture()
def app():
    return create_app(testing=True)

@pytest.fixture()
def client(app):
    return TestClient(app)

def test_krs_api_health_check(client):
    response = client.get('/krs-api/health')
    assert response.status_code == 200

def test_krs_df_health_check(client):
    response = client.get('/krs-df/health')
    assert response.status_code == 200

def test_krs_api_get_valid_krs_odpis_aktualny(client):
    response = client.get('/krs-api/get-odpis?krs=0000057814&rejestr=P')
    assert response.status_code == 200
    assert response.json()['data']['odpis']['rodzaj'] == 'Aktualny'
