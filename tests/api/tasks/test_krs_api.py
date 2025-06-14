import pytest
from business_data_api.tasks.krs_api.get_krs_api import KRSApi
from business_data_api.tasks.exceptions import InvalidParameterException, EntityNotFoundException


VALID_KRS = "0000057814"


def test_odpis_aktualny_invalid_krs_characters():
    krs_api = KRSApi()
    krs = "abc1234567"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_aktualny(krs=krs, rejestr="P")

def test_odpis_aktualny_invalid_krs_length():
    krs_api = KRSApi()
    krs = "12345678901"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_aktualny(krs=krs, rejestr="P")

def test_odpis_aktualny_invalid_krs_type():
    krs_api = KRSApi()
    krs = 1234567890 
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_aktualny(krs=krs, rejestr="P")

def test_odpis_aktualny_invalid_rejestr_characters():
    krs_api = KRSApi()
    rejestr = "X"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_aktualny(krs=VALID_KRS, rejestr=rejestr)

def test_odpis_aktualny_invalid_rejestr_length():
    krs_api = KRSApi()
    rejestr = "PP"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_aktualny(krs=VALID_KRS, rejestr=rejestr)

def test_odpis_aktualny_valid_non_existing_krs():
    krs_api = KRSApi()
    krs = "9999999999"
    with pytest.raises(EntityNotFoundException):
        krs_api._get_odpis_aktualny(krs=krs, rejestr="P")

def test_odpis_aktualny_valid():
    krs_api = KRSApi()
    response = krs_api._get_odpis_aktualny(krs=VALID_KRS, rejestr="P")
    assert response['odpis']['rodzaj'] == "Aktualny"

def test_odpis_pelny_invalid_krs_characters():
    krs_api = KRSApi()
    krs = "abc1234567"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_pelny(krs=krs, rejestr="P")

def test_odpis_pelny_invalid_krs_length():
    krs_api = KRSApi()
    krs = "12345678901"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_pelny(krs=krs, rejestr="P")

def test_odpis_pelny_invalid_krs_type():
    krs_api = KRSApi()
    krs = 1234567890 
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_pelny(krs=krs, rejestr="P")
    
def test_odpis_pelny_invalid_rejestr_characters():
    krs_api = KRSApi()
    rejestr = "X"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_pelny(krs=VALID_KRS, rejestr=rejestr)

def test_odpis_pelny_invalid_rejestr_length():
    krs_api = KRSApi()
    rejestr = "PP"
    with pytest.raises(InvalidParameterException):
        krs_api._get_odpis_pelny(krs=VALID_KRS, rejestr=rejestr)

def test_odpis_pelny_valid_non_existing_krs():
    krs_api = KRSApi()
    krs = "9999999999" 
    with pytest.raises(EntityNotFoundException):
        krs_api._get_odpis_pelny(krs=krs, rejestr="P")

def test_odpis_pelny_valid():
    krs_api = KRSApi()
    response = krs_api._get_odpis_pelny(krs=VALID_KRS, rejestr="P")
    assert response['odpis']['rodzaj'] == "Pełny"

def test_get_odpis_valid_krs_aktualny():
    krs_api = KRSApi()
    response = krs_api.get_odpis(krs=VALID_KRS, rejestr="P", typ_odpisu="aktualny")
    assert response['odpis']['rodzaj'] == "Aktualny"