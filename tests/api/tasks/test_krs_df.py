import pytest
import warnings
from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe
from business_data_api.scraping.exceptions import (
                                            EntityNotFoundException, 
                                            InvalidParameterException,
                                            ScrapingFunctionFailed,
                                            WebpageThrottlingException)
VALID_KRS = "0000057814"

def test_non_existing_krs_number():
    krs = "9999999999"
    with pytest.raises(EntityNotFoundException):
        krsdf = KRSDokumentyFinansowe(krs)
        document_list = krsdf.get_document_list()

def test_invalid_krs_characters():
    krs = "abc1234567"
    with pytest.raises(InvalidParameterException):
        krsdf = KRSDokumentyFinansowe(krs)

def test_invalid_krs_lenght():
    krs = "12345678901"
    with pytest.raises(InvalidParameterException):
        krsdf = KRSDokumentyFinansowe(krs)

def test_invalid_krs_variable_type():
    krs = 1234567890
    with pytest.raises(InvalidParameterException):
        krsdf = KRSDokumentyFinansowe(krs)

def test_existing_krs_number_get_document_list():
    krsdf = KRSDokumentyFinansowe(VALID_KRS)
    document_list = krsdf.get_document_list()
    assert len(document_list) > 38
    assert document_list[-1]['document_type'] == (
                "Opinia biegłego rewidenta / sprawozdanie z badania rocznego "
                "sprawozdania finansowego"
            )
    

