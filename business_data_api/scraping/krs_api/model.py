import requests
from datetime import datetime
from typing import Literal

from business_data_api.scraping.exceptions import (
    EntityNotFoundException, 
    InvalidParameterException)

class KRSApi():
    """
    Krajowy Rejestr Sądowy (KRS) API client for accessing company data in Poland.
    This class provides methods to retrieve current and full extracts, as well as change history
    from the KRS API.
    Methods:
        - get_odpis_aktualny(krs, rejestr): Retrieves the current extract for a given KRS number and register.
        - get_odpis_pelny(krs, rejestr): Retrieves the full extract for a given KRS number and register.
        - get_historia_zmian(dzien, godzinaOd, godzinaDo): Retrieves change history for a specific date and time range.
    Attributes:
        - krs (str): The KRS number of the company.[10 digits]
        - rejestr (str): The register type, P - business, S - associations.
        - dzien (str): The date for which the change history is requested in the format YYYY-MM-DD.
        - godzinaOd (str): The start time for the change history in the format HH.
        - godzinaDo (str): The end time for the change history in the format HH.
    """
    def __init__(self):
        # Loading links to respective KRS API endpoints
        self._links = {
            "odpis_aktualny":"https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr={registry}&format=json",
            "odpis_pełny":"https://api-krs.ms.gov.pl/api/krs/OdpisPelny/{krs}?rejestr={registry}&format=json",
            "historia_zmian":"https://api-krs.ms.gov.pl/api/Krs/Biuletyn/{day}?godzinaOd={hour_fron}&godzinaDo={hour_to}"
        }

    def _check_parameter_krs(self, krs:str):
        """
        Function that checks validity of provided krs number
        """
        if not isinstance(krs, str):
            raise InvalidParameterException("KRS number must be a string.")
        if len(krs) != 10:
            raise InvalidParameterException("KRS number must be exactly 10 digits.")
        if not krs.isdigit():
            raise InvalidParameterException("KRS number must contain only digits.")

    def _check_parameter_rejestr(self, registry:str):
        """
        Function that check validity of provided registry string
        """
        if not isinstance(registry, str):
            raise InvalidParameterException("Register type must be a string.")
        if len(registry) != 1:
            raise InvalidParameterException("Register type must be a single character.")
        if registry not in ["P", "S"]:
            raise InvalidParameterException("Register type must be 'P' for business or 'S' for associations.")

    def _check_parameter_dzien(self, day:str):
        """
        Function that checks validity of provided day string
        """
        if not isinstance(day, str):
            raise InvalidParameterException("Date must be a string in the format YYYY-MM-DD.")
        try: datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            raise InvalidParameterException("Date must be in the format YYYY-MM-DD.")

    def _check_parameter_godzina(self, hour:str):
        """
        Function that checks validity of provided hour string
        """
        if not isinstance(hour, str):
            raise InvalidParameterException("Time must be a string in the format HH.")
        if len(hour) != 2:
            raise InvalidParameterException("Time must be exactly 2 digits.")
        if not hour.isdigit():
            raise InvalidParameterException("Time must contain only digits.")

    def _make_request(self, url:str) -> requests.Response:
        """
        Function that sends request to the KRS API endpoint
        """
        response = requests.get(url)
        if response.status_code == 404:
            raise EntityNotFoundException(f"\nKRS API source error:\n"
                                          f"Entity not found for URL: {url}")
        if response.status_code != 200:
            raise Exception(f"\nKRS API source error:\n"
                            f"Error: {response.status_code} - {response.text}\n"
                            f"URL: {url}")
        return response

    def _get_odpis_aktualny(self, krs:str, registry:str) -> dict:
        """
        Retrieves the current extract for a given KRS number and register.
        """
        self._check_parameter_krs(krs)
        self._check_parameter_rejestr(registry)
        url = self._links["odpis_aktualny"].format(krs=krs, registry=registry)
        return self._make_request(url).json()

    def _get_odpis_pelny(self, krs:str, registry:str) -> dict:
        """
        Retrieves the full extract for a given KRS number and register.
        """
        self._check_parameter_krs(krs)
        self._check_parameter_rejestr(registry)
        url = self._links["odpis_pełny"].format(krs=krs, registry=registry)
        return self._make_request(url).json()

    def get_odpis(self, 
            krs:str, 
            registry:Literal["P", "S"]="P", 
            extract_type:Literal["aktualny","pelny"]="aktualny") -> dict:
        """
        Retrieves the KRS extract (current or full) for a given KRS number and register.
        """
        if extract_type == "aktualny":
            return self._get_odpis_aktualny(krs, registry)
        elif extract_type == "pelny":
            return self._get_odpis_pelny(krs, registry)
        else:
            raise InvalidParameterException("Invalid type of extract. Use 'aktualny' or 'pelny'.")

    def get_historia_zmian(self, 
            day:str, 
            hour_from:str, 
            hour_to:str) -> dict:
        """
        Retrieves change history for a specific date and time range.
        """
        self._check_parameter_dzien(day)
        self._check_parameter_godzina(hour_from)
        self._check_parameter_godzina(hour_to)
        if int(hour_from) >= int(hour_to):
            raise ValueError("Start time must be earlier than end time.")
        url = self._links["historia_zmian"].format(day=day, hour_from=hour_from, hour_to=hour_to)
        return self._make_request(url).json()
