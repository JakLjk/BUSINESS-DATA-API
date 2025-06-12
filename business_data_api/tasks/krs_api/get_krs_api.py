import requests
from datetime import datetime

from business_data_api.tasks.exceptions import (EntityNotFoundException, 
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
        self._links = {
            "odpis_aktualny":"https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr={rejestr}&format=json",
            "odpis_pełny":"https://api-krs.ms.gov.pl/api/krs/OdpisPelny/{krs}?rejestr={rejestr}&format=json",
            "historia_zmian":"https://api-krs.ms.gov.pl/api/Krs/Biuletyn/{dzien}?godzinaOd={godzinaOd}&godzinaDo={godzinaDo}"
        }


    def _check_parameter_krs(self, krs):
        if not isinstance(krs, str):
            raise InvalidParameterException("KRS number must be a string.")
        if len(krs) != 10:
            raise InvalidParameterException("KRS number must be exactly 10 digits.")
        if not krs.isdigit():
            raise InvalidParameterException("KRS number must contain only digits.")

    def _check_parameter_rejestr(self, rejestr):
        if not isinstance(rejestr, str):
            raise InvalidParameterException("Register type must be a string.")
        if len(rejestr) != 1:
            raise InvalidParameterException("Register type must be a single character.")
        if rejestr not in ["P", "S"]:
            raise InvalidParameterException("Register type must be 'P' for business or 'S' for associations.")

    def _check_parameter_dzien(self, dzien):
        if not isinstance(dzien, str):
            raise InvalidParameterException("Date must be a string in the format YYYY-MM-DD.")
        try: datetime.strptime(dzien, "%Y-%m-%d")
        except ValueError:
            raise InvalidParameterException("Date must be in the format YYYY-MM-DD.")

    def _check_parameter_godzina(self, godzina):
        if not isinstance(godzina, str):
            raise InvalidParameterException("Time must be a string in the format HH.")
        if len(godzina) != 2:
            raise InvalidParameterException("Time must be exactly 2 digits.")
        if not godzina.isdigit():
            raise InvalidParameterException("Time must contain only digits.")

    def _make_request(self, url:str) -> requests.Response:
        response = requests.get(url)
        if response.status_code == 404:
            raise EntityNotFoundException(f"\nKRS API source error:\n"
                                          f"Entity not found for URL: {url}")
        if response.status_code != 200:
            raise Exception(f"\nKRS API source error:\n"
                            f"Error: {response.status_code} - {response.text}\n"
                            f"URL: {url}")
        return response

    def get_odpis_aktualny(self, krs:str, rejestr:str) -> dict:
        """Retrieves the current extract for a given KRS number and register.
        Args:
            krs (str): The KRS number of the company.
            rejestr (str): The register type, P - business, S - associations.
        """
        self._check_parameter_krs(krs)
        self._check_parameter_rejestr(rejestr)
        url = self._links["odpis_aktualny"].format(krs=krs, rejestr=rejestr)
        return self._make_request(url).json()

    def get_odpis_pelny(self, krs:str, rejestr:str) -> dict:
        """Retrieves the full extract for a given KRS number and register.
        Args:
            krs (str): The KRS number of the company.
            rejestr (str): The register type, P - business, S - associations.
        """
        self._check_parameter_krs(krs)
        self._check_parameter_rejestr(rejestr)
        url = self._links["odpis_pełny"].format(krs=krs, rejestr=rejestr)
        return self._make_request(url).json()

    def get_historia_zmian(self, dzien:str, godzinaOd:str, godzinaDo:str) -> dict:
        """Retrieves change history for a specific date and time range.
        Args:
            dzien (str): The date for which the change history is requested in the format YYYY-MM-DD.
            godzinaOd (str): The start time for the change history in the format HH.
            godzinaDo (str): The end time for the change history in the format HH.
        """
        self._check_parameter_dzien(dzien)
        self._check_parameter_godzina(godzinaOd)
        self._check_parameter_godzina(godzinaDo)
        if int(godzinaOd) >= int(godzinaDo):
            raise ValueError("Start time must be earlier than end time.")
        url = self._links["historia_zmian"].format(dzien=dzien, godzinaOd=godzinaOd, godzinaDo=godzinaDo)
        return self._make_request(url).json()
