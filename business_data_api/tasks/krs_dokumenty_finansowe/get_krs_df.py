import requests
import re
import warnings
import unicodedata
import hashlib
from lxml import etree
from lxml.etree import XMLSyntaxError
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
from typing import Dict, List, Tuple
from business_data_api.tasks.exceptions import (
                                            EntityNotFoundException, 
                                            InvalidParameterException,
                                            ScrapingFunctionFailed,
                                            WebpageThrottlingException)



warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class KRSDokumentyFinansowe():
    """
    Class to handle the retrieval of financial documents from the KRS (Krajowy Rejestr Sądowy).
    """
    KRS_DF_URL = "https://ekrs.ms.gov.pl/rdf/pd/search_df"

    def __init__(self, krs_number):
        # TODO provide KRS in init and check - each KRSDokuemnty finansowe object should be used for specific KRS
        self._session = requests.Session()
        self._ajax_headers = {
            "Faces-Request": "partial/ajax",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.KRS_DF_URL,
            "Origin":"https://ekrs.ms.gov.pl"
        }
        self.__krs_number: str = None 
        self.krs_number = krs_number

    @property
    def krs_number(self):
        return self._krs_number
    
    @krs_number.setter
    def krs_number(self, krs_number):
        if not isinstance(krs_number, str):
            raise InvalidParameterException("KRS number must be a string.")
        if len(krs_number) != 10:
            raise InvalidParameterException("KRS number must be exactly 10 digits.")
        if not krs_number.isdigit():
            raise InvalidParameterException("KRS number must contain only digits.")
        self._krs_number = krs_number

    def _request_main_page(self) -> requests.Response:
        response = self._session.get(self.KRS_DF_URL)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # fetching initial viewstate
        viewstate = soup.find("input", {"name": "javax.faces.ViewState"}).get("value")

        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "unloggedForm:timeDelBtn",
            "javax.faces.partial.execute": "@all",
            "unloggedForm:timeDelBtn": "unloggedForm:timeDelBtn",
            "unloggedForm": "unloggedForm",
            "unloggedForm:krs0": self.krs_number,
            "javax.faces.ViewState": viewstate
        }
        response = self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)
        self._check_exist_documents_for_krs(response)
        self._check_cannot_display_page(response)
        self._check_webpage_throttling(response)
        return response
 
    def _request_page(self, page_num:int, response: requests.Response) -> requests.Response:
        viewstate = self._extract_current_viewstate(response)
        if page_num < 1:
            raise ValueError("Page number must be greater than or equal to 1.")
        first_row = (page_num - 1) * 10
        payload = {
            'javax.faces.partial.ajax': 'true',
            'javax.faces.source': 'searchForm:docTable',
            'javax.faces.partial.execute': 'searchForm:docTable',
            'javax.faces.partial.render': 'searchForm:docTable',
            'searchForm:docTable': 'searchForm:docTable',
            'searchForm:docTable_pagination': 'true',
            'searchForm:docTable_first': first_row,
            'searchForm:docTable_rows': '10',
            'searchForm:docTable_skipChildren': 'true',
            'searchForm:docTable_encodeFeature': 'true',
            'searchForm': 'searchForm',
            'searchForm:j_idt194_focus': '',
            'searchForm:j_idt194_input': '',
            'searchForm:j_idt197_focus': '',
            'searchForm:j_idt197_input': '',
            'searchForm:docTable_rppDD': '10',
            'javax.faces.ViewState': viewstate
        }
        response = self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)
        self._check_cannot_display_page(response)
        return response

    def _request_document_details(self, response: requests.Response, details_id:str) -> dict:
        viewstate = self._extract_current_viewstate(response)
        payload = {
            'javax.faces.partial.ajax': 'true',
            'javax.faces.source': details_id,
            'javax.faces.partial.execute': '@all',
            'javax.faces.partial.render': 'searchForm',
            details_id: details_id,
            'searchForm': 'searchForm',
            'searchForm:j_idt194_focus': '',
            'searchForm:j_idt194_input': '',
            'searchForm:j_idt197_focus': '',
            'searchForm:j_idt197_input': '',
            'searchForm:docTable_rppDD': '10',
            'javax.faces.ViewState': viewstate
        }
        response = self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)
        self._check_cannot_display_page(response)
        return response

    def _request_pokaz_tresc_dokumentu(self, 
                                        response: requests.Response, 
                                        id_pokaz_tresc_dokumentu: str) -> Tuple[str, requests.Response]:
        viewstate = self._extract_current_viewstate(response)
        payload = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": id_pokaz_tresc_dokumentu,
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": "searchForm",
            id_pokaz_tresc_dokumentu: id_pokaz_tresc_dokumentu,
            "searchForm": "searchForm",
            "searchForm:j_idt194_focus": "",
            "searchForm:j_idt194_input": "",
            "searchForm:j_idt197_focus": "",
            "searchForm:j_idt197_input": "",
            "searchForm:docTable_rppDD": "10",
            "javax.faces.ViewState": viewstate

        }
        response =  self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)
        content_disposition = response.headers.get('Content-Disposition')
        if not content_disposition:
            raise ValueError("File name could not be found")
        match = re.search(r'filename="(.+?)"', content_disposition)
        if not match:
            raise ValueError("File name could not be found")
        else:
            filename = match.group(1)
            self._check_file_name_error(filename)
            self._check_cannot_display_page(response)
            return filename, response

    def _extract_current_viewstate(self, response: requests.Response) -> str:
        response_text = response.text
        root = etree.fromstring(response_text.encode())
        viewstate_string = root.xpath('//update[@id="j_id1:javax.faces.ViewState:0"]')
        if not viewstate_string:
            raise ValueError("ViewState not found in the response.")
        return viewstate_string[0].text.strip()

    def _extract_number_of_pages(self, response: requests.Response) -> int:
        response_text = response.text
        root = etree.fromstring(response_text.encode('utf-8'))
        search_form_update_element = root.xpath('.//update[@id="searchForm"]')[0].text
        soup = BeautifulSoup(search_form_update_element, 'html.parser')
        num_of_pages_text = soup.find('span', class_='ui-paginator-current').get_text(strip=True)
        return int(re.search(r'Strona: \s*\d+/(\d+)', num_of_pages_text).group(1))

    def _extract_documents_table_data(self, response: requests.Response) -> list:
        response_text = response.text
        root = etree.fromstring(response_text.encode('utf-8'))
        try:
            search_form_update_element = root.xpath('.//update[@id="searchForm"]')[0].text
        except IndexError:
            search_form_update_element = root.xpath('.//update[@id="searchForm:docTable"]')[0].text
        soup = BeautifulSoup(search_form_update_element, 'html.parser')
        table_soup = soup.find_all('tr')
        if not table_soup:
            raise ValueError("No data table found in the response.")
        table_data = []
        for row in table_soup:
            columns = []
            for cell in row.find_all('td'):
                link = cell.find('a')
                if link and 'Pokaż szczegóły' in link.text:
                    columns.append(link.get('id'))
                else:
                    columns.append(cell.get_text(strip=True))
            table_data.append(columns)
        table_headers = [
            "document_id",
            "document_type",
            "document_name",
            "document_from",
            "document_to",
            "document_status",
            "internal_element_id",
        ]
        table_rows = []
        for row in table_data:
            row_dict = dict(zip(table_headers, row))
            row_dict['document_hash_id'] = self._helper_hash_string(
                self._helper_normalize_string(
                    row_dict['document_type'] +
                    row_dict['document_name'] +
                    row_dict['document_from'] +
                    row_dict['document_to']
                ))
            table_rows.append(row_dict)
        return table_rows

    def _extract_pokaz_tresc_dokumentu_id(self, response: requests.Response) -> str:
        response_text = response.text
        root = etree.fromstring(response_text.encode('utf-8'))
        element_pokaz_tresc_dokumentu = root.xpath('.//update[@id="searchForm"]')[0].text
        soup = BeautifulSoup(element_pokaz_tresc_dokumentu, 'html.parser')
        return soup.find('a', text='Pokaż treść dokumentu')['id']

    def _helper_normalize_string(self, string:str) -> str:
        return unicodedata.normalize("NFKD", string).strip().lower().replace('\xa0', ' ')

    def _helper_hash_string(self, string:str) ->str:
        return hashlib.sha256(string.encode('UTF-8')).hexdigest()

    def _check_cannot_display_page(self, response: requests.Response) -> bool:
        """Error can appear when stale viewstate was provided"""
        response_text = response.text
        print(response_text)
        print("////_check_cannot_display_page")
        try:
            root = etree.fromstring(response_text.encode('utf-8'))
            viewroot_update = root.xpath('.//update[@id="javax.faces.ViewRoot"]')[0].text
        except IndexError:
            return
        except XMLSyntaxError:
            return
        soup = BeautifulSoup(viewroot_update, 'html.parser')
        
        if 'Witryna sieci Web nie może wyświetlić strony' in soup.get_text():
            raise ScrapingFunctionFailed("\nCould not display page based using injected AJAX function"
            "\nError can arrise when stale viewstate is used"
            )

    def _check_file_name_error(self, filename:str):
        """File name = Error most probably means that the id provided
        by '_extract_pokaz_tresc_dokumentu_id' is incorrect (due to scraping error 
        or webpage structure change)"""
        if 'error' in filename:
            raise ScrapingFunctionFailed(
            "\nFile name = Error most probably means that the id provided"
            "\nby '_extract_pokaz_tresc_dokumentu_id' is incorrect"
            "\n(due to scraping error or webpage structure change)")

    def _check_exist_documents_for_krs(self, response: requests.Response) -> bool:
        response_text = response.text
        try:
            root = etree.fromstring(response_text.encode('utf-8'))
            no_documents_element = root.xpath('.//update[starts-with(@id, "unloggedForm:j_idt")]')[0].text
        except IndexError:
            return
        soup = BeautifulSoup(no_documents_element, 'html.parser')
        if 'Brak dokumentów dla KRS:' in soup.get_text():
            raise EntityNotFoundException("Server Error - No documents for specified KRS")

    def _check_webpage_throttling(self, response: requests.Response):
        response_text = response.text
        try:
            root = etree.fromstring(response_text.encode('utf-8'))
            webpage_throttling_element = root.xpath('.//update[starts-with(@id, "unloggedForm:j_idt")]')[0].text    
        except IndexError:
            return
        soup = BeautifulSoup(webpage_throttling_element, 'html.parser')
        print(soup)
        print("////_check_webpage_throttling")
        if 'Wymagane oczekiwanie pomiędzy kolejnymi wywołaniami' in soup.get_text():
            raise WebpageThrottlingException("\nWebpage sent throttling error"
                                            "\nBigger intervals between requests may be necessary"
                )
    def get_document_list(self):
        response = self._request_main_page()
        num_pages = self._extract_number_of_pages(response)
        table_data = []
        for n_page in range(1,num_pages+1):
            response = self._request_page(n_page, response)
            table_data.extend(self._extract_documents_table_data(response))
        return table_data

    def download_document(self, document_hash_id_s: str | List):
        if isinstance(document_hash_id_s, str):
            document_hash_id_s = [document_hash_id_s]
        response = self._request_main_page()
        num_pages = self._extract_number_of_pages(response)
        for n_page in range(1, num_pages + 1):
            response = self._request_page(n_page, response)
            table = self._extract_documents_table_data(response)
            matched_document_ids = [(row['internal_element_id'], row['document_hash_id'])
                for row in table if row['document_hash_id'] in document_hash_id_s]
            for internal_id, hash_id in matched_document_ids:
                request_document_details = self._request_document_details(response, internal_id)
                pokaz_tresc_dokumentu_id = self._extract_pokaz_tresc_dokumentu_id(request_document_details)
                document_name, document_data = self._request_pokaz_tresc_dokumentu(
                                        request_document_details, 
                                        pokaz_tresc_dokumentu_id)
                file_extension = document_name.split('.')[-1]
                document_save_name = hash_id + '.' + file_extension
                if isinstance(document_data, requests.Response):
                    with open(document_save_name, 'wb') as f:
                        f.write(document_data.content)
                else:
                    with open(document_save_name, 'w') as f:
                        f.write(document_data)


                


def test():
    krsdf = KRSDokumentyFinansowe("0000057814")
    # krsdf = KRSDokumentyFinansowe("1234567890")
    f = krsdf.get_document_list()
    print(f)
    print("\n".join([f"{row['document_id']}-{row['document_type']} - {row['document_from']} - {row['document_hash_id']}" for row in f]))
    print(len(f))
    document_mock_hashes = [
        '1efafb5d855c95440446f70412e14ffcf0c267050b4167003c6554739c516142',
        'cad3cb20c83529ad55e255c16f9dabf3a6f03647ca23812ef0a4b0ef002c40c5',
        '6fcf6bb1131a8066c27a024544af6588068986c3731b2aea1b6e53ea8e7c0eee',
        '89eb162b724b986501cc27904ef5576b90f117828955ed3a966ed61fd66666b3'
    ]
    krsdf.download_document(document_mock_hashes)
    
    # a =  krsdf._request_main_page()
    # # print(a.text)
    # # b = krsdf._extract_number_of_pages(a)
    # # c = krsdf._request_page(4, a)
    # # d = krsdf._extract_documents_table_data(c)
    # # e = krsdf._request_document_details(c, d[0]['internal_element_id'])
    # # e = krsdf._request_document_details(c, 'xd')
    # # print(e.text)
    # # f = krsdf._extract_pokaz_tresc_dokumentu_id(e)
    # # n, g = krsdf._request_pokaz_tresc_dokumentu(e, f)
    # # print(g.text)
    # # print(n)
    # print(a.text)


    # a =  krsdf._request_main_page("0000057814")
    # b = krsdf._number_of_pages(a)
    # c = krsdf._request_page(1, a)
    # d = krsdf._documents_table_data(c)
    # e = krsdf._request_document_details(c, d[0][-1])
    # f = krsdf._extract_pokaz_tresc_dokumentu_id(e)
    # g = krsdf._request_pokaz_tresc_dokumentu(e, f)
    # print(len(d))
    # print(len(e.text))
    # print(len(f))
    # save_response_to_file(g, "document_content.xml")
   



test()


# from typing import NewType
# import requests

# PageResponse = NewType("PageResponse", requests.Response)
# MainPageResponse = NewType("MainPageResponse", requests.Response)

# def _request_page(self, page_num: int, response: requests.Response) -> PageResponse:
#     ...
#     return PageResponse(self._session.post(...))

# def _request_main_page(self, krs_number: str) -> MainPageResponse:
#     ...
#     return MainPageResponse(self._session.post(...))

# def _request_document_details(self, response: PageResponse, details_id: str) -> dict:
#     ...