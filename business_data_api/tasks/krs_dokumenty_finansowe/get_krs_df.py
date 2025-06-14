import requests
import re
import warnings
from lxml import etree
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning



warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class KRSDokumentyFinansowe():
    """
    Class to handle the retrieval of financial documents from the KRS (Krajowy Rejestr Sądowy).
    """
    KRS_DF_URL = "https://ekrs.ms.gov.pl/rdf/pd/search_df"

    def __init__(self):
        # TODO provide KRS in init and check - each KRSDokuemnty finansowe object should be used for specific KRS
        self._krs_number: str = None 
        self._session = requests.Session()
        self._ajax_headers = {
            "Faces-Request": "partial/ajax",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.KRS_DF_URL,
            "Origin":"https://ekrs.ms.gov.pl"
        }

    @property
    def krs_number(self):
        return self._krs_number
    
    @krs_number.setter
    def krs_number(self, krs_number):
        #TODO check validity
        self._krs_number = krs_number

    def _extract_current_viewstate(self, response: requests.Response) -> str:
        response_text = response.text
        root = etree.fromstring(response_text.encode())
        viewstate_string = root.xpath('//update[@id="j_id1:javax.faces.ViewState:0"]')
        if not viewstate_string:
            raise ValueError("ViewState not found in the response.")
        return viewstate_string[0].text.strip()

    
    def _request_main_page(self, krs_number: str) -> requests.Response:
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
            "unloggedForm:krs0": krs_number,
            "javax.faces.ViewState": viewstate
        }
        return self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)

    def _extract_number_of_pages(self, response: requests.Response) -> int:
        response_text = response.text
        root = etree.fromstring(response_text.encode('utf-8'))
        search_form_update_element = root.xpath('.//update[@id="searchForm"]')[0].text
        soup = BeautifulSoup(search_form_update_element, 'html.parser')
        num_of_pages_text = soup.find('span', class_='ui-paginator-current').get_text(strip=True)
        return int(re.search(r'Strona: \s*\d+/(\d+)', num_of_pages_text).group(1))

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
        return self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)
    
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
            "docuemnt_from",
            "document_to",
            "document_status",
            "internal_element_id"
        ]
        return [dict(zip(table_headers, row)) for row in table_data]

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
        return self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)

    def _extract_pokaz_tresc_dokumentu_id(self, response: requests.Response) -> str:
        response_text = response.text
        root = etree.fromstring(response_text.encode('utf-8'))
        element_pokaz_tresc_dokumentu = root.xpath('.//update[@id="searchForm"]')[0].text
        soup = BeautifulSoup(element_pokaz_tresc_dokumentu, 'html.parser')
        return soup.find('a', text='Pokaż treść dokumentu')['id']

    def _request_pokaz_tresc_dokumentu(self, response: requests.Response, id_pokaz_tresc_dokumentu: str):
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
        return self._session.post(self.KRS_DF_URL, headers=self._ajax_headers, data=payload)

    def _save_file(filename:str, filepath:str, data:str):
        pass

    def get_document_list(self, krs_number:str):
        # TODO set property of krs number with the number provided
        response = self._request_main_page(krs_number)
        num_pages = self._extract_number_of_pages(response)
        table_data = []
        for n_page in range(1,num_pages+1):
            response = self._request_page(n_page, response)
            table_data.extend(self._extract_documents_table_data(response))
        return table_data

    def download_document(self, document_type_s:int|list):
        if isinstance(document_name_s, int):
            document_name_s = [document_name_s]
        response = self._request_main_page(krs_number)
        num_pages = self._extract_number_of_pages(response)
        for n_page in range(1, num_pages + 1):
            response = self._request_page(n_page, response)
            table = self._extract_documents_table_data(response)
            for document_type in document_type_s:
                





  



def test():
    krsdf = KRSDokumentyFinansowe()
    f = krsdf.get_document_list("0000057814")
    print(f)
    print("\n".join([f"{row['document_id']}-{row['document_type']}" for row in f]))
    print(len(f))



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