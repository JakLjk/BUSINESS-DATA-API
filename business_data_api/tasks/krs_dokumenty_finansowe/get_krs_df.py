import os
import requests
import re
import warnings
import unicodedata
import hashlib
from typing import Literal
from lxml import etree
from lxml.etree import XMLSyntaxError
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
from typing import Dict, List, Tuple
from business_data_api.tasks.exceptions import (
                                            EntityNotFoundException, 
                                            InvalidParameterException,
                                            ScrapingFunctionFailed,
                                            WebpageThrottlingException,
                                            WebpageInMaintenanceMode)

# Filter XMLParsedAsHTMLWarning, since current logic parses 
# fragmets of XML that are embedded into HTML
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class KRSDokumentyFinansowe():
    """
    Class to handle the retrieval of financial documents from the KRS (Krajowy Rejestr Sądowy).
    """
    KRS_DF_URL = "https://ekrs.ms.gov.pl/rdf/pd/search_df"

    def __init__(self, krs_number):
        # Initialising requests session for handling future requests
        # That invovle remembering cookies and other session parameters
        self._session = requests.Session()
        # Setting up default ajaxx headers used in requests
        self._ajax_headers = {
            "Faces-Request": "partial/ajax",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.KRS_DF_URL,
            "Origin":"https://ekrs.ms.gov.pl"
        }
        # Defining variables for krs property 
        self.__krs_number: str = None 
        self.krs_number = krs_number

        # Holds infomrations about docuemnts that are
        # available on currently loaded documents preview table 
        self._download_documents_state = {}
        self._next_download_element_function_triggered = False

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
        """
        Loads the main KRS portal page
        """
        response = self._session.get(self.KRS_DF_URL)
        # Check if webpage is notin maintenance mode
        self._check_webpage_in_maintenance(response)
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
        """
        Requests a specific page number containing table with documents
        """
        viewstate = self._extract_current_viewstate(response)
        if page_num < 1:
            raise ValueError("Page number must be greater than or equal to 1.")
        # This function actually returns page based on document index 
        # like: return first 10 documents = return first page
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
        """
        When on document table list page, this function is responsible for 
        'clicking' button that will activate function returning popup containing 
        document information details
        """
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
        """
        Function that is used to press 'pokaz tresc dokumentu' button
        In turn, clicking this button downloads the document
        """
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
        # Re - decoding str from content disposition to read polish signs
        raw_content_dispositionn = content_disposition.encode('latin1')
        content_disposition = raw_content_dispositionn.decode('utf-8')
        if not content_disposition:
            raise ValueError("File name could not be found")
        match = re.search(r'filename="(.+?)"', content_disposition)
        if not match:
            raise ValueError("File name could not be found")
        else:
            filename = match.group(1)
            self._check_file_name_error(filename)
            self._check_cannot_display_page(response)
            return filename, response.content

    def _extract_current_viewstate(self, response: requests.Response) -> str:
        """
        Function that returns information about viewstate that is embedded into the
        response. It is necessarry to use the current viewstate when sending next request
        """
        response_text = response.text
        root = etree.fromstring(response_text.encode())
        viewstate_string = root.xpath('//update[@id="j_id1:javax.faces.ViewState:0"]')
        if not viewstate_string:
            raise ValueError("ViewState not found in the response.")
        return viewstate_string[0].text.strip()

    def _extract_number_of_pages(self, response: requests.Response) -> int:
        """
        Function for extracting available number of pages with documents
        for current KRS company
        """
        response_text = response.text
        root = etree.fromstring(response_text.encode('utf-8'))
        search_form_update_element = root.xpath('.//update[@id="searchForm"]')[0].text
        soup = BeautifulSoup(search_form_update_element, 'html.parser')
        num_of_pages_text = soup.find('span', class_='ui-paginator-current').get_text(strip=True)
        return int(re.search(r'Strona: \s*\d+/(\d+)', num_of_pages_text).group(1))

    def _extract_documents_table_data(self, response: requests.Response) -> list:
        """
        Function that extracts document information from loaded table
        """
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
                    self.krs_number +
                    row_dict['document_type'] +
                    row_dict['document_name'] +
                    row_dict['document_from'] +
                    row_dict['document_to']
                )
            )
            table_rows.append(row_dict)
        return table_rows

    def _extract_pokaz_tresc_dokumentu_id(self, response: requests.Response) -> str:
        """
        Function for extracting id of the button that is responsible for
        downloading the document
        """
        response_text = response.text
        root = etree.fromstring(response_text.encode('utf-8'))
        element_pokaz_tresc_dokumentu = root.xpath('.//update[@id="searchForm"]')[0].text
        soup = BeautifulSoup(element_pokaz_tresc_dokumentu, 'html.parser')
        return soup.find('a', string='Pokaż treść dokumentu')['id']

    def _helper_normalize_string(self, string:str) -> str:
        """
        Helper function that used for normalising string to a standard
        """
        return unicodedata.normalize("NFKD", string).strip().lower().replace('\xa0', ' ')

    def _helper_hash_string(self, string:str) ->str:
        """
        Helper function that is used for hashing strings, so that
        they can be used as optimised and unique document ID
        """
        return hashlib.sha256(string.encode('UTF-8')).hexdigest()

    def _check_cannot_display_page(self, response: requests.Response) -> bool:
        """
        Function that checks if the response that was returned contains elements
        that would suggest that the site was not loaded correctly
        Error can often appear when stale viewstate was provided
        """
        response_text = response.text
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
        """
        File name = Error most probably means that the id provided
        by '_extract_pokaz_tresc_dokumentu_id' is incorrect (due to scraping error 
        or webpage structure change)
        """
        if 'error' in filename:
            raise ScrapingFunctionFailed(
            f"\nFile name = Error most probably means that the id provided"
            f"\nby '_extract_pokaz_tresc_dokumentu_id' is incorrect"
            f"\n(due to scraping error or webpage structure change)"
            f"Filename: {filename}")

    def _check_exist_documents_for_krs(self, response: requests.Response) -> bool:
        """
        Function that checks if there are any documents available for
        the KRS number provided
        If not documents are available it means that such entity is not registered
        on the KRS platform
        """
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
        """
        Function that checks if the webpage is not throttling the user
        due to issues like too many requests
        """
        response_text = response.text
        try:
            root = etree.fromstring(response_text.encode('utf-8'))
            webpage_throttling_element = root.xpath('.//update[starts-with(@id, "unloggedForm:j_idt")]')[0].text    
        except IndexError:
            return
        soup = BeautifulSoup(webpage_throttling_element, 'html.parser')
        if 'Wymagane oczekiwanie pomiędzy kolejnymi wywołaniami' in soup.get_text():
            raise WebpageThrottlingException("\nWebpage sent throttling error"
                                            "\nBigger intervals between requests may be necessary"
                                            )
    def _check_webpage_in_maintenance(self, response: requests.Response):
        """
        Function that checks if the webpage is in maintenance mode,
        which in turn means that data cannot be scraped
        """
        soup = BeautifulSoup(response.text, 'html.parser')
        if soup.title.string == "Przerwa techniczna":
            raise WebpageInMaintenanceMode(
                "\nWepage is currently in service mode"
                "\nIt cannot be user for scraping data"
            )

    def get_document_list(self) -> List:
        """
        Main function responsible for getting document names list 
        from the KRS webpage
        """
        response = self._request_main_page()
        num_pages = self._extract_number_of_pages(response)
        table_data = []
        for n_page in range(1,num_pages+1):
            response = self._request_page(n_page, response)
            table_data.extend(self._extract_documents_table_data(response))
        return table_data

    def download_documents(self, document_hash_id_s: str | List):
        """
        Function responsible for initialising document
        scraping process.
        It saves the information about requested hash ids and other
        informations utilised by functions responsible for finding and scraping 
        krs df documents
        """

        if isinstance(document_hash_id_s, str):
            document_hash_id_s = [document_hash_id_s]

        self._download_documents_state = {
            "hash_ids":set(document_hash_id_s),
            "matched_documents":[],
            "current_index":0,
            "num_pages":0,
            "current_page_num":0,
            "response":self._request_main_page()
        }

        self._download_documents_state["num_pages"] = (self._extract_number_of_pages(
                                                        self._download_documents_state["response"]))
        self._download_documents_load_next_page()
        self._next_download_element_function_triggered = True

    def _download_documents_load_next_page(self):
        """
        Function that loads next page with documents 
        after all matching documents were found on the current page
        """
        state = self._download_documents_state
        state["current_page_num"] += 1
        state["response"] = self._request_page(state["current_page_num"], 
                                                state["response"])
        table = self._extract_documents_table_data(state["response"])
        state["matched_documents"] = [row for row in table if row['document_hash_id'] in state["hash_ids"]]
        state["current_index"] = 0

    def download_documents_next_id_value(self) -> str | None:
        """
        Functon that fetches next hash id that is supposed to be scraped
        """
        if not self._next_download_element_function_triggered:
            raise IndexError(
                "\nNext element was not called"
                "\nUse <download_documents_skip_id>"
                "\nor <download_documents_scrape_id>")

        state = self._download_documents_state
        while True:
            if state["current_index"] < len(state["matched_documents"]):
                row = state["matched_documents"][state["current_index"]]
                return row["document_hash_id"]
            else:
                if state["current_page_num"] >= state["num_pages"]:
                    return None
                else:
                    self._download_documents_load_next_page()

    def download_documents_skip_id(self):
        """
        Function that skips the id in order not to scrape it
        """
        state = self._download_documents_state
        state["current_index"] += 1
        self._next_download_element_function_triggered = True

    def download_documents_scrape_id(self)->dict:
        """
        Function responsible for physically scraping the document
        data from the webpage
        """
        state = self._download_documents_state
        row = state["matched_documents"][state["current_index"]]
        state["current_index"] += 1
        self._next_download_element_function_triggered = True

        internal_id = row['internal_element_id']
        hash_id = row['document_hash_id']
        request_document_details = self._request_document_details(
                                                            state["response"], 
                                                            internal_id)
        pokaz_tresc_dokumentu_id = self._extract_pokaz_tresc_dokumentu_id(
                                                            request_document_details)
        document_save_name, document_data = self._request_pokaz_tresc_dokumentu(
                                                            request_document_details, 
                                                            pokaz_tresc_dokumentu_id)
        file_extension = document_save_name.split('.')[-1]

        record = {
            'hash_id':hash_id,
            'krs_number':self.krs_number,
            'document_internal_id':row['internal_element_id'],
            'document_type':row['document_type'],
            'document_name':row['document_name'],
            'document_date_from':row['document_from'],
            'document_date_to':row['document_to'],
            'document_status':row['document_status'],
            'document_content_save_name':document_save_name,
            'document_content':document_data,
            "document_content_file_extension":file_extension
            }
        
        return record