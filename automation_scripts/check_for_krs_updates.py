
import requests
import datetime
import time
import ast
import argparse

from config import (
    LOG_TO_POSTGRE_SQL,
    SOURCE_LOG_SYNC_PSQL_URL
)
from logging_utils import setup_logger

log = setup_logger(
    logger_name="krsapi_scheduler_log_job",
    log_to_db=LOG_TO_POSTGRE_SQL,
    log_to_db_url=SOURCE_LOG_SYNC_PSQL_URL
)
log.propagate = False

def check_for_updates(api_url:str, days_to_check:int=1):
    """
    Function that checks KRS API endpoint for updates in company registries and
    sends refresh query to the business data api, so that it updates local repositories
    with new data.

    days_to_check - default = 1. This argument tells function how many days back from
    the current day to check for updates.
    """
    URL_KRS_API = "https://api-krs.ms.gov.pl/api/Krs/Biuletyn/{dzien}?godzinaOd={godzinaOd}&godzinaDo={godzinaDo}"
    URL_ADD_KRS_BUSINESS_INFORMATION_TO_QUEUE =f"http://{api_url}/krs-api/update-business-information/{{krs}}"
    URL_ADD_KRS_DOCUMENTS_TO_QUEUE = f"http://{api_url}/krs-df/update-document-list/{{krs}}"
    
    log.info("Initialising job")
    unique_krs_numbers = set()
    non_unique_krs_numbers = []
    date_to=datetime.date.today()
    log.info(f"Checking for {days_to_check} last days")
    log.info("Gathering KRS numbers")
    for i, day_num in enumerate(range(days_to_check-1, -1, -1)):
        date = date_to - datetime.timedelta(days=day_num)
        log.info(f"Scraping day {date.strftime("%Y-%m-%d")} {i+1}/{days_to_check}")
        message = f"[{i+1}/{days_to_check}] Fetching krs changes for day {date}"
        krs_numbers = requests.get(URL_KRS_API.format(
            dzien=f"{date.year}-{date.month:02d}-{date.day:02d}",
            godzinaOd=0,
            godzinaDo=23
        )).text
        krs_numbers = ast.literal_eval(krs_numbers)
        krs_numbers = [krs.zfill(10) for krs in krs_numbers]
        unique_krs_numbers.update(krs_numbers)
    len_krs_numbers = len(unique_krs_numbers)
    log.info(f"Sending {str(len_krs_numbers)} krs records to backend for scraping")
    for i, krs_num in enumerate(unique_krs_numbers):
        time.sleep(0.05)
        message = f"[{i+1}/{len_krs_numbers}] Sending request for scraping krs number: {krs_num}"
        print(f"\r{message:<80}", end="", flush=True)
        requests.get(URL_ADD_KRS_BUSINESS_INFORMATION_TO_QUEUE.format(krs=krs_num)) 
        requests.get(URL_ADD_KRS_DOCUMENTS_TO_QUEUE.format(krs=krs_num)) 
    print("")
    log.info("KRS numbers were sent successfully")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script checking for updates in KRS API and adding update jobs to business API")
    parser.add_argument("--api-url", 
                        required=True, 
                        help="Base API URL without endpoints")
    parser.add_argument("--days", 
                        type=int,
                        default=1,
                        required=False, 
                        help="How many days to check back from today (default: 1 - today only)")
    args = parser.parse_args()
    check_for_updates(api_url=args.api_url, days_to_check=args.days)