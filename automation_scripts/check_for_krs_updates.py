
import requests
import datetime
import time
import ast
import argparse


def check_for_updates(api_url:str, days_to_check:int=1):
    """
    Function that checks KRS API endpoint for updates in company registries and
    sends refresh query to the business data api, so that it updates local repositories
    with new data.

    days_to_check - default = 1. This argument tells function how many days back from
    the current day to check for updates.
    """
    URL_KRS_API = "https://api-krs.ms.gov.pl/api/Krs/Biuletyn/{dzien}?godzinaOd={godzinaOd}&godzinaDo={godzinaDo}"
    URL_ADD_KRS_BUSINESS_INFORMATION_TO_QUEUE =f"http://{api_url}/update-business-information/{{krs}}"
    URL_ADD_KRS_DOCUMENTS_TO_QUEUE = f"http://{api_url}/update-document-list/{{krs}}"

    unique_krs_numbers = set()
    date_to=datetime.date.today()
    for i, day_num in enumerate(range(days_to_check-1, -1, -1)):
        date = date_to - datetime.timedelta(days=day_num)
        message = f"[{i+1}/{days_to_check}] Fetching krs changes for day {date}"
        print(f"\r{message:<80}", end="", flush=True)
        krs_numbers = requests.get(URL_KRS_API.format(
            dzien=f"{date.year}-{date.month:02d}-{date.day:02d}",
            godzinaOd=0,
            godzinaDo=23
        )).text
        krs_numbers = ast.literal_eval(krs_numbers)
        unique_krs_numbers.update(krs_numbers)
    len_krs_numbers = len(unique_krs_numbers)
    for i, krs_num in enumerate(unique_krs_numbers):
        time.sleep(0.05)
        print(f"\r[{i}/{len_krs_numbers}] Sending request for scraping krs number: {krs_num:<10}", end="", flush=True)
        requests.get(URL_ADD_KRS_BUSINESS_INFORMATION_TO_QUEUE.format(krs=krs_num)) 
        requests.get(URL_ADD_KRS_DOCUMENTS_TO_QUEUE.format(krs=krs_num)) 
    print("")
    

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