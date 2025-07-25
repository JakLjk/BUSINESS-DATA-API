import os
from dotenv import load_dotenv
from redis import Redis

from business_data_api.utils.logger import setup_logger
from business_data_api.db import create_sync_sessionmaker
from business_data_api.db.models import (
    RawKSRAPIFullExtract,
    CompanyInfo,
    CompanyInfoDetails)
from business_data_api.scraping.krs_api.model import KRSApi
from business_data_api.scraping.exceptions import (
    EntityNotFoundException,
    InvalidParameterException)


load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
psql_sync_url = os.getenv("SYNC_POSTGRE_URL")
redis_url = os.getenv("REDIS_URL")
stale_job_treshold_seconds = os.getenv("STALE_JOB_TRESHOLD_SECONDS")
sessionmaker = create_sync_sessionmaker(psql_sync_url)
redis_conn = Redis.from_url(redis_url)


def task_scrape_krs_api_extract(
        job_id:str,
        krs:str
):
    log = setup_logger(
    logger_name=f"worker_scrape_krs_api_full_extract",
    logger_id=job_id,
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
    )
    log.info(f"Starting process of scraping extract for krs {krs}")
    log.debug("Fetching extract from KRS API")
    extract_type = "aktualny"
    for registry in ["P", "S"]:
        try:
            extract = KRSApi().get_odpis(
            krs=krs,
            registry=registry,
            extract_type=extract_type
            )
            break
        except EntityNotFoundException as e:
            log.warning(f"\nEntity was not found for provided arguments:"
                        f"\nKRS: {krs}",
                        f"\nRegistry: {registry}",
                        f"\nExtract type: {extract_type}")
            continue
        except InvalidParameterException as e:
            log.error(
                f"\nScraping model has found invalid parameters when"
                f"\ntrying to scrape data from KRS API extract"
                f"\nException: {str(e)}")
            raise e
        except Exception as e:
            log.error(f"Exception has occurred during scrpaing process: \n{str(e)}")
            raise e
    else:
        log.error(f"Entity could not be found in KRS API repository")
        raise EntityNotFoundException
    log.info(f"starting process of populating tables with scraped extract")
    populate_tables_etl_process(
        job_id=job_id,
        krs=krs,
        extract=extract
    )
        
# TODO use data processor for manipulating data from JSON
def populate_tables_etl_process(job_id:str, krs:str, extract:dict):
    log = setup_logger(
    logger_name=f"worker_populate_tables_etl_process",
    logger_id=job_id,
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
    )
    log.debug("Populating table with raw extract data")
    table_raw_data = RawKSRAPIFullExtract(
            is_current=True,
            krs_number=krs,
            raw_data=extract
            
        )
    log.debug("Populating table with company info data")
    odpis = extract.get("odpis")
    dane_podmiotu = (odpis
                     .get("dane")
                     .get("dzial1")
                     .get("danePodmiotu"))
    siedziba_i_adres = (odpis
                    .get("dane")
                    .get("dzial1")
                    .get("siedzibaIAdres"))
    table_company_info_data = CompanyInfo(
        is_current=True,
        full_name=(dane_podmiotu
                   .get("nazwa")),
        legal_form=(dane_podmiotu
                   .get("formaPrawna")),
        krs_number=krs,
        nip_number=(dane_podmiotu
                    .get("identyfikatory")
                    .get("nip")),
        regon_number=(dane_podmiotu
                    .get("identyfikatory")
                    .get("regon")),
        country=(siedziba_i_adres
                   .get("adres")
                   .get("kraj")),
        voivodeship=(siedziba_i_adres
                   .get("siedziba")
                   .get("wojewodztwo")),
        municipality=(siedziba_i_adres
                   .get("siedziba")
                   .get("powiat")),
        county=(siedziba_i_adres
                   .get("siedziba")
                   .get("gmina")),
        city=(siedziba_i_adres
                   .get("adres")
                   .get("miejscowosc")),
        postal_number=(siedziba_i_adres
                   .get("adres")
                   .get("kodPocztowy")),
        street=(siedziba_i_adres
                   .get("adres")
                   .get("ulica")),
        house_number=(siedziba_i_adres
                   .get("adres")
                   .get("nrDomu")),
        email=(siedziba_i_adres
                   .get("adresPocztyElektronicznej")),
        webpage=(siedziba_i_adres
                   .get("adresStronyInternetowej")),
    )
    log.info(f"Starting DB session")
    with sessionmaker() as session:
        log.debug(
            f"\nSetting value of is_current to False for previous records"
            f"\nin raw table data, for krs={krs}")
        session.query(RawKSRAPIFullExtract).filter(
            RawKSRAPIFullExtract.krs_number==krs
        ).update({RawKSRAPIFullExtract.is_current: False})
        log.debug(
            f"\nSetting value of is_current to False for previous records"
            f"\nin company info table data, for krs={krs}")
        session.query(CompanyInfo).filter(
            CompanyInfo.krs_number==krs
        ).update({CompanyInfo.is_current: False})
        log.debug(f"Adding new table records to session")
        session.add(table_raw_data)
        session.add(table_company_info_data)
        log.info(f"Committing changes to DB")
        session.commit()