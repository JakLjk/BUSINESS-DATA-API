from typing import List
from sqlalchemy import text

from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe
from business_data_api.db import psql_syncsession
from business_data_api.db.models import ScrapedKrsDF, ScrapingStatus, RedisScrapingRegistry



def task_scrape_krsdf_documents(job_id: str, krs: str, hash_ids: List[str]):
    session = psql_syncsession()

    # List of all hash ids found by the scraping function
    # It is used to evaluate if all provided hash ids were found
    found_hash_ids = []

    krsdf = KRSDokumentyFinansowe("0000057814")
    krsdf.download_documents(hash_ids)
    while (hash_id := krsdf.download_documents_next_id()):
        print(f"SCRAPING: {hash_id}")
        # Check and lock if hash id in db
        with session.begin():
            # Lock and wait for the row if it is used by another process
            hash_id_int = int(hash_id[:16], 16)
            session.execute(
                text("SELECT pg_advisory_xact_lock(:hash_id);"),
                {"hash_id":hash_id_int}
            )
            existing_record = (
                session.query(RedisScrapingRegistry)
                .filter(RedisScrapingRegistry.hash_id == hash_id)
                .first()
            )
            if existing_record:
                print("Record Exists")
                if existing_record.job_status == ScrapingStatus.FAILED:
                    print("Record failed")
                    pass
                elif existing_record.job_status == ScrapingStatus.PENDING:
                    print("Record pending")
                    pass
                else:
                    print("Different status - skipping")
                    krsdf.download_documents_skip_id()
            else:
                print("Record does not exists")
                print("Scraping")
                data = krsdf.download_documents_scrape_id()
                record_scraped_krsdf = ScrapedKrsDF(**data)
                record_redis_registry = RedisScrapingRegistry(
                    hash_id = hash_id,
                    job_id = job_id,
                    job_status = ScrapingStatus.FINISHED,
                    scraping_error_message = "")
                print("Saving record to DB")
                session.add(record_scraped_krsdf)
                session.add(record_redis_registry)
                session.commit()






    # # print(krsdf.get_document_list())
    # hash_ids = ["ef030fa04d4430446185035eeba4a968055c0a2dd833b4d4d23b99db2cc4729a",
    # "a2e0c3156329bca9306e7c761f709542d575ed0bee500578f8065f508cac528f",
    # "26ac4c219cc9ddd369e0cae8786a24dfa98fc5dd5f6e4551fe2820899f036a2d"]
    # krsdf.download_documents(hash_ids)
    # next_hash_id = krsdf.download_documents_next_id()
    # print(f"NEXT HASH: {next_hash_id}")
    # downloaded_document = krsdf.download_documents_scrape_id()
    # print(downloaded_document["hash_id"], " ", downloaded_document["document_content_save_name"])

    # from time import sleep
    # sleep(5)
    # next_hash_id = krsdf.download_documents_next_id()
    # print(f"NEXT HASH: {next_hash_id}")
    # downloaded_document = krsdf.download_documents_scrape_id()
    # print(downloaded_document["hash_id"], " ", downloaded_document["document_content_save_name"])

    # pg = PGManagerKRSDF()
    # krsdf = KRSDokumentyFinansowe(krs)
    # krsdf.download_documents(hash_ids)
    # id = krsdf.download_documents.next_id()
    # if pg.id_in_db(id):
    #     krsdf.download_documents.skip_id()
    # else:
    #     record = krsdf.download_documents.scrape_id()
    #     pg.insert(record)


hash_ids = ["ef030fa04d4430446185035eeba4a968055c0a2dd833b4d4d23b99db2cc4729a"]
# "a2e0c3156329bca9306e7c761f709542d575ed0bee500578f8065f508cac528f",
# "26ac4c219cc9ddd369e0cae8786a24dfa98fc5dd5f6e4551fe2820899f036a2d"]
task_scrape_krsdf_documents("dummy_job", "0000057814", hash_ids)