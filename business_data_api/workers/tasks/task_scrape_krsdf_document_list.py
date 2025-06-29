from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe

def task_get_document_list(krs:str):
    krsdf = KRSDokumentyFinansowe(krs)
    return krsdf.get_document_list()