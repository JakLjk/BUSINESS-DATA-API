from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.requests import Request
from typing import Literal

from business_data_api.utils.dict_response_template import compile_message
from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe

router = APIRouter()

@router.get("/health", summary="API KRS-DF health check")
async def health():
    return {"status": "ok", "message": "API is running"}


@router.get("/get-document-names", summary="Get names of available documents for KRS number")
async def get_document_names():
    pass

@router.get("/get-documents", summary="Get specified documents for KRS")
async def get_documents():
    pass