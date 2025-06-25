from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.requests import Request
from typing import Literal

from business_data_api.utils.dict_response_template import compile_message
from business_data_api.tasks.krs_api.get_krs_api import KRSApi
from business_data_api.tasks.exceptions import InvalidParameterException, EntityNotFoundException


router = APIRouter()

@router.get("/health", summary="API KRS health check")
async def health():
    return {"status": "ok", "message": "API is running"}

@router.get("/get-odpis", summary="Get current KRS extract")
async def get_odpis(
    request: Request,
    krs: str = Query(..., 
                    min_length=10,
                    max_length=10),
    rejestr: Literal["P", "S"] = Query("P"),
    typ_odpisu: Literal["aktualny", "pelny"] = Query("aktualny")
):
    try:
        fetched_extract = KRSApi().get_odpis(krs, rejestr, typ_odpisu)
        return compile_message(
            "KRS Extract Retrieved",
            "The requested KRS extract has been successfully retrieved.",
            fetched_extract,
        )
    except InvalidParameterException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except EntityNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal error")

@router.get("/get-historia-zmian", summary="Get history of changes for krs record")
async def get_hisoria_zmian(
    request: Request,
    dzien: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
    godzinaOd: str = Query(..., regex=r"^\d{2}$"),
    godzinaDo: str = Query(..., regex=r"^\d{2}$")
):
    try:
        fetched_changes_history = KRSApi().get_historia_zmian(
            dzien,
            godzina_od, 
            godzina_do
        )
        return compile_message(
            "History of Changes Retrieved",
            "The requested history of changes has been successfully retrieved.",
            fetched_changes_history)
    except InvalidParameterException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except EntityNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal error")
    
