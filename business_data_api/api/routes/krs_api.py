import os
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.requests import Request
from typing import Literal
from dotenv import load_dotenv

from business_data_api.utils.response_templates.default_response import APIResponse
from business_data_api.tasks.krs_api.get_krs_api import KRSApi
from business_data_api.tasks.exceptions import InvalidParameterException, EntityNotFoundException
from business_data_api.utils.logger import setup_logger




router = APIRouter()

load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")

log_api_krsapi = setup_logger(
    logger_name="fast_api_route_krsapi",
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
)


@router.get(
    "/health", 
    summary="API KRS health check",
    response_model=APIResponse)
async def health(
    request: Request
    ):
    log_api_krsapi.info(f"Requested site: {request.url}")
    return APIResponse(
        status=ResponseStatus.SUCCESS,
        title="Health OK",
        message=""
    )

@router.get(
    "/get-extract", 
    summary=(
        "Get KRS extract information"
        "In form of raw json response fetched from KRS API"
    ),
    response_model=APIResponse)
async def getextract(
    request: Request,
    krs: str = Query(..., 
                    min_length=10,
                    max_length=10),
    registry: Literal["P", "S"] = Query("P"),
    extract_type: Literal["aktualny", "pelny"] = Query("aktualny")
    ):
    log_api_krsapi.info(f"Requested site: {response.url}")
    log_api_krsapi.debug(
        f"\nFetching extract [type-{extract_type}] information"
        f"\nfor [{rejestr}] {krs} from KRS API")
    try:
        fetched_extract = KRSApi().get_odpis(krs, registry, extract_type)
        log_api_krsapi.debug(f"Returning current extract for [{rejestr}] {krs}")
        return APIResponse(
            status=ResponseStatus.SUCCESS,
            title="KRS extract retrieved",
            message="The requested KRS extract has been successfully retrieved.",
            data=fetched_extract
        )
    except InvalidParameterException as e:
        error_message = (
            f"\nUnable to retrieve information about current krs extract"
            f"\nReason: Invalid Parameter"
            f"\nError message: {str(e)}"
        )
        log_api_krsapi.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    except EntityNotFoundException as e:
        error_message = (
            f"\nUnable to retrieve information about current krs extract"
            f"\nReason: Entity not found"
            f"\nError message: {str(e)}"
        )
        log_api_krsapi.error(error_message)
        raise HTTPException(status_code=404, detail=error_message)
    except Exception as e:
        error_message = (
            f"\nUnable to retrieve information about current krs extract"
            f"\nReason: Internal unhandled exception"
            f"\nError message: {str(e)}"
        )
        log_api_krsapi.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)

@router.get(
    "/get-history-of-changes", 
    summary="Get history of changes for registered krs records",
    response_model=APIResponse)
async def get_history_of_changes(
    request: Request,
    day: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
    hour_from: str = Query(..., regex=r"^\d{2}$"),
    hour_to: str = Query(..., regex=r"^\d{2}$")
    ):
    log_api_krsapi.info(f"Requested site: {request.url}")
    log_api_krsapi.debug(
        f"\nReturning history of changes for krs records"
        f"\nDay: {day}, Hour From {hour_from}, Hour To: {hour_to}"
        )
    try:
        fetched_changes_history = KRSApi().get_historia_zmian(
            day,
            hour_from, 
            hour_to
        )
        log_api_krsapi.debug(f"Returning hisotry of changes information to the client")
        return APIResponse(
            status=ResponseStatus.SUCCESS,
            title="History of Changes Retrieved",
            message="The requested history of changes has been successfully retrieved.",
            data=fetched_changes_history
        )
    except InvalidParameterException as e:
        error_message = (
            f"\nUnable to retrieve information about history of changes"
            f"\nReason: Invalid Parameter"
            f"\nError message: {str(e)}"
        )
        log_api_krsapi.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    except EntityNotFoundException as e:
        error_message = (
            f"\nUnable to retrieve information about history of changes"
            f"\nReason: Entity not found"
            f"\nError message: {str(e)}"
        )
        log_api_krsapi.error(error_message)
        raise HTTPException(status_code=404, detail=error_message)
    except Exception as e:
        error_message = (
            f"\nUnable to retrieve information about history of changes"
            f"\nReason: Internal unhandled exception"
            f"\nError message: {str(e)}"
        )
        log_api_krsapi.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    
