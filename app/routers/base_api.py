from fastapi import (
    APIRouter,
    HTTPException,
    Depends,
    Response,
    status,
    File,
    UploadFile,
    Request,
    Form,
)
from fastapi import Query, Body, status, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError, model_validator
from fastapi.encoders import jsonable_encoder


from io import BytesIO
from pathlib import Path
from functools import wraps
from typing import List, Tuple, Annotated, Optional
import json
from urllib.parse import urlparse

from app.loggers import logger_i

from dotenv import load_dotenv
load_dotenv()


from app.models import Base, base_checker


# FASTAPI Router
################
router = APIRouter(
    prefix="/api",
    tags=["api"],
    responses={404: {"description": "Operation on Astragale API not found"}},
)

# BIGLAKE Clients
#################
# triplestore = _get_triplestore_client()
# s3 = _get_s3_client()

def check_starlette_payload(func):
    """Decorator checking the validity of the payload depending on:
       + the 'Content-Type' header as 'application/json'
       + TODO: deserialize the json and pass it to the function
       + TODO: security-check the json with existing function

    Args:
        func (_type_): _description_

    Raises:
        HTTPException: _description_
        HTTPException: _description_

    Returns:
        _type_: _description_
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        # logger_i.debug(f"Checking Payload, args={args} kwargs={kwargs}")
        content_type = kwargs["req"].headers.get("Content-Type")

        if content_type is None:
            raise HTTPException(
                status_code=400,
                detail="Content-Type header is not provided, Please provide `application/json` and compliant data.",
            )
        elif content_type == "application/json":
            return await func(*args, **kwargs)
        elif content_type == "multipart/form-data":
            return await func(*args, **kwargs)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Content-Type '{content_type}' not supported on endpoint. Please provide `application/json` and compliant data",
            )

    return wrapper


#     __
#    / /
#   / /
#  /_/
@router.get("/")
async def home(req: Request):
    # return templates.TemplateResponse('home.html', {'request': req})
    return {"msg": "Hello Astragale"}


#     ___          _
#    / / |_ ___ __| |_
#   / /|  _/ -_|_-<  _|
#  /_/  \__\___/__/\__|


# @router.get("/test/get_static")
# async def get_static_all_builtworks(
#     req: Request, triplestore: TripleStore = Depends(_get_triplestore_client)
# ):
#     qstring = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#                     PREFIX acrm: <http://astragale.cnrs.fr/sem/acrm/>
#                     SELECT ?e22 ?e22lab
#                     WHERE {
#                         FILTER NOT EXISTS { ?e22  acrm:P89_falls_within  [] } 
#                         ?e22 a acrm:E22_HumanMadeObject .
#                         ?e22 rdfs:label ?e22lab .
#                         ?e22 acrm:P53_has_former_or_current_location/rdfs:label ?e53lab .
#                     } LIMIT 20"""
#     result = triplestore.select_static(query=qstring, format="dict")
#     return JSONResponse(content=result)  # {"Content": json_response}