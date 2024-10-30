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
from fast_clients.fast_triplestore import TripleStore
from fast_clients.fast_files import S3
from app.deps import _get_s3_client, _get_triplestore_client
from pydantic import BaseModel, ValidationError, model_validator
from fastapi.encoders import jsonable_encoder
from io import BytesIO
from urllib.parse import urlparse

import smart_open

from pathlib import Path

from functools import wraps

from typing import List, Tuple, Annotated, Optional

import os
import asyncio
import json
import numpy as np
import pandas as pd
import logging
from app.loggers import logger_i

from dotenv import load_dotenv

load_dotenv()

from processors.annopnn import match_points_to_cloud_rknn
from app.routers.ingress import (
    Record,
    produce_record_json_unsecured,
    produce_record_json_n_binary_unsecured,
)

from app.models import (
    RecordLightGaxxxps,
    RecordLightGaxaalt,
    RecordLightAlag,    # Annotation
    RecordLightSaasg,   # Simulation
    RecordLightSedffea, # Observation alteration -> endommagement
    PointMatchData,
    LightPointMatchData,
)
from app.models import Base, base_checker


# FASTAPI Router
################
router = APIRouter(
    prefix="/api",
    tags=["api"],
    responses={404: {"description": "Operation on Astragale API not found"}},
)


# FASTAPI Data Models
#####################


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


@router.get("/config")
async def get_config(
    req: Request, triplestore: TripleStore = Depends(_get_triplestore_client)
):
    logger_i.warning(f"CONFIG: {triplestore.config}")
    logger_i.info(f"default: {triplestore.default_named_graph_uri}")

    return JSONResponse(content=triplestore.config)  # {"Content": json_response}


from app.deps import (
    _get_s3_client,
    _get_triplestore_client,
    _get_localfiles_client,
    _get_client_kafka,
)

#     ___          _
#    / / |_ ___ __| |_
#   / /|  _/ -_|_-<  _|
#  /_/  \__\___/__/\__|


@router.get("/test/get_static")
async def get_static_all_builtworks(
    req: Request, triplestore: TripleStore = Depends(_get_triplestore_client)
):
    qstring = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX acrm: <http://astragale.cnrs.fr/sem/acrm/>
                    SELECT ?e22 ?e22lab
                    WHERE {
                        FILTER NOT EXISTS { ?e22  acrm:P89_falls_within  [] } 
                        ?e22 a acrm:E22_HumanMadeObject .
                        ?e22 rdfs:label ?e22lab .
                        ?e22 acrm:P53_has_former_or_current_location/rdfs:label ?e53lab .
                    } LIMIT 20"""
    result = triplestore.select_static(query=qstring, format="dict")
    return JSONResponse(content=result)  # {"Content": json_response}


class NewBase(BaseModel):
    name: str
    point: Optional[float] = None
    is_accepted: Optional[bool] = False

    @model_validator(mode="before")
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value


@router.post("/test/submit3")
def submit3(model: Base = Depends(base_checker), files: List[UploadFile] = File(...)):
    return {"JSON Payload ": model, "Filenames": [file.filename for file in files]}


@router.post("/test/submit3/{dummy_id}/sub")
def submit2(
    dummy_id, model: Base = Depends(base_checker), files: List[UploadFile] = File(...)
):
    return {
        "JSON Payload ": model,
        "Filenames": [file.filename for file in files],
        "Query Params": dummy_id,
    }


@router.post("/test/submit3/{dummy_id}/sub_and_param")
def submit1(
    dummy_id,
    creator: Annotated[str | None, Query(max_length=50)] = None,
    data: NewBase = Body(...),
    files: List[UploadFile] = File(...),
):
    return {
        "JSON Payload ": data,
        "Filenames": [file.filename for file in files],
        "Path Params": dummy_id,
        "Query Params": creator,
    }




#     ___
#    / / |____ __ __
#   / /| '_ \ V  V /
#  /_/ |_.__/\_/\_/
@router.get("/builtworks")
async def get_all_builtworks(
    req: Request,
    recursive: Annotated[bool | None, Query()] = None,
    triplestore: TripleStore = Depends(_get_triplestore_client)
):
    try:
        if recursive is True:
            result = triplestore.select_templated(
                query_filename="s4bc0-all_builtworks.sparql", format="dict"
            )
            return JSONResponse(content=result)
        else:
            result = triplestore.select_templated(
                query_filename="s4bc1-all_builtworks_subuiltworks.sparql", format="dict",
                named_graph_uri=[triplestore.default_named_graph_uri, "http://astragale.cnrs.fr/graphs/th/th21_icomos"]
            )
            return JSONResponse(content=result)

    except Exception as e:
        logger_i.error(e)
        raise HTTPException(
            status_code=500,
            detail=f"Inlake Internal Error, check logs and healthcheck triplestore",
        )


@router.post("/builtworks", status_code=status.HTTP_201_CREATED)
@check_starlette_payload
async def post_bw(  req: Request,
                    response: Response,
                    namedgraph_override: Annotated[str | None, Query(max_length=50)] = None):
    try:
        jsondata = await req.json()
    except json.JSONDecodeError:
        return "Invalid JSON data."

    logger_i.info(jsondata)  # return JSONResponse(content=jsondata)

    return await produce_record_json_unsecured(
        response=response,
        record=Record(key_inlk="udb00-omabacap", content=json.dumps(jsondata)),
        kafkaio=_get_client_kafka(),
        namedgraph_override=namedgraph_override
    )


#     __                      _       _
#    / /_ _ ___ ___ _ __  ___| |_ _ _(_)___ ___
#   / / _` / -_) _ \ '  \/ -_)  _| '_| / -_|_-<
#  /_/\__, \___\___/_|_|_\___|\__|_| |_\___/__/
#     |___/


# /geometries
#############
@router.get("/builtworks/{builtwork_id}/geometries")
async def get_bw_id_geoms(
    req: Request,
    builtwork_id,
    creator: Annotated[str | None, Query(max_length=50)] = None,
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    logger_i.debug(
        f"existingbw: {triplestore.default_named_graph_uri+'/'+builtwork_id}"
    )
    result = triplestore.select_templated(
        query_filename="s3a10-all_geometries.sparql",
        format="dict",
        bw_uri=triplestore.config["default_triples_root_uri"] + builtwork_id,
        # enforce_parameters={"sub_sp6a": creator},
    )
    logger_i.info(f"result={result}")
    return JSONResponse(content=result)


@router.post("/builtworks/{builtwork_id}/geometries")
async def post_geometry_to_builtwork(
    builtwork_id,
    response: Response,
    geometry_type: Annotated[str | None, Query(max_length=50)] = None,
    triplestore: TripleStore = Depends(_get_triplestore_client),
    record: RecordLightGaxxxps = Body(...),
    file: UploadFile = File(...),
    namedgraph_override: Annotated[str | None, Query(max_length=50)] = None
):
    logger_i.info(
        {
            "JSON Payload ": record,
            "file.filename": file.filename,
            "filename pathlib": Path(file.filename).name,
            "Path Params": builtwork_id,
            "Query Params": geometry_type,
        }
    )
    filename = Path(file.filename).name

    builtwork_uri = triplestore.config["default_triples_root_uri"] + builtwork_id
    record_with_params = record.dict() | {
        "builtwork_uri": builtwork_uri,
        "resource_uri": f"s3://astra-3d-geom/{filename}",
        "file_label": filename,
        "file_format": Path(file.filename).suffix,
    }
    logger_i.debug(f"record_with_params={record_with_params}")

    match geometry_type:
        case "measure":
            if record_with_params["measure_type_uri"] == None:
                raise HTTPException(
                    status_code=422,
                    detail=f"Shall provide 'measure_type_uri' while uploading a measure",
                )

            return await produce_record_json_n_binary_unsecured(
                response=response,
                record=Record(
                    key_inlk="u3c00-gameaps", content=json.dumps(record_with_params)
                ),
                # file={'file': ('filetitle', data)},
                file=file,  # test: file=UploadFile(file=data)
                kafkaio=_get_client_kafka(),
                s3=_get_s3_client(),
            )
        case "model":
            if record_with_params["model_type_uri"] == None:
                raise HTTPException(
                    status_code=422,
                    detail=f"Shall provide 'model_type_uri' while uploading a model",
                )
            return await produce_record_json_n_binary_unsecured(
                response=response,
                record=Record(
                    key_inlk="u3c01-gamodps", content=json.dumps(record_with_params)
                ),
                # file={'file': ('filetitle', data)},
                file=file,  # test: file=UploadFile(file=data)
                kafkaio=_get_client_kafka(),
                s3=_get_s3_client(),
            )
        case "abstract_model":
            if (
                record_with_params["model_type_uri"] == None
                or record_with_params["scrs_geom_label"] == None
            ):
                raise HTTPException(
                    status_code=422,
                    detail=f"Shall provide 'model_type_uri' and 'scrs_geom_label' while uploading an abstract model",
                )
            return await produce_record_json_n_binary_unsecured(
                response=response,
                record=Record(
                    key_inlk="u3c02-gamodaps", content=json.dumps(record_with_params)
                ),
                # file={'file': ('filetitle', data)},
                file=file,  # test: file=UploadFile(file=data)
                kafkaio=_get_client_kafka(),
                s3=_get_s3_client(),
            )
        case _:
            raise HTTPException(
                status_code=404,
                detail=f"Provide the 'geometry_type' while uploading a geometry",
            )


# /geometries/{geometry_id}
###########################
@router.get("/builtworks/{builtwork_id}/geometries/{geometry_id}")
async def get_details_geometry(
    req: Request,
    builtwork_id,
    geometry_id,
    download_link: Annotated[str | None, Query(max_length=5)] = None,
    triplestore: TripleStore = Depends(_get_triplestore_client),
    s3: S3 = Depends(_get_s3_client),
):
    logger_i.debug(
        f"existingbw={triplestore.default_named_graph_uri+'/'+builtwork_id} ; existinggeom={triplestore.default_named_graph_uri+'/'+geometry_id}"
    )

    result = triplestore.select_templated(
        query_filename="sd696-details_geometry.sparql",
        format="dict",
        geom_uri=triplestore.config["default_triples_root_uri"] + geometry_id,
    )

    geom_presigned_url = s3.create_presigned_url(s3_url=result[0]["geom_path"])
    #    custom_netloc=urlparse(s3.config["s3_public_endpoint_url"]).netloc)

    # logger_i.info(f"{type(result)}, result={result}")
    logger_i.info(
        f"presigned={geom_presigned_url}, {s3.config['s3_public_endpoint_url']}"
    )

    result.append({"presigned_url": geom_presigned_url})
    return JSONResponse(content=result)


#     __  _           _      _   _             
#    / /_(_)_ __ _  _| |__ _| |_(_)___ _ _  ___
#   / (_-< | '  \ || | / _` |  _| / _ \ ' \(_-<
#  /_//__/_|_|_|_\_,_|_\__,_|\__|_\___/_||_/__/

# /builtworks/{builtwork_id}/simulations
########################################
@router.get("/builtworks/{builtwork_id}/simulations")
async def get_all_bw_simulations(
    builtwork_id,
    req: Request,
    triplestore: TripleStore = Depends(_get_triplestore_client)
):
    logger_i.debug(
        f"existingbw: {triplestore.default_named_graph_uri+'/'+ builtwork_id}"
    )
    result = triplestore.select_templated(
        query_filename="s9f2b-all_simulations_bw.sparql",
        format="dict",
        bw_uri=triplestore.config["default_triples_root_uri"] + builtwork_id,
        # enforce_parameters={"sub_sp6a": creator},
    )
    return JSONResponse(content=result)


@router.post("/builtworks/{builtwork_id}/simulations")
async def post_simulation_to_builtwork(
    builtwork_id,
    response: Response,
    geom_id: Annotated[str, Query(max_length=10)],
    record: RecordLightSaasg = Body(...),
    triplestore: TripleStore = Depends(_get_triplestore_client),
    file: UploadFile = File(...),
):
    logger_i.info(
        {
            "JSON Payload ": record,
            "file.filename": file.filename,
            "filename pathlib": Path(file.filename).name,
            "Path Params": builtwork_id,
        }
    )
    filename = Path(file.filename).name

    builtwork_uri = triplestore.config["default_triples_root_uri"] + builtwork_id
    geom_uri = triplestore.config["default_triples_root_uri"] + geom_id
    record_with_params = record.dict() | {
        "builtwork_uri": builtwork_uri,
        "geom_uri": geom_uri,
        "resource_uri": f"s3://astra-anno-aioli/{filename}",
        "file_label": filename,
        "file_format": Path(file.filename).suffix,
    }

    logger_i.debug(f"record_with_params={record_with_params}")

    return await produce_record_json_n_binary_unsecured(
        response=response,
        record=Record(key_inlk="u0cb1-saasg2", content=json.dumps(record_with_params)),
        # file={'file': ('filetitle', data)},
        file=file,  # test: file=UploadFile(file=data)
        kafkaio=_get_client_kafka(),
        s3=_get_s3_client(),
    )


# /simulations/{simulation_id}/damage
#####################################
@router.post("/simulations/{simulation_id}/damage")
@check_starlette_payload
async def post_enrich_simulation_alteration_to_damage(  
    simulation_id,
    response: Response,
    req: Request,
    observation_id: Annotated[str, Query(max_length=10)],
    record: RecordLightSedffea = Body(...),
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    logger_i.info(
        {
            "JSON Payload ": record,
            "Path Params": {"simulation_id": simulation_id, "observation_id": observation_id},
        }
    )
    simulation_uri = triplestore.config["default_triples_root_uri"] + simulation_id
    observation_uri = triplestore.config["default_triples_root_uri"] + observation_id
    record_with_params = record.dict() | {
        "simulation_uri": simulation_uri,
        "observation_uri": observation_uri
    }

    logger_i.debug(f"record_with_params={record_with_params}")

    return await produce_record_json_unsecured(
        response=response,
        record=Record(key_inlk="u59bb-sedffea", content=json.dumps(record_with_params)),
        kafkaio=_get_client_kafka(),
        # namedgraph_override=namedgraph_override
    )




#     __                  _        _   _
#    / /_ _ _ _  _ _  ___| |_ __ _| |_(_)___ _ _  ___
#   / / _` | ' \| ' \/ _ \  _/ _` |  _| / _ \ ' \(_-<
#  /_/\__,_|_||_|_||_\___/\__\__,_|\__|_\___/_||_/__/


# /builtworks/{builtwork_id}/geometries/{geometry_id}/annotations
#################################################################
@router.post("/builtworks/{builtwork_id}/geometries/{geometry_id}/annotations")
async def post_annotation_to_builtwork(
    builtwork_id,
    geometry_id,
    response: Response,
    annotation_type: Annotated[str | None, Query(max_length=50)] = None,
    annotation_layer_id: Annotated[str | None, Query(max_length=50)] = None,
    record: RecordLightGaxaalt = Body(...),
    triplestore: TripleStore = Depends(_get_triplestore_client),
    file: UploadFile = File(...),
):
    logger_i.info(
        {
            "JSON Payload ": record,
            "file.filename": file.filename,
            "filename pathlib": Path(file.filename).name,
            "Path Params": builtwork_id,
            "Query Params": annotation_type,
        }
    )
    filename = Path(file.filename).name

    builtwork_uri = (triplestore.config["default_triples_root_uri"] + builtwork_id) if (builtwork_id is not None) else None
    geom_uri = (triplestore.config["default_triples_root_uri"] + geometry_id) if (geometry_id is not None) else None
    annotationLayer_uri = (triplestore.config["default_triples_root_uri"] + annotation_layer_id) if (annotation_layer_id is not None) else None
    record_with_params = record.dict() | {
        "builtwork_uri": builtwork_uri,
        "geom_uri": geom_uri,
        "annotationLayer_uri": annotationLayer_uri,
        "resource_uri": f"s3://astra-3d-geom/{filename}",
        "file_label": filename,
        "file_format": Path(file.filename).suffix,
    }

    logger_i.debug(f"record_with_params={record_with_params}")

    match annotation_type:
        case "bw_feature":
            if annotation_layer_id == None:
                return await produce_record_json_n_binary_unsecured(
                    response=response,
                    record=Record(
                        key_inlk="u1c5b-gafaalt2", content=json.dumps(record_with_params)
                    ),
                    # file={'file': ('filetitle', data)},
                    file=file,  # test: file=UploadFile(file=data)
                    kafkaio=_get_client_kafka(),
                    s3=_get_s3_client(),
                )
            else:
                return await produce_record_json_n_binary_unsecured(
                    response=response,
                    record=Record(
                        key_inlk="u1c5a-gafaaltil2", content=json.dumps(record_with_params)
                    ),
                    # file={'file': ('filetitle', data)},
                    file=file,  # test: file=UploadFile(file=data)
                    kafkaio=_get_client_kafka(),
                    s3=_get_s3_client(),
                )
        case "bw_part":
            raise HTTPException(status_code=404, detail=f"Not Implemented Yet")
        case "bw_material":
            raise HTTPException(status_code=404, detail=f"Not Implemented Yet")
        case _:
            raise HTTPException(
                status_code=404,
                detail=f"Provide the 'annotation_type' while uploading an annotation",
            )


# /builtworks/{builtwork_id}/geometries/{geometry_id}/annotations
#################################################################
@router.get("/builtworks/{builtwork_id}/geometries/{geometry_id}/annotations")
async def get_geom_id_annofeats(
    req: Request,
    builtwork_id,
    geometry_id,
    only_observations: Annotated[bool | None, Query()] = None,
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    logger_i.debug(
        f"existingbw: {triplestore.default_named_graph_uri+'/'+builtwork_id}"
    )

    if only_observations is True:
        result = triplestore.select_templated(
            query_filename="s16eb-allH_observations_geom.sparql",
            override_named_graph_uri=[triplestore.default_named_graph_uri, "http://astragale.cnrs.fr/graphs/th/th21_icomos"],
            format="dict",
            geom_uri=triplestore.config["default_triples_root_uri"] + geometry_id,
            # enforce_parameters={"annotype": creator},
        )
        return JSONResponse(content=result)
    else:
        raise HTTPException(status_code=422, detail="Not Implemented")
        # result = triplestore.select_templated(
        #     query_filename="s2a7a-all_annotations_bw.sparql",
        #     override_named_graph_uri=[triplestore.default_named_graph_uri, "http://astragale.cnrs.fr/graphs/th/th21_icomos"],
        #     format="dict",
        #     geom_uri=triplestore.config["default_triples_root_uri"] + geometry_id,
        #     # enforce_parameters={"annotype": creator},
        # )
        # return JSONResponse(content=result)

    

# /builtworks/{builtwork_id}/annotations
########################################
@router.get("/builtworks/{builtwork_id}/annotations")
async def get_bw_id_annofeats(
    req: Request,
    builtwork_id,
    only_observations: Annotated[bool | None, Query()] = None,
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    logger_i.debug(
        f"existingbw: {triplestore.default_named_graph_uri+'/'+builtwork_id}"
    )

    if only_observations is True:
        result = triplestore.select_templated(
            query_filename="s16ea-allH_observations_bw.sparql",
            override_named_graph_uri=[triplestore.default_named_graph_uri, "http://astragale.cnrs.fr/graphs/th/th21_icomos"],
            format="dict",
            bw_uri=triplestore.config["default_triples_root_uri"] + builtwork_id,
            # enforce_parameters={"annotype": creator},
        )
        return JSONResponse(content=result)
    else:
        result = triplestore.select_templated(
            query_filename="s16ea-allH_observations_bw.sparql",
            override_named_graph_uri=[triplestore.default_named_graph_uri, "http://astragale.cnrs.fr/graphs/th/th21_icomos"],
            format="dict",
            bw_uri=triplestore.config["default_triples_root_uri"] + builtwork_id,
            # enforce_parameters={"annotype": creator},
        )
        return JSONResponse(content=result) 



# /builtworks/{builtwork_id}/annotations/pointsProximity
########################################################
# Post
@router.post("/builtworks/{builtwork_id}/annotations/pointsProximity")
async def post_annotations_points_proximity(
    in_pmdata: LightPointMatchData,
    builtwork_id,
    triplestore: TripleStore = Depends(_get_triplestore_client),
    distance_treshold: Annotated[float | None, Query()] = None,
    s3: S3 = Depends(_get_s3_client),
):
    result = triplestore.select_templated(
        query_filename="s16eb-allH_annotations_geom.sparql",
        format="dict",
        bw_uri=triplestore.config["default_triples_root_uri"] + builtwork_id,
        # enforce_parameters={"annotype": creator},
    )

    # logger_i.info(result)
    # logger_i.info(type(result))
    geom_paths = pd.DataFrame(result)["geom_path"].to_list()  # baaaah
    logger_i.info(geom_paths)

    # Get and merge annotation's point clouds
    np_pts = np.ndarray(shape=(0, 3))
    for s3_url in geom_paths:
        np_pts = np.vstack(
            (np_pts, s3.smart_read_ply(s3_url=s3_url)["points"].iloc[:, :3])
        )

    logger_i.info(f"Loaded table points={np.shape(np_pts)}")
    logger_i.info(f"Params: distance_treshold={distance_treshold}")
    kept_indexes = match_points_to_cloud_rknn(
        points_candidates=in_pmdata.points,
        points_sourcecloud=np_pts,
        distance_treshold=distance_treshold,  # 0.2,
        previsualization=False,
    )
    logger_i.info(f"Kept indices={kept_indexes}")

    return JSONResponse(content=json.dumps(kept_indexes.tolist(), sort_keys=True))
    # logger_i.debug(
    #     f"existingbw: {triplestore.default_named_graph_uri+'/'+builtwork_id}"
    # )
    # result = triplestore.rdfstore_sparql_select_templated(
    #     query_filename="383f-allUr_features_in_bw.sparql",
    #     format="dict",
    #     builtwork_uri=triplestore.config["default_root_uri"] + builtwork_id,
    # )
    # return JSONResponse(content=result)


# Post
@router.post("/builtworks/{builtwork_id}/annotations/pointsProximity2")
async def DEPRECATED_post_annotations_points_proximity_from_url_(
    in_pmdata: PointMatchData,
    builtwork_id,
    triplestore: TripleStore = Depends(_get_triplestore_client),
    s3: S3 = Depends(_get_s3_client),
):
    # tasks = [asyncio.create_task(s3.load_xyz_points(s3url) for s3url in in_pmdata.clouds_s3urls]

    # for s3url in in_pmdata.clouds_s3urls:
    #     np_pts = np.vstack((np_pts, await s3.load_xyz_points(s3url)))
    # for s3url in in_pmdata.clouds_s3urls:

    # result = triplestore.select_templated(
    #     query_filename="s16eb-allH_annotations_geom.sparql",
    #     format="dict",
    #     bw_uri=triplestore.config["default_triples_root_uri"] + builtwork_id,
    #     # enforce_parameters={"annotype": creator},
    # )

    np_pts = np.ndarray(shape=(0, 3))
    for s3_url in in_pmdata.clouds_s3urls:
        np_pts = np.vstack(
            (np_pts, s3.smart_read_ply(s3_url=s3_url)["points"].iloc[:, :3])
        )

    logger_i.info(f"Loaded table points={np.shape(np_pts)}")

    kept_indexes = match_points_to_cloud_rknn(
        points_candidates=in_pmdata.points,
        points_sourcecloud=np_pts,
        distance_treshold=0.2,
        previsualization=False,
    )
    logger_i.info(f"Kept indices={kept_indexes}")

    return JSONResponse(content=json.dumps(kept_indexes.tolist(), sort_keys=True))
    # logger_i.debug(
    #     f"existingbw: {triplestore.default_named_graph_uri+'/'+builtwork_id}"
    # )
    # result = triplestore.rdfstore_sparql_select_templated(
    #     query_filename="383f-allUr_features_in_bw.sparql",
    #     format="dict",
    #     builtwork_uri=triplestore.config["default_root_uri"] + builtwork_id,
    # )
    # return JSONResponse(content=result)


#     __                  _        _   _          _
#    / /_ _ _ _  _ _  ___| |_ __ _| |_(_)___ _ _ | |   __ _ _  _ ___ _ _ ___
#   / / _` | ' \| ' \/ _ \  _/ _` |  _| / _ \ ' \| |__/ _` | || / -_) '_(_-<
#  /_/\__,_|_||_|_||_\___/\__\__,_|\__|_\___/_||_|____\__,_|\_, \___|_| /__/
#                                                           |__/


# /geometries/{geometry_id}/annotationLayers
############################################
@router.post("/builtworks/{builtwork_id}/geometries/{geometry_id}/annotationLayers")
async def post_annotationLayer_to_geometry(
    builtwork_id,
    geometry_id,
    response: Response,
    record: RecordLightAlag = Body(...),
    triplestore: TripleStore = Depends(_get_triplestore_client),
    file: UploadFile = File(...),
):
    logger_i.info(
        {
            "JSON Payload ": record,
            "file.filename": file.filename,
            "filename pathlib": Path(file.filename).name,
            "Path Params": builtwork_id,
        }
    )
    filename = Path(file.filename).name

    builtwork_uri = triplestore.config["default_triples_root_uri"] + builtwork_id
    geom_uri = triplestore.config["default_triples_root_uri"] + geometry_id
    record_with_params = record.dict() | {
        "builtwork_uri": builtwork_uri,
        "geom_uri": geom_uri,
        "resource_uri": f"s3://astra-anno-aioli/{filename}",
        "file_label": filename,
        "file_format": Path(file.filename).suffix,
    }

    logger_i.debug(f"record_with_params={record_with_params}")

    return await produce_record_json_n_binary_unsecured(
        response=response,
        record=Record(key_inlk="uadf7-alag2", content=json.dumps(record_with_params)),
        # file={'file': ('filetitle', data)},
        file=file,  # test: file=UploadFile(file=data)
        kafkaio=_get_client_kafka(),
        s3=_get_s3_client(),
    )


# /builtworks/{builtwork_id}/annotationLayers
#############################################

# /annotationLayers
########################################
@router.get("/annotationLayers")
async def get_annolayers(
    req: Request,
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    result = triplestore.select_templated(
        query_filename="s6e18-all_annotation_layers.sparql",
        format="dict",
        # enforce_parameters={"annotype": creator},
    )
    return JSONResponse(content=result)


# /annotationLayers/{annotationLayer_id}
########################################
@router.get("/annotationLayers/{annotationLayer_id}")
async def get_details_and_status_annotationLayer(
    req: Request,
    annotationLayer_id,
    download_link: Annotated[str | None, Query(max_length=5)] = None,
    triplestore: TripleStore = Depends(_get_triplestore_client),
    s3: S3 = Depends(_get_s3_client),
):
    logger_i.debug(
        f"existing annotationLayer_id={triplestore.default_named_graph_uri+'/'+annotationLayer_id}"
    )

    result = triplestore.select_templated(
        query_filename="s41e9-details_annotationLayer.sparql",
        format="dict",
        annotationLayer_uri=triplestore.config["default_triples_root_uri"]
        + annotationLayer_id,
    )

    annotationLayer_path = result[0]["annotationLayer_path"]
    logger_i.info(f"annotationLayer_path={annotationLayer_path}")

    # ## Presigned URI
    # geom_presigned_url=s3.create_presigned_url(s3_url=annotationLayer_path)
    #                                         #    custom_netloc=urlparse(s3.config["s3_public_endpoint_url"]).netloc)

    # logger_i.info(f"presigned={geom_presigned_url}, {s3.config['s3_public_endpoint_url']}")

    # result.append({"presigned_url": geom_presigned_url})

    ## Status
    ### test if annotations loaded in graph
    annotations_exists = False if (result[0]["feat_uri"] == None) else True
    ### test file & floders
    (
        annotationLayer_file_exists,
        annotationLayer_folder_exists,
    ) = s3.check_s3_file_and_folder_coexistence(s3_url=annotationLayer_path)

    logger_i.info(
        f"annotations_exists={annotations_exists}, annotationLayer_file_exists={annotationLayer_file_exists}, annotationLayer_folder_exists={annotationLayer_folder_exists}"
    )

    if annotations_exists is True:
        if (
            annotationLayer_file_exists is True
            and annotationLayer_folder_exists is True
        ):
            result.append({"annotationLayer_status": "Loaded"})
        else:
            result.append(
                {
                    "annotationLayer_status": "Error",
                    "msg": "annotations are loaded but files and folders are deleted",
                }
            )
    else:
        if (
            annotationLayer_file_exists is True
            and annotationLayer_folder_exists is True
        ):
            result |= {"annotationLayer_status": "Extracted"}  # Archive and files
        elif (
            annotationLayer_file_exists is True
            and annotationLayer_folder_exists is False
        ):
            result |= {"annotationLayer_status": "Archived"}  # Only the archive
        else:
            result |= {
                    "annotationLayer_status": "Error, Archive incomplete, files or folders are deleted"
                }
            

    return result  # return JSONResponse(content=result) le cast en JSONResponse se fait par starlette, pas besoin de le faire à l'avance


# /annotationLayers/{annotationLayer_id}/ExtractArchive
#######################################################
@router.get("/annotationLayers/{annotationLayer_id}/ExtractArchive")
async def process_annotationLayers_extract_archives_in_s3(
    req: Request,
    annotationLayer_id,
    s3: S3 = Depends(_get_s3_client),
):
    # Get status, should be "Archived" to extract
    resp_status = await get_details_and_status_annotationLayer(
        req=req,
        annotationLayer_id=annotationLayer_id,
        triplestore=_get_triplestore_client(),
        s3=_get_s3_client(),
    )

    annotationLayer_path = resp_status[0]["annotationLayer_path"]
    logger_i.info(f"type={type(resp_status)}, val={resp_status}")

    annotationLayer_status = resp_status[1]["annotationLayer_status"]
    if annotationLayer_status != "Archived":
        logger_i.info("")
        return JSONResponse(
            content={
                "msg": f"Annotation Layer id={annotationLayer_id} has status={annotationLayer_status}, won't extract an already extracted or loaded annotationLayer archive at path {annotationLayer_path}"
            },
            status_code=422,
        )

    # Status='Archived' => Extracting s3 file on s3 folder
    logger_i.info(f"Extracting archive at annotationLayer_path={annotationLayer_path}")
    s3.extract_archive(s3_url=annotationLayer_path)

    (
        annotationLayer_file_exists,
        annotationLayer_folder_exists,
    ) = s3.check_s3_file_and_folder_coexistence(s3_url=annotationLayer_path)
    if annotationLayer_file_exists and annotationLayer_folder_exists:
        logger_i.info("ok")
        return JSONResponse(
            content={
                "msg": f"Annotation Layer id={annotationLayer_id} has been successfully extracted at path {annotationLayer_path}"
            },
            status_code=200,
        )
    else:
        return JSONResponse(
            content={
                "msg": f"Annotation Layer id={annotationLayer_id} extraction met an error at path {annotationLayer_path}, please consult Inlake's logs"
            },
            status_code=500,
        )


# /annotationLayers/{annotationLayer_id}/LoadGeometryAnnotations
################################################################
@router.get("/annotationLayers/{annotationLayer_id}/LoadGeometryAnnotations")
async def process_annotationLayers_load_annotations_from_s3(
    req: Request,
    annotationLayer_id,
    response: Response,
    triplestore: TripleStore = Depends(_get_triplestore_client),
    bw_id: Annotated[str | None, Query(max_length=8)] = None,
    geom_id: Annotated[str | None, Query(max_length=8)] = None,
    curated_table_filename: Annotated[
        str | None, Query(max_length=256)
    ] = "curated_table_layer.xlsx",
    s3: S3 = Depends(_get_s3_client),
):
    resp_status = await get_details_and_status_annotationLayer(
        req=req,
        annotationLayer_id=annotationLayer_id,
        triplestore=_get_triplestore_client(),
        s3=_get_s3_client(),
    )
    annotationLayer_status = resp_status[1]["annotationLayer_status"]
    logger_i.info(f"type={type(resp_status)}, val={resp_status}")

    # s3 Path to archive and folder
    annotationLayer_archive_path = resp_status[0]["annotationLayer_path"]
    bucket, key = s3.parse_url_s3_as_bucket_and_filename(annotationLayer_archive_path)
    annotationLayer_folder_path = f"s3://{bucket}/{key.split('.')[0]}"
    logger_i.info(
        f"annotationLayer_archive_path={annotationLayer_archive_path}, annotationLayer_folder_path={annotationLayer_folder_path}"
    )

    # Test annotationLayer status and curation table presence
    if annotationLayer_status != "Extracted":
        logger_i.info("")
        return JSONResponse(
            content={
                "msg": f"Annotation Layer id={annotationLayer_id} has status={annotationLayer_status}, won't load a still archived or already loaded annotationLayer archive at path {annotationLayer_path}"
            },
            status_code=422,
        )

    curated_table_path = f"{annotationLayer_folder_path}/{curated_table_filename}"
    if s3.check_s3_file_existence(s3_url=curated_table_path) == False:
        logger_i.info("")
        l_files, l_folders = s3.list_s3_contents_at_folder(
            s3_url=annotationLayer_folder_path
        )  # try except here
        return JSONResponse(
            content={
                "msg": f"No curation table for Annotation Layer id={annotationLayer_id} found at path={curated_table_path} ; existing files={l_folders}, existing files={l_files}"
            },
            status_code=422,
        )

    # params to uris
    bw_uri = triplestore.config["default_triples_root_uri"] + bw_id
    geom_uri = triplestore.config["default_triples_root_uri"] + geom_id
    annotationLayer_uri = triplestore.config["default_triples_root_uri"] + annotationLayer_id

    logger_i.info(f"Loading annotations to bw_uri={bw_uri}, geom_uri={geom_uri}, annotationLayer_id={annotationLayer_uri}")

    # Stream load from table

    with smart_open.open(
        curated_table_path, "rb", transport_params=dict(client=s3.client)
    ) as fin:
        df = pd.read_excel(fin)

        def record_maker_geom_annotation(row):
            # records_bw_feature = []
            match row["annotation_type"]:
                case "bw_feature":
                    logger_i.info(
                        f"Loading 'bw_feature' with feature_label={row['feature_label']}"
                    )

                    anno_geom_filename_s3 = str(row["resource_key"]).replace("/", "_")
                    anno_resource_path = (
                        f"{annotationLayer_folder_path}/{row['resource_key']}"
                    )

                    inlk_record_gafaalt = {
                        "feature_label": row["feature_label"],
                        "feature_type_uri": row[
                            "feature_type_uri"
                        ],  #  https://frollo.notre-dame.science/opentheso/th21/...
                        "observation_type_uri": row["observation_type_uri"],
                        "file_creator": row["file_creator"],
                        "file_date": row["file_date"],

                        "builtwork_uri": bw_uri,
                        "geom_uri": geom_uri,
                        "annotationLayer_uri": annotationLayer_uri,
                        "resource_uri": anno_resource_path,
                        "file_label": anno_geom_filename_s3,
                        "file_format": Path(anno_geom_filename_s3).suffix,
                    }

                    # logger_i.info(inlk_record_gafaalt)
                    # records_bw_feature.append(inlk_record_gafaalt)
                    return inlk_record_gafaalt
                
                case "bw_part":
                    logger_i.error(f"'bw_part' Not Implemented Yet")
                    pass
                case "bw_material":
                    logger_i.error(f"'bw_material' Not Implemented Yet")
                    pass
                case _:
                    logger_i.error(
                        f"Provide the 'annotation_type' while uploading an annotation"
                    )
                    pass
            
        records = df.apply(lambda row: record_maker_geom_annotation(row=row), axis=1)
        logger_i.info(records)

        results = []
        for record in records:
            logger_i.info(record)
            resp = await produce_record_json_unsecured(
                            response=response,
                            record=Record(
                                key_inlk="u1c5a-gafaaltil",
                                content=json.dumps(record),
                            ),
                            kafkaio=_get_client_kafka(),
                        )            
            logger_i.info(resp)
            results.append(resp)

        return results

    # logger_i.info({"JSON Payload ": record, "file.filename": file.filename, "filename pathlib": Path(file.filename).name, "Path Params": builtwork_id, "Query Params": annotation_type})
    # filename = Path(file.filename).name

    # builtwork_uri = triplestore.config["default_triples_root_uri"] + builtwork_id
    # geom_uri = triplestore.config["default_triples_root_uri"] + geometry_id
    # record_with_params = record.dict() | {"builtwork_uri": builtwork_uri, "geom_uri": geom_uri,
    #                                     "resource_uri": f"s3://astra-3d-geom/{filename}",
    #                                     "file_label": filename,
    #                                     "file_format": Path(file.filename).suffix}

    # logger_i.debug(f"record_with_params={record_with_params}")

    # match annotation_type:
    #     case "bw_feature":
    #         return await produce_record_json_n_binary_unsecured(
    #             response=response,
    #             record=Record(
    #                 key_inlk="u1c5b-gafaalt", content=json.dumps(record_with_params)
    #             ),
    #             # file={'file': ('filetitle', data)},
    #             file=file,         # test: file=UploadFile(file=data)
    #             kafkaio = _get_client_kafka(),
    #             s3 = _get_s3_client()
    #         )
    #     case "bw_part":
    #         raise HTTPException(status_code=404, detail=f"Not Implemented Yet")
    #     case "bw_material":
    #         raise HTTPException(status_code=404, detail=f"Not Implemented Yet")
    #     case _:
    #         raise HTTPException(status_code=404, detail=f"Provide the 'annotation_type' while uploading an annotation")


#     __              _
#    / / __  __ _ _ _| |_ ___
#   / / '_ \/ _` | '_|  _(_-<
#  /_/| .__/\__,_|_|  \__/__/
#     |_|
@router.get("/builtworks/{builtwork_id}/parts")
async def get_bw_id_maj(
    req: Request,
    builtwork_id,
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    logger_i.debug(
        f"existingbw: {triplestore.default_named_graph_uri+'/'+builtwork_id}"
    )
    result = triplestore.rdfstore_sparql_select_templated(
        query_filename="92d1-allUr_subuiltworks.sparql",
        format="dict",
        builtwork_uri=triplestore.config["default_root_uri"] + builtwork_id,
    )
    return JSONResponse(content=result)  # {"Content": json_response}


@router.get("/builtworks/{builtwork_id}/parts/{part_id}")
async def get_bw_id_part_id(
    req: Request,
    builtwork_id,
    part_id,
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    """Synoptic description of the part {part_id}
       Reminder: a part of a builtwork is still a builtwork, so this simply call get_synoptic_bw due to graph-based hierarchy implementation

    Args:
        req (Request): _description_
        builtwork_id (_type_): _description_
        part_id (_type_): _description_

    Returns:
        _type_: _description_
    """
    return await get_synoptic_bw(req=req, builtwork_id=part_id)


@router.get("/builtworks/{builtwork_id}/parts/{part_id}/geometries")
async def get_bw_id_part_id_geom(
    req: Request,
    builtwork_id,
    part_id,
    triplestore: TripleStore = Depends(_get_triplestore_client),
):
    """Synoptic description of the part {part_id}
       Reminder: a part of a builtwork is still a builtwork, so this simply call get_synoptic_bw due to graph-based hierarchy implementation

    Args:
        req (Request): _description_
        builtwork_id (_type_): _description_
        part_id (_type_): _description_

    Returns:
        _type_: _description_
    """
    return await get_bw_id_geoms(req=req, builtwork_id=part_id)
