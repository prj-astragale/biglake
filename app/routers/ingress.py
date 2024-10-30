from fastapi import APIRouter, HTTPException, Depends, Response, status, File, UploadFile
from fast_clients.fast_files import FileData, S3, Local
from fast_clients.fast_kafka import KafkaAio
from app.deps import _get_s3_client, _get_triplestore_client, _get_localfiles_client, _get_client_kafka



import logging
from app.loggers import logger_i

from dotenv import load_dotenv
load_dotenv()

from typing import (Union, Tuple, Optional)
from pydantic import BaseModel
import json
import uuid
import ast
from urllib.parse import urlparse
import os, sys


router = APIRouter(
    prefix="/ingress",
    tags=["ingress"],
    responses={404: {"description": "Operation on ingress not found"}},
)



class UrlS3(BaseModel):
    """
    key_inlk (str): inlake key as the data pipeline identifier used for ETL/EATL
    content (str):  contents of the record formatted as datapipeline's schema
    url_filestore (Optional[str]): target url in filestore for optional filestorage
    _topic_override (Optional[str]): overide of kafka destination topic

    Args:
        BaseModel (_type_): _description_
    """
    url_filestore:          str      # for minio, url_filestore is bucket path


@router.post('/upload_s3', name='upload_s3')
async def upload_to_s3(response: Response,
                       urls3: UrlS3 = Depends(), 
                       file: UploadFile = File(...),
                       s3: S3 = Depends(_get_s3_client)) -> FileData:
    # r = s3.parse_url_s3(urls3.url_filestore)
    r = await s3.upload(file=file, url_s3=urls3.url_filestore)
    logger_i.info(f"{r}")
    return r

# @router.post('/upload_local', name='upload_local')
# async def upload_to_local(files: list[FileData] = Depends(_get_localfiles_client)) -> list[FileData]:
#     """Upload multiple files to the container filesystem

#     Args:
#          file (FileData, optional): _description_. Defaults to Depends(s3).

#     Returns:
#         FileData: A pydantic BaseModel representing the result of an UploadFile operation
#     """
#     return files




class Record(BaseModel):
    """
    key_inlk (str): inlake key as the data pipeline identifier used for ETL/EATL
    content (str):  contents of the record formatted as datapipeline's schema
    url_filestore (Optional[str]): target url in filestore for optional filestorage
    _topic_override (Optional[str]): overide of kafka destination topic

    Args:
        BaseModel (_type_): _description_
    """
    key_inlk:               str             
    content:                str
    url_filestore:          Optional[str] = None     # for minio, url_filestore is bucket path
    topic_override:         Optional[str] = None

##########################
# PARSERS Context to Kafka
##########################
def inlk_to_kafka_key(my_key_inlk: str) -> Tuple[str, str]:
    duuid = str(uuid.uuid4())[:8]
    kkey = f"{duuid}://{my_key_inlk}"
    return kkey, duuid

def parse_filesys_dirpath(s: str) -> Tuple[str, str]:
    o = urlparse(s, allow_fragments=False)
    return o.scheme, o.netloc, o.path

#                                         __
#  __ _____  ___ ___ ______ _________ ___/ /
# / // / _ \(_-</ -_) __/ // / __/ -_) _  / 
# \_,_/_//_/___/\__/\__/\_,_/_/  \__/\_,_/  
                                          
@router.post("/unsecured/record-json")
async def produce_record_json_unsecured(    record: Record, 
                                            response: Response,
                                            kafkaio: KafkaAio = Depends(_get_client_kafka),
                                            namedgraph_override : str | None = None
                                        ):
    """Send a record to inlake.records.topic with the schema name as key
    Record's content is NOT validated against the provided schema.
    The key can be formated as follow : 
        + "SCHEMA": specifying only the schema to be used
        + "SCHEMA/DESTINATION": specifying the schema and the database/part of database to be sent to. Convenient for debugging of Named Graphs in Knowledge Bases

    Args:
        record (Record): pydantic model for sent content, format query with params={"kafka_key": str, "content": str}
        response (Response): handler for fastapi's starlette http response
        client_kafka (AIOKafkaApi, dependance): asynchronous client for Kafka Producer. Defaults to Depends(get_client_kafka).
        client_schema (InlkSchemaRClient, dependance): exposes synchronous client for confluent_kafka Schema Registry. Defaults to Depends(get_client_schema).
        global_config (configParser, dependance): configuration. Defaults to Depends(get_global_config).

    Raises:
        HTTPException (HTTP_404_NOT_FOUND): when schema named "kafka_key" is not found on Schema Registry
        HTTPException (HTTP_422_UNPROCESSABLE_ENTITY): when record's content is not compliant with schema

    Returns:
        httpResponse: HTTP_200_OK for successfully sent message with record
    """
    k_topic = record.topic_override or os.environ['INLAKE_TOPIC_INGRESS_UNSECURED']

    kkey, duuid = inlk_to_kafka_key(record.key_inlk)
    kkey = f"{kkey}/{namedgraph_override}" if (namedgraph_override != None) else kkey
    logger_i.info(f"Producing json-record with key {kkey} to Kafka topic {k_topic}")    

    await kafkaio.produce_message_str(topic=k_topic, 
                                      key=kkey,
                                      value=record.content)
    
    # msg = await kafkaio.consume()
    msg = await kafkaio.consume_key(key_to_wait_for=f"{duuid}://end")
    logger_i.debug(f"awaited streamgraphiti-job msg={msg}")
    response.status_code=status.HTTP_201_CREATED

    # return {"msg": f"INGRESS json-record with key={kkey} ; to Kafka topic={k_topic}", "duuid": {duuid}} # On garde cette magnifique archive
    return {"msg": f"Data upload success (key={kkey}, duuid={duuid})",
            "data": msg.value}


@router.post("/unsecured/record-json-and-binary")
async def produce_record_json_n_binary_unsecured( response: Response,
                                        record: Record = Depends(), 
                                        file: UploadFile = File(...),
                                        s3: S3 = Depends(_get_s3_client),
                                        kafkaio: KafkaAio = Depends(_get_client_kafka),
                                        ):
    """Send a record to inlake.records.topic with a) the schema name as key ; b) data file uploaded to s3 bucket
    Record's content is validated against the provided schema.
    Record's content features the uri adress for the uploaded data file

    Record's content is validated against the provided schema.
    The key can be formated as follow : 
        + "SCHEMA": specifying only the schema to be used
        + "SCHEMA/DESTINATION": specifying the schema and the database/part of database to be sent to. Convenient for the use of Named Graphs in Knowledge Bases

    TODO: The configuration of different filesystems put behind inlake is hardcoded for now, put it in .cfg if needed

    Args:
        response (Response): handler for fastapi's starlette http response
        record (Record): pydantic model for sent content, format query with params={"kafka_key": str, "content": str}
        file (UploadFile): binary file for s3 bucket upload. Defaults to File(...).
        client_kafka (AIOKafkaApi, dependance): asynchronous client for Kafka Producer. Defaults to Depends(get_client_kafka).
        client_schema (InlkSchemaRClient, dependance): exposes synchronous client for confluent_kafka Schema Registry. Defaults to Depends(get_client_schema).
        global_config (configParser, dependance): configuration. Defaults to Depends(get_global_config).

    Raises:
        HTTPException (HTTP_404_NOT_FOUND): when schema named "kafka_key" is not found on Schema Registry
        HTTPException (HTTP_422_UNPROCESSABLE_ENTITY): when schema does not feature a uri_adress to reference the s3 file upload
        HTTPException (HTTP_422_UNPROCESSABLE_ENTITY): when record's content is not compliant with schema
        HTTPException (HTTP_404_NOT_FOUND): when the bucket is not found on the s3 server

    Returns:
        httpResponse: HTTP_200_OK for successfully sent message with record
    """
    
    # topic
    k_topic = record.topic_override or os.environ['INLAKE_TOPIC_INGRESS_UNSECURED']

    # keys&content
    kkey, duuid = inlk_to_kafka_key(record.key_inlk)

    # logger_i.warning(f"type={type(record.content)} : content_asdict={record.content}")

    try:
        content_asdict = ast.literal_eval(record.content.replace('null', 'None'))
        logging.debug(f"content_asdict={content_asdict}")
        logging.debug(f"parse={parse_filesys_dirpath(content_asdict['resource_uri'])}")
        filesys_id, s3bucket_name, s3path = parse_filesys_dirpath(content_asdict["resource_uri"])
    except Exception as e:
        logger_i.error(e)
        raise HTTPException(status_code=422, 
                            detail=f"File destination shall be written in a 'resource_uri' field in json-params ; {record.content}")

    logger_i.info(f"(json) Producing metadatas json-record with key '{kkey}' to Kafka topic '{k_topic}'")

    # FILESYS
    if filesys_id == 's3':
        logger_i.info(f"(file) {content_asdict['resource_uri']}, Pushing to {filesys_id}://{s3bucket_name}{s3path} with s3_host={s3.config['s3_endpoint_url']}") # s3_symbolic_adress
        r = await s3.upload(file=file, url_s3=content_asdict['resource_uri'])
    else:
        raise HTTPException(status_code=422, 
                            detail=f"Filesystem filesystem={filesys_id} doesn not exist exist ; File destination shall be written in a 'resource_uri' field in json-params ; {record.content}")


    # # KAFKA
    await kafkaio.produce_message_str(topic=k_topic, key=kkey, value=record.content)
    
    msg = await kafkaio.consume_key(key_to_wait_for=f"{duuid}://end")
    logger_i.debug(f"awaited streamgraphiti-job msg={msg}")
    response.status_code=status.HTTP_201_CREATED

    return {"msg": f"Data upload success (key={kkey}, duuid={duuid})",
            "data": msg.value}
    # return {"msg": f"INGRESS json-record with key={kkey} ; to Kafka topic={k_topic}", "duuid": {duuid}} # Pour la postérité