# # from inlake.schemaregistry_basicapi import InlkSchemaRClient
# # from inlake.kafka_aioapi import AIOKafkaApi
# # import configparser

# # global_config = configparser.ConfigParser()
# # client_schema = InlkSchemaRClient()
# # client_kafka = AIOKafkaApi()
# # my_global = None

# from fast_clients.fast_files import S3, Local
# from fast_clients.fast_triplestore import TripleStore
# from fast_clients.fast_kafka import KafkaAio

# import os
# from dotenv import load_dotenv

# from app.loggers import logger_r

# load_dotenv()

# # TripleStore
# triplestore = TripleStore(
#     config_store={
#         "query_endpoint": os.environ["SPARQL_ENDPOINT_QUERY"],
#         "update_endpoint": os.environ["SPARQL_ENDPOINT_UPDATE"],
#     },
#     config={
#         "default_triples_root_uri": os.environ["SPARQL_DEFAULT_ROOT_URI"],
#         "default_named_graph_root_uri": os.environ["SPARQL_DEFAULT_NAMED_GRAPH_ROOT_URI"],
#         "thesaurus_named_graph_full_uris": ["http://astragale.cnrs.fr/graphs/th/all"],
#         "default_named_graph_name": os.environ["SPARQL_DEFAULT_NAMED_GRAPH_NAME"],
#         "datapip_sparql_select_path": "/data/sparql-select",  # Load `/data/sparql-select` directory with DockerFile or Compose File
#     },
# )
# logger_r.debug(f"TRIPLE STORE\nCONFIG_STORE={triplestore.config_store}\nCONFIG={triplestore.config}")

# # Files
# localfiles = Local()
# s3 = S3(
#     config={
#         "s3_endpoint_url": os.environ["MINIO_DOCKER_HOST"],
#         "s3_public_endpoint_url": os.environ["MINIO_HOST"],
#         "s3_key_id": os.environ["MINIO_ROOT_USER"],
#         "s3_access_key": os.environ["MINIO_ROOT_PASSWORD"],
#         "extra-args": {"ACL": "public-read"},
#     }
# )

# # Kafka
# kafkaio = KafkaAio(
#     config_basic={"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"]},
#     config_aio_producer={"bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVER"]},
#     config_aio_consumer={
#         "topic": "jobs-streamgraphiti",
#         "config": {
#             "bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
#             "group_id": "consumers_sgraphiti_jobs",
#         },
#     },
# )
