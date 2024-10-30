from fast_clients.fast_files import FileData, S3, Local
from fast_clients.fast_triplestore import TripleStore
from fast_clients.fast_kafka import KafkaAio

from app.sessions import triplestore, s3, localfiles, kafkaio
def _get_s3_client() -> S3:
    return s3

def _get_triplestore_client() -> TripleStore:
    return triplestore

def _get_localfiles_client() -> Local:
    return localfiles

def _get_client_kafka() -> KafkaAio:
    return kafkaio