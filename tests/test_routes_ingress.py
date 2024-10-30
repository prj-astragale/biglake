import os
from app.main import app
import pytest
from fastapi.testclient import TestClient
import json
from uuid import uuid4
import asyncio

from fast_clients.fast_kafka import KafkaAio
from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv()

client = TestClient(app)

# python -m pytest -o log_cli=true --log-cli-level=INFO
# Future: dependency injection for testing environment
# @pytest.fixture.... set minio/kafka/triplestore as localhost

# @pytest.mark.skip(reason="wip")
def test_upload_s3():
    with TestClient(app) as client:
        data = b'\x01'*2048
        response = client.post(
            "/ingress/upload_s3",
            params={"url_filestore": "s3://astragale-testbucket/data2.bin"},
            files={'file': ('filetitle', data)}
        )
        
        print(response.json())
        assert response.status_code == 200


# @pytest.mark.skip(reason="wip")
def test_unsecured_record_json():
    with TestClient(app) as client:
        inlkey = "0000-noschema_json"
        good_record = {"firstname": "Bob", "lastname": "Du Buc Du Ferray", "job": "Pianiste"}
        
        response = client.post(
            "/ingress/unsecured/record-json",
            json={"key_inlk": f"{inlkey}", 
                  "content": f"{json.dumps(good_record)}",
                  "url_filestore": None,
                  "topic_override": "inlake-gateway-test"}
        )
        
        print(response.json())
        assert response.status_code == 201

# @pytest.mark.skip(reason="wip")
def test_unsecured_record_json_bin():
    with TestClient(app) as client:
        inlkey = "0000-noschema_jsonbin"
        s3_symbolic = "s3://astragale-testbucket/madata.bin"
        data = b'\x01'*2048
        good_record = {"firstname": "Bob", "lastname": "Du Buc Du Ferray", "job": "Pianiste", "uri_ressource": s3_symbolic}
        
        response = client.post(
            "/ingress/unsecured/record-json-and-binary",
            params={"key_inlk": f"{inlkey}", 
                  "content": f"{json.dumps(good_record)}",
                  "url_filestore": None,
                  "topic_override": "inlake-gateway-test"},
            files={'file': ('filetitle', data)}
        )
        
        print(response.json())
        assert response.status_code == 201

        # # clean
        # o = urlparse(s3_symbolic)
        # asyncio.run(delete_file_from_s3(bucket=o.netloc, key=o.path))