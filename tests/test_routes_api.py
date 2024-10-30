import os
from app.main import app
import pytest
from fastapi.testclient import TestClient
import json
from uuid import uuid4
import asyncio

from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv()

client = TestClient(app)



def test_api_root():
    response = client.get("/api")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello Astragale"}


@pytest.mark.skip(reason="WIP")
def test_api_pointproximity():
    with TestClient(app) as client:
        dummy_bw_id = "xxxxxxxx"
        good_pointlist = [(5.0,5.0,5.0),(1.0,2.0,3.0)]
        
        response = client.post(
            f"/api/builtworks/{dummy_bw_id}/annotations/pointsProximity",
            json={"list_of_points": good_pointlist} # f"{json.dumps(good_pointlist)}"
        )
        
        print(response.json())
        assert response.status_code == 200
        assert response.json() == {"msg": [[5.0, 5.0, 5.0], [1.0, 2.0, 3.0]]}



# class PointList(BaseModel):
#     list_of_points: List[Tuple[float, float, float]]
#     # list_of_points: List[str]

# @router.post("/builtworks/{builtwork_id}/annotations/pointsProximity")
# async def post_annotations_points_proximity(in_pl: PointList, builtwork_id):
#     return JSONResponse(content={'msg': in_pl.list_of_points})

# def test_api_pointproximity():
#     with TestClient(app) as client:
#         dummy_bw_id = "xxxxxxxx"
#         good_pointlist = [(5.0,5.0,5.0),(1.0,2.0,3.0)]
        
#         response = client.post(
#             f"/api/builtworks/{dummy_bw_id}/annotations/pointsProximity",
#             json={"list_of_points": good_pointlist} # f"{json.dumps(good_pointlist)}"
#         )
        
#         print(response.json())
#         assert response.status_code == 200
#         assert response.json() == {"msg": [[5.0, 5.0, 5.0], [1.0, 2.0, 3.0]]}