import os, sys
import requests
import logging
import json
import uuid
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Examples of requests on the /api/builtsworks/...
# Some of the examples are included from the tests/test_route_api.py


INLAKE_HOST = "http://inlake.localhost"
USER_TEST_UNAME="bernard_test"
USER_TEST_PASWD="bXfE86W7ZJEJ3dd"
CURRENT_ID_BW = "4f7e8373"


def api_post_root():
    response = requests.post(f"{INLAKE_HOST}/api/", json={"motd": "Maintenant avec des chacals !"})

    logging.info(response.status_code)
    logging.info(json.dumps(response.json(), indent=2, sort_keys=True))

def api_get_bw():
    response = requests.get(f"{INLAKE_HOST}/api/builtworks",
                            auth=requests.auth.HTTPBasicAuth(USER_TEST_UNAME, USER_TEST_PASWD)
                            )

    logging.info(response.status_code)
    logging.info(json.dumps(response.json(), indent=2, sort_keys=True))

def api_post_bw_():
    dummy_bwt = f"Puente-{str(uuid.uuid4())[:3]}"
    response = requests.post(f"{INLAKE_HOST}/api/builtworks", json={"built_work_label": dummy_bwt},
                             auth=requests.auth.HTTPBasicAuth(USER_TEST_UNAME, USER_TEST_PASWD))

    logging.info(response.status_code)
    logging.info(json.dumps(response.json(), indent=2, sort_keys=True))


def api_get_bw_geoms():
    response = requests.get(f"{INLAKE_HOST}/api/builtworks/{CURRENT_ID_BW}")

    logging.info(response.status_code)
    logging.info(json.dumps(response.json(), indent=2, sort_keys=True))

def api_bw_pointproximity():
    my_pointlist = [(5.0, 5.0, 5.0), (1.0, 2.0, 3.0)]
    my_s3urls = ["s3://astragale-testbucket/C3DC_MicMac - Cloud - subs100k - portionA.pts"]
                #  "s3://astragale-testbucket/C3DC_MicMac - Cloud - subs100k - portionB.pts"]
    my_pointlist2 = [[1130.0,1980.0,50.0], 
                    [1132.0,1980.0,50.0], 
                    [1130.0,1982.0,50.0], 
                    [1130.0,1980.0,52.0]]
    response = requests.post(
        f"{INLAKE_HOST}/api/builtworks/{CURRENT_ID_BW}/annotations/pointsProximity",
        json={"clouds_s3urls": my_s3urls,
              "points":my_pointlist2 },
    )

    logging.info(response.status_code)
    logging.info(json.dumps(response.json(), indent=2, sort_keys=True))


def api_test():
    url = f"{INLAKE_HOST}/api/test/submit3"
    files = [('files', BytesIO(b'\x01'*32)), ('files', BytesIO(b'\x01'*32))]
    data = {'data': json.dumps({"name": "foo", "point": 0.13, "is_accepted": False})}
    resp = requests.post(url=url, data=data, files=files) 
    print(resp.json())

    url = f"{INLAKE_HOST}/api/test/submit3/dumdum/sub"
    resp = requests.post(url=url, data=data, files=files) 
    print(resp.json())

    url = f"{INLAKE_HOST}/api/test/submit3/dumdum/sub_and_param"
    params = {"creator": "Raphaello"}
    resp = requests.post(url=url, data=data, files=files, params=params) 
    print(resp.json())

def api_geom_test():
    builtwork_id = 'TourEiffel'
    url = f"{INLAKE_HOST}/api/builtworks/{builtwork_id}/geometries"
    files = {'file': ('filetitle', b'\x01'*32)} # BytesIO(b'\x01'*32) # ('myfile', BytesIO(b'\x01'*32))

    inlk_record_gameaps = {
        "scrs_label": f"Default SCRS",  # {bw_label}
        "uri_ressource": str("s3://astragale-testbucket/" + "bunny" + ".pts"),
        "uri_existing_builtwork": "http://astragale.map.cnrs.fr/triples/", # uris_omabacap["__uri__bw"],
        "uri_measure_type": "https://astragale.cnrs.fr/th/astra-actdiag/releve_geometre",
        "file_label": f"pts-{str(uuid.uuid4())[:3]}",
        "file_creator": "random.choice(DUM_PERSONS)",
        "file_format": "pts",
        "file_date": "1611",
    }

    data = {'record': json.dumps(inlk_record_gameaps)}

    params = {"geom_type": "Raphaello"}
    resp = requests.post(url=url, data=data, files=files, params=params) 
    print(resp.json())


# Main
if __name__ == "__main__":
    # api_get_bw()
    api_test()
    api_geom_test()
    # api_post_bw_()
    # api_post_root()
