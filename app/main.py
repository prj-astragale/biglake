import os, sys
from fastapi import FastAPI, Request, Depends
from fastapi.responses import RedirectResponse
from fastapi.openapi.utils import get_openapi

# from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

import logging
from .loggers import logger_r

# from app.sessions import kafkaio

from app.routers import base_api


load_dotenv()
app = FastAPI()
# app.include_router(ingress.router)
app.include_router(base_api.router)
# templates = Jinja2Templates(directory='.')


#########
# STARTUP
#########
@app.on_event("startup")
async def startup_event():
    try:
        if os.environ['KAFKA_DISABLE_CONNECTION'] == 'True' or os.environ['KAFKA_DISABLE_CONNECTION'] is True:
            logger_r.warning(f"INLAKE KAFKA CONNECTION IS DISABLED")
            return
        else:
            pass
    except:
        pass

    logger_r.info(f"INLAKE CONNECTING TO KAFKA : {os.environ['KAFKA_BOOTSTRAP_SERVER']}")
    # await kafkaio.connect_and_start()
    # # Create inlake topics
    # inlk_topics = [
    #     os.environ["INLAKE_TOPIC_INGRESS_UNSECURED"],
    #     os.environ["INLAKE_TOPIC_INGRESS_SECURED"],
    # ]
    # logger_r.info(f"INLAKE KAFKA TOPICS : {inlk_topics}")
    # for topic in inlk_topics:
    #     if (topic in kafkaio._adminclient.list_topics(topic).topics) is False:
    #         kafkaio.create_topic(topic)
    #     else:
    #         logger_r.info(f"Skipping, Topic {topic} already exists")



# @app.on_event("shutdown")
# async def shutdown_event():
#     await kafkaio.stop()


@app.get("/")
async def home(req: Request):
    # return templates.TemplateResponse('home.html', {'request': req})
    return {"msg": "Hello Biglake"}


##############
# DOCS OPENAPI
##############
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="API astragale-biglake",
        version="0.0.9",
        # summary="This is a very custom OpenAPI schema",
        description="Here's a longer description of the custom **OpenAPI** schema",
        routes=app.routes,
    )
    # openapi_schema["info"]["x-logo"] = {
    #     "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    # }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
