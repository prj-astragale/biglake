from fastapi import (
    HTTPException,
    status,
    File,
    UploadFile,
    Request,
    Form
)
from pydantic import BaseModel, ValidationError, model_validator
from pydantic import BaseModel, ValidationError
from fastapi.encoders import jsonable_encoder
from typing import List, Tuple, Annotated, Optional
import json

# Geometries
class RecordLightGaxxxps(BaseModel):
    scrs_label:             str
    builtwork_type_uri:     Optional[str] = None
    measure_type_uri:       Optional[str] = None
    model_type_uri:         Optional[str] = None
    scrs_geom_label:        Optional[str] = None
    file_creator:           str
    file_date:              str    

    @model_validator(mode='before')
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value

# Annotation - observation
class RecordLightGaxaalt(BaseModel):
    feature_label:          str
    feature_type_uri:       str
    observation_type_uri:   str
    file_creator:           str
    file_date:              str    

    @model_validator(mode='before')
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value

# AnnotationLayer
class RecordLightAlag(BaseModel):
    annotation_layer_label:     str
    annotation_tool_type_uri:   str
    file_creator:               str
    file_date:                  str    

    @model_validator(mode='before')
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value

# Simulations
class RecordLightSaasg(BaseModel):
    simulation_label:           str
    simulation_type_uri:        str
    file_creator:               str
    file_date:                  str    

    @model_validator(mode='before')
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value

# Altération->Endommagement
class RecordLightSedffea(BaseModel):
    damaging_quantity_uri:      str
    damaging_value:             str
    damaging_type_uri:          str
    inference_type_uri:         str

    @model_validator(mode='before')
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value


# Processes
class PointMatchData(BaseModel):
    clouds_s3urls: List[str]
    points: List[Tuple[float, float, float]]
    # list_of_points: List[str]

class LightPointMatchData(BaseModel):
    # clouds_s3urls: List[str]
    points: List[Tuple[float, float, float]]
    # list_of_points: List[str]


# TESTS
class DataChecker:
    def __init__(self, name: str):
        self.name = name

    def __call__(self, data: str = Form(...)):
        try:
            model = models[self.name].parse_raw(data)
        except ValidationError as e:
            raise HTTPException(
                detail=jsonable_encoder(e.errors()),
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )
        return model

class Base(BaseModel):
    name: str
    point: Optional[float] = None
    is_accepted: Optional[bool] = False


models = {"base": Base, "other": Base}
base_checker = DataChecker("base")
other_checker = DataChecker("other")