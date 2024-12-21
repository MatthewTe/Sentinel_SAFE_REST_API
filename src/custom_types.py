from typing import TypedDict
from pydantic import BaseModel

class PolygonGeometryObject(BaseModel):
    type: str
    coordinates: list[list[list[float, float]]]

class GeoJsonPolygonGeometry(BaseModel):
    geometry: PolygonGeometryObject

class AOI_Geojson(BaseModel):
    type: str
    features: list[dict]

class Secrets(TypedDict):
    minio_url: str
    minio_access_key: str
    minio_secret_key: str
    copernicus_username: str
    copernicus_password: str
    neo4j_rest_api: str