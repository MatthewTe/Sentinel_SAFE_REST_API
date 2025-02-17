import shapely
import json
import os

from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_basic_s2_rgb_ingestion():
    
    with open("./src/tests/test_aoi.json", "r") as f:
        obj: shapely.Geometry = shapely.from_geojson(f.read())
    
    print(shapely.to_wkt(obj))


    tt_aoi_geojson = None
    response = client.post(
        "/aoi_ingest/",
        json=tt_aoi_geojson

    )

    assert response.status_code == 200
