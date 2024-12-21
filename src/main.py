from fastapi import FastAPI, BackgroundTasks
from custom_types import AOI_Geojson
from api import process_sentinel_tiles, extract_sentinel_tiles_from_aoi

import requests
from shapely import Polygon
import pandas as pd
import shapely
import geopandas as gpd
from minio import Minio
import sys
import os
import io
from dotenv import load_dotenv

load_dotenv(os.environ.get("ENV_FILE_PATH"))

app = FastAPI()

@app.post("/aoi_ingest/")
async def ingest_sentinel_AOIs(aoi_polygons: AOI_Geojson, background_tasks: BackgroundTasks):

    sentinel_data_tiles_df: pd.DataFrame = extract_sentinel_tiles_from_aoi(aoi_polygons)
    background_tasks.add_task(process_sentinel_tiles, sentinel_data_tiles_df)

    return {
        "user_provided_aois": aoi_polygons,
        "total_sentinel_tiles": sentinel_data_tiles_df.to_records(),
        "message": "Background task for ingesting unique tile SAFE files"
    }
    
