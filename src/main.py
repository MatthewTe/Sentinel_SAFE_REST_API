from fastapi import FastAPI, BackgroundTasks
import os
import shapely
from src.api.STAC_catalog import download_footprints_from_aoi
from src.load_secrets import load_secrets, Secrets

from dotenv import load_dotenv

load_dotenv("/Users/matthewteelucksingh/Repos/Sentinel_SAFE_REST_API/config/dev_env.env")

app = FastAPI()
@app.post("/aoi_ingest/")
async def ingest_sentinel_AOIs(aoi_wkt: str, background_tasks: BackgroundTasks):

    secrets: Secrets = load_secrets("dev")
    aoi_geom: shapely.Geometry = shapely.from_wkt(aoi_wkt)

    background_tasks.add_task(download_footprints_from_aoi, aoi_geom, secrets)

    return {"message": "Background task for ingesting unique tiles for aoi begun"}