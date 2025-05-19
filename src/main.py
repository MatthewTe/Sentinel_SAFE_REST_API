from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import os
import shapely
import pandas as pd
import terracotta as tc
from pathlib import Path
from terracotta.drivers import TerracottaDriver

from loguru import logger

from src.api.STAC_catalog import download_footprints_from_aoi
from src.api.areas_of_interest import AOI_Extent, insert_aoi_extent, get_convex_hulls_for_aoi_extent, _check_aoi_exists

from src.load_secrets import load_secrets, Secrets

from dotenv import load_dotenv

load_dotenv("/Users/matthewteelucksingh/Repos/Sentinel_SAFE_REST_API/config/dev_env.env")

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

'''
secrets: Secrets = load_secrets("dev")
tc.update_settings(
    RASTER_AWS_ACCESS_KEY=secrets['minio_access_key'],
    RASTER_AWS_SECRET_KEY=secrets['minio_secret_key'],
    RASTER_AWS_S3_ENDPOINT=secrets['minio_url']
)
db_path = Path(secrets["terracotta_db"])
driver: TerracottaDriver = tc.get_driver(db_path)

if not db_path.is_file():
    driver.create(
        keys=["id", "collection_date", "grid_code", "band"]
    )
'''

@app.post("/insert_unique_sen2_tiles/")
async def ingest_sentinel_AOIs(aoi_extent: AOI_Extent, background_tasks: BackgroundTasks):

    secrets: Secrets = load_secrets("dev")

    if hasattr(aoi_extent, "aoi_name"):
        existing_aois_df: pd.DataFrame = _check_aoi_exists(
            aoi_names=[aoi_extent.aoi_name],
            secrets=secrets
        )

        logger.info(f"Ingesting new tiles based on existing extent in db {aoi_extent.aoi_name}")    

        if existing_aois_df.empty:
            return JSONResponse(
                content={"message": f"No AOI in the database found with the name {aoi_extent.aoi_name}"},
                status_code=404
            )

        aoi_wkt: str = existing_aois_df[existing_aois_df['aoi_name'] == aoi_extent.aoi_name].iloc[0]['aoi_wkt']

    aoi_geom: shapely.Geometry = shapely.from_wkt(aoi_wkt)

    background_tasks.add_task(download_footprints_from_aoi, aoi_geom, aoi_extent.aoi_name, secrets)

    return {"message": "Background task for ingesting unique tiles for aoi begun"}

@app.post("/insert_aoi_geometry_to_db/")
async def insert_core_custom_AOIs(aois: list[AOI_Extent]):

    secrets: Secrets = load_secrets("dev")

    current_aois: pd.DataFrame = _check_aoi_exists(aoi_names=aois, secrets=secrets)
    if not current_aois.empty:
        unique_aois = [aoi for aoi in aois if aoi.aoi_name not in current_aois['aoi_name'].to_list()]
        aois = unique_aois

    inserted_responses: dict = {}
    for aoi in aois:
        aoi_inserted_response: int | None = insert_aoi_extent(aoi.aoi_name, aoi.aoi_wkt, secrets)
        inserted_responses[aoi.aoi_name] = aoi_inserted_response
    
    return inserted_responses

@app.get("/aoi_map/{aoi_name}", response_class=HTMLResponse)
async def display_aoi_leaflet_map(request: Request, aoi_name: str):

    secrets: Secrets = load_secrets("dev")
    existing_aois_df: pd.DataFrame = _check_aoi_exists(aoi_names=[aoi_name], secrets=secrets)

    if existing_aois_df.empty:
        return JSONResponse(
            content={"message": f"No AOI in the database found with the name {aoi_name}"},
            status_code=404
        )

    aoi_wkt: str = existing_aois_df[existing_aois_df['aoi_name'] == aoi_name].iloc[0]['aoi_wkt']
    aoi_geom = shapely.from_wkt(aoi_wkt)

    return templates.TemplateResponse(
        request=request, 
        name="aoi_leaflet_map.html", 
        context={
            "aoi_name": aoi_name,
            "geojson_geom": aoi_geom.__geo_interface__
        }
    )

@app.get("/aoi_tiles/{aoi_name}", response_class=HTMLResponse)
async def display_tile_bbox_leaflet(request: Request, aoi_name: str):

    secrets: Secrets = load_secrets("dev")
    existing_aois_df: pd.DataFrame = _check_aoi_exists(aoi_names=[aoi_name], secrets=secrets)

    if existing_aois_df.empty:
        return JSONResponse(
            content={"message": f"No AOI in the database found with the name {aoi_name}"},
            status_code=404
        )

    aoi_wkt: str = existing_aois_df[existing_aois_df['aoi_name'] == aoi_name].iloc[0]['aoi_wkt']
    aoi_geom = shapely.from_wkt(aoi_wkt)

    convex_hull_gdf = get_convex_hulls_for_aoi_extent(aoi_wkt=aoi_wkt, secrets=secrets)

    return templates.TemplateResponse(
        request=request, 
        name="aoi_leaflet_map_w_bbox.html", 
        context={
            "aoi_name": aoi_name,
            "aoi_geojson_geom": aoi_geom.__geo_interface__,
            "tile_convex_hulls": convex_hull_gdf.to_geo_dict()
        }
    )

