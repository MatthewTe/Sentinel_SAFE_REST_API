from pystac_client import Client
import shapely
from loguru import logger
from minio import Minio
import tempfile
import subprocess
from urllib.request import urlretrieve
import os
import typing
from datetime import datetime
from pathlib import Path
import pystac

import terracotta as tc
from terracotta.drivers import TerracottaDriver
from src.load_secrets import load_secrets, Secrets

Sentine2STACProperties = typing.TypedDict(
    "Sentine2STACProperties",
    {
        "s2:generation_time": str,
        "grid:code": str,
    }
)
class Sentinel2STACItem(typing.TypedDict):
    id: str
    properties: Sentine2STACProperties

def insert_RBG_bands(sentinel_item: pystac.Item, minio_client: Minio, tc_driver: TerracottaDriver):
    colour_band_mapping = {
        "red": "B04",
        "green": "B03",
        "blue": "B02"
    }

    with tempfile.TemporaryDirectory() as tmpdirname:

        logger.info(f"Creating temp directory for processing {sentinel_item.id} - {tmpdirname}")

        new_collected_tile: Sentinel2STACItem = sentinel_item.to_dict()

        collection_date: datetime = datetime.strptime(new_collected_tile['properties']['s2:generation_time'], "%Y-%m-%dT%H:%M:%S.%fZ") 
        formatted_date: str = collection_date.strftime("%Y-%m-%d")
        collected_grid_code: str = new_collected_tile['properties']['grid:code']

        temp_file_base_name = f"{tmpdirname}/{new_collected_tile['id']}_{formatted_date}_{collected_grid_code}"
        logger.info(f"Created base file location for RBG tile bands: {temp_file_base_name}")
            
        
        for colour, band in colour_band_mapping.items():

            logger.info(f"Attempting to download {colour} ({band}) Band")
            urlretrieve(
                new_collected_tile['assets'][colour]['href'], 
                f"{temp_file_base_name}_{band}.tif"
            )
            logger.info(f"Downloaded {colour} band {band} to {temp_file_base_name}_{band}.tif")

        geotiff_output_subdir = Path(Path(tmpdirname) / "processed_geotiffs")
        os.makedirs(geotiff_output_subdir)

        logger.info(f"Beginning to convert tiffs to Geotiffs:")

        to_geotiff_results = subprocess.run(
            f"terracotta optimize-rasters -o {geotiff_output_subdir} --reproject {tmpdirname}/*.tif",
            shell=True,
            stdout=subprocess.PIPE
        )

        logger.info(to_geotiff_results.stdout.decode("utf-8"))

        for colour, band in colour_band_mapping.items():

            with tc_driver.connect():
                tc_driver.insert(
                    {
                        "id": new_collected_tile['id'],
                        "collection_date": formatted_date, 
                        "grid_code": collected_grid_code,
                        "band": band
                    },
                    f"{str(geotiff_output_subdir)}/{new_collected_tile['id']}_{formatted_date}_{collected_grid_code}_{band}.tif",
                    override_path=f"s3://sentinel-2-data/{new_collected_tile['id']}_{formatted_date}_{collected_grid_code}_{band}.tif"
                )
                logger.info(f"Inserted metadata for tile {new_collected_tile['id']} for band {band}")

                minio_client.fput_object(
                    "sentinel-2-data",
                    f"{new_collected_tile['id']}_{formatted_date}_{collected_grid_code}_{band}.tif",
                    f"{str(geotiff_output_subdir)}/{new_collected_tile['id']}_{formatted_date}_{collected_grid_code}_{band}.tif",
                )
                logger.info(f"Uploaded {new_collected_tile['id']}_{formatted_date}_{collected_grid_code}_{band}.tif to minio blob.")

def download_footprints_from_aoi(aoi: typing.Union[shapely.Polygon | shapely.MultiPolygon], secrets: Secrets):

    db_path = Path(secrets["terracotta_db"])
    driver: TerracottaDriver = tc.get_driver(db_path)

    if not db_path.is_file():
        driver.create(
            keys=["id", "collection_date", "grid_code", "band"]
        )

    catalog: Client = Client.open("https://earth-search.aws.element84.com/v1/")
    sentinel_2a_search_results = catalog.search(
        collections="sentinel-2-l2a",
        sortby="properties.datetime",
        intersects=aoi
    )

    client = Minio(
        secrets["minio_url"], 
        access_key=secrets['minio_access_key'], 
        secret_key=secrets["minio_secret_key"], 
        secure=False
    )

    geospatial_bucket_found = client.bucket_exists("sentinel-2-data")
    if not geospatial_bucket_found:
        client.make_bucket("sentinel-2-data")
        logger.info("Created bucket", "geospatial_bucket")
    else:
        logger.info("Bucket", "sentinel-2-data", "already exists") 

    for item in sentinel_2a_search_results.items():
        existing_raster: dict = driver.get_datasets(where={"id":item.id})
        if len(existing_raster) > 0:
            logger.warning(f"{item.id} already exists in database")
            continue
        
        insert_RBG_bands(
            sentinel_item=item,
            minio_client=client,
            tc_driver=driver
        )

