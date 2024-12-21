from fastapi import FastAPI
from custom_types import AOI_Geojson, Secrets
from load_secrets import load_secrets

import requests
import sqlalchemy as sa
from shapely import Polygon
from minio import Minio
from loguru import logger
import matplotlib.pyplot as plt
import pandas as pd
import shapely
import geopandas as gpd
import io
import os

def get_sqlite_engine(uri: str) -> sa.engine.Engine:

    sqlite_engine: sa.engine.Engine = sa.create_engine(uri, echo=True)
    logger.info(f"Creating connection to the SQLITE database")

    with sqlite_engine.connect() as conn, conn.begin():

        conn.execute(sa.text("SELECT load_extension('mod_spatialite');"))
        logger.info("Executing the spatialite extension loading")

        conn.execute(sa.text("SELECT InitSpatialMetadata();"))
        logger.info("Initalizing spatialite extension")

        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS copernicus_ingested_catalog (
                Id TEXT PRIMARY KEY,
                Name TEXT,
                ContentType TEXT,
                ContentLength TEXT,
                OriginDate TEXT,
                PublicationDate TEXT,
                EvictionDate TEXT,
                ModificationDate TEXT,
                Online TEXT,
                S3Path TEXT,
                Checksum TEXT,
                ContentDate TEXT,
                Footprint TEXT,
                GeoFootprint TEXT
            );
            """
        ))
        logger.info("Ran copernicus_ingested_catalog table creation if it does not exists")

        column_exists = conn.execute(sa.text("""
            SELECT COUNT (*) 
            FROM geometry_columns
            WHERE f_table_name = 'copernicus_ingested_catalog' AND f_geometry_column = 'geometry';
        """)).scalar()
        logger.info("Checked copernicus_ingested_catalog table for the persence of a geometry column")

        if not column_exists:
            conn.execute(sa.text("SELECT AddGeometryColumn('copernicus_ingested_catalog', 'geometry', 4326, 'POLYGON', 2);"))
            logger.info("geometry column did not exist - adding geometry column for polygon called 'geometry'")

    return sqlite_engine

def extract_sentinel_tiles_from_aoi(user_input_aoi: AOI_Geojson) -> pd.DataFrame | None:

    logger.info("Starting to process user provided AOI")

    data_collection = "SENTINEL-2"
    copernicus_catalog_dfs = []
    for polygon_verticies in user_input_aoi.features:

        try:
            aoi_polygon: Polygon = Polygon(polygon_verticies['geometry']['coordinates'][0])
            aoi_wkt_unformatted: str = shapely.to_wkt(aoi_polygon).replace(" ", "", 1)
            aoi_wkt_formatted = f"{aoi_wkt_unformatted}'"
            logger.info(f"Parsed aoi polygon from user aois: {aoi_wkt_formatted}")
        except Exception as e:
            logger.error(f"Error in extracting wkt from user provided geometry: {polygon_verticies}")
            logger.error(e.with_traceback(None))
            continue


        # Sentinel Catalogue requests:
        try:
            copernicus_catalog_response = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{aoi_wkt_formatted})")
            copernicus_catalog_response.raise_for_status()
            copernicus_catalog_json = copernicus_catalog_response.json()
            copernicus_catalog_df: pd.DataFrame = pd.DataFrame.from_dict(copernicus_catalog_json['value'])
            copernicus_catalog_dfs.append(copernicus_catalog_df)

            logger.info(f"Extracted and appended {len(copernicus_catalog_df)} copernicus records from AOI polygon")

        except Exception as e:
            logger.error(f"Error in making a request to the copernicus catalog to get records from AOI. {aoi_wkt_formatted}")
            logger.error(f"Status Code: {copernicus_catalog_response.status_code}, Content: {copernicus_catalog_response.raw}")
            logger.error(e.with_traceback(None))
            continue
        
    all_tiles_df = pd.concat(copernicus_catalog_dfs)
    if all_tiles_df.empty:
        logger.warning(f"No data take tiles found from any of the AOIs provided.")
        return None

    all_tiles_df = all_tiles_df.duplicated(subset=['Id'], keep="first")
    return all_tiles_df

def generate_geometry_from_catalogy_dict(geofootprint):
    aoi_polygon: Polygon = shapely.from_wkt(geofootprint.split(";")[1]) 
    return aoi_polygon

def ingest_SAFE_files(static_client: Minio, unique_copernicus_catalog_gdf: gpd.GeoDataFrame, access_token: str) -> list[str]:

    uploaded_copernicus_catalog_ids = []
    for index, row in unique_copernicus_catalog_gdf.iterrows():

        logger.info(f"Making request for compressed SAFE catalog for entry {row['Id']}")
        url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({row['Id']})/$value"
        headers = {"Authorization": f"Bearer {access_token}"}

        session = requests.Session()
        session.headers.update(headers)

        bucket_name = "test-bucket"
        logger.info(f"Using bucket name {bucket_name}")

        memory_stream = io.BytesIO()
        zipped_response = session.get(url, headers=headers, stream=True)

        for chunk in zipped_response.iter_content(chunk_size=8192):
            logger.info(f"Wrote chunk of SAFE data for tile {row['Id']} to in-memory buffer")
            memory_stream.write(chunk)

        memory_stream.seek(0)

        logger.info("Uploading to bucket:", bucket_name)
        logger.info("Object name:", f"{row['Id']}.SAFE.zip")
        logger.info("Content length:", memory_stream.getbuffer().nbytes)

        try:
            static_client.put_object(
                bucket_name=bucket_name,
                object_name=f"{row['Id']}.SAFE.zip",
                data=memory_stream,
                length=memory_stream.getbuffer().nbytes,
                content_type="application/zip"
            )

            logger.info(f"Uploaded {row['Id']} to blob storage - appending to list of ingested tiles")
            uploaded_copernicus_catalog_ids.append(row['Id'])
        except Exception as e:
            logger.error(f"Error in uploading tile {row['Id']} to blob storage. Skipping catalog tile...")
            logger.error(e.with_traceback(None))
            continue
    
    logger.info(f"Tile Ids processed and added to que: {uploaded_copernicus_catalog_ids}")
    if len(uploaded_copernicus_catalog_ids) != len(unique_copernicus_catalog_gdf):
        logger.warning(f"There is a difference between the input tiles and the tiles that were processed and uploaded. Input: {len(unique_copernicus_catalog_gdf)} Output: {len(uploaded_copernicus_catalog_ids)}")

    return uploaded_copernicus_catalog_ids

def process_sentinel_tiles(copernicus_catalog_df: pd.DataFrame):

    env_secrets: Secrets = load_secrets(os.environ.get("ENV", "dev"))

    copernicus_catalog_df['geometry'] = copernicus_catalog_df['Footprint'].apply(lambda x: generate_geometry_from_catalogy_dict(x))
    copernicus_catalog_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(copernicus_catalog_df, geometry="geometry", crs=4326)
    
    sqlite_engine = get_sqlite_engine(env_secrets['sqlite_uri'])

    with sqlite_engine.connect() as conn, conn.begin():
        existing_ids_df: pd.DataFrame = pd.read_sql(sa.text("SELECT Id FROM copernicus_ingested_catalog"), con=conn)
        logger.info(f"Ids queried from database {existing_ids_df['Id']}")

        unique_copernicus_catalog_gdf: pd.DataFrame = copernicus_catalog_gdf[~copernicus_catalog_gdf['Id'].isin(existing_ids_df['Id'])]
        logger.info(f"Total length of catalog df: {len(copernicus_catalog_gdf)}")
        logger.info(f"Unique catalog tiles to ingest: {len(unique_copernicus_catalog_gdf)}")

        # Step 3: Ingesting all of the items to the minio application:
        client = Minio(env_secrets['minio_url'], access_key=env_secrets['minio_access_key'], secret_key=env_secrets['minio_secret_key'], secure=False)
        logger.info(f"Created minio client")

        data = {
            "client_id": "cdse-public",
            "username": env_secrets["copernicus_username"],
            "password": env_secrets['copernicus_password'],
            "grant_type": "password"
        }
        auth_response = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data
        ).json()
        access_token = auth_response['access_token']

        ingested_copernicus_ids: list[str] = ingest_SAFE_files(client, unique_copernicus_catalog_gdf, access_token)
        ingested_copernicus_catalog_gdf = unique_copernicus_catalog_gdf[unique_copernicus_catalog_gdf["Id"].isin(ingested_copernicus_ids)]

        print(f"Copernicus datasets that have been ingested: {ingested_copernicus_ids}")
        ingested_copernicus_catalog_gdf.drop(columns=['@odata.mediaContentType', 'geometry'], inplace=True)
        print(ingested_copernicus_catalog_gdf)

        ingested_copernicus_catalog_gdf.to_sql("copernicus_ingested_catalog", con=conn, if_exists="append", index=False)
        print("Done!")
        
        df = pd.read_sql(sa.text("SELECT * FROM copernicus_ingested_catalog"), con=conn)

        print(df)
