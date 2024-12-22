from fastapi import FastAPI
from custom_types import AOI_Geojson, Secrets
from load_secrets import load_secrets

import requests
import pprint
import sqlalchemy as sa
from shapely import Polygon
from minio import Minio
from loguru import logger
import matplotlib.pyplot as plt
import pandas as pd
import shapely
import urllib3
import geopandas as gpd
import io
import os
import sqlite3

def update_insert_geometry_parquet_metadata(gdf_to_insert: gpd.GeoDataFrame, client: Minio, blob_parquet: str = "sentinel_2_metadata/uploaded_sentinel_2_footprints.parquet"): 
    
    geospatial_bucket_found = client.bucket_exists("sentinel-2-data")
    if not geospatial_bucket_found:
        client.make_bucket("sentinel-2-data")
        logger.info("Created bucket", "geospatial_bucket")
    else:
        logger.info("Bucket", "sentinel-2-data", "already exists") 
    
    gdf_to_insert['local_blob_storage_path'] = gdf_to_insert['Id'].apply(lambda x: f"{x}.SAFE.zip")

    # Check to see if the parquet file exists:
    try:
        parquet_exists = True
        parquet_response: urllib3.response.HTTPResponse = client.stat_object("sentinel-2-data", blob_parquet)
        logger.info(f"Sentinel 2 footprint parquet already exists {parquet_response}")
    
    except Exception as e:
        logger.info(f"{'sentinel_2_data/uploaded_sentinel_2_footprints.parquet'} did not exist in blob storage - creating it.")
        logger.info(e.with_traceback(None))
        parquet_exists = False

    if parquet_exists:
        # TODO: This can be set up to read the geopandas geodataframe directly from the minio client:
        existing_parquet = client.get_object("sentinel-2-data", blob_parquet)
        parquet_stream = io.BytesIO(existing_parquet.read())
        parquet_stream.seek(0)
        logger.info("Read in parquet file into memory buffer")

        existing_parquet_gdf: gpd.GeoDataFrame = gpd.read_parquet(parquet_stream)

        # Only inserting new footprints:
        new_footprints_gdf = gdf_to_insert[~gdf_to_insert['Id'].isin(existing_parquet_gdf["Id"])]
        if new_footprints_gdf.empty:
            logger.info(f"No New Ids extracted from the gdf to insert - not modifying existing parquet file.")
            return

        logger.info(f"Length of input gdf {len(gdf_to_insert)} vs Length of gdf rows to add: {len(new_footprints_gdf)}")

        new_parquet_gdf_to_upload: gpd.GeoDataFrame =  pd.concat([existing_parquet_gdf, new_footprints_gdf])

    else:
        logger.info("Building the parquet file from scratch")
        new_parquet_gdf_to_upload: gpd.GeoDataFrame = gdf_to_insert.copy()

    # Uploading the geodataframe to a buffer and streaming buffer to minio:
    file_upload_buffer_stream = io.BytesIO()
    new_parquet_gdf_to_upload.to_parquet(file_upload_buffer_stream, index=False, compression="None", write_covering_bbox=True, geometry_encoding="WKB")
    file_upload_buffer_stream.seek(0)

    try:
        upload_result = client.put_object(
            "sentinel-2-data", 
            blob_parquet, 
            file_upload_buffer_stream, 
            length=file_upload_buffer_stream.getbuffer().nbytes,
            content_type="application/vnd.apache.parquet"
        )
        logger.info(f"Uploaded new geoparquet file to blob storage")
        logger.info(upload_result)
    except Exception as e:
        logger.error(e.with_traceback(None))
        


def get_sqlite_engine(uri: str, enable_spatial: bool = False) -> sa.engine.Engine:
    logger.info(f"SQLITE Version:")
    logger.info(sqlite3.sqlite_version)
    logger.info(sqlite3.version)

    sqlite_engine: sa.engine.Engine = sa.create_engine(uri, echo=True)
    logger.info(f"Creating connection to the SQLITE database")

    engine = sa.create_engine(uri)

    if enable_spatial:
        with engine.connect() as conn:
            sqlite_conn = conn.connection
            sqlite_conn.enable_load_extension(True)
            
            sqlite_conn.execute("SELECT load_extension('mod_spatialite');")
            logger.info("Executing the spatialite extension loading")

            sqlite_conn.execute("SELECT InitSpatialMetadata();")
            logger.info("Initializing spatialite extension")


    with sqlite_engine.connect() as conn, conn.begin():
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

        if enable_spatial:
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

    all_tiles_df.duplicated(subset=['Id'], keep="first")

    logger.info("All tiles found for the AOIs provided:")
    logger.info(all_tiles_df)

    return all_tiles_df

def generate_geometry_from_catalogy_dict(geofootprint):
    aoi_polygon: Polygon = shapely.from_wkt(geofootprint.split(";")[1]) 
    return aoi_polygon

def ingest_raw_SAFE_files_blob(static_client: Minio, unique_copernicus_catalog_gdf: gpd.GeoDataFrame, access_token: str) -> list[str]:

    uploaded_copernicus_catalog_ids = []
    for index, row in unique_copernicus_catalog_gdf.iterrows():
        
        logger.info(f"Making request for compressed SAFE catalog for entry {row['Id']}")
        url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({row['Id']})/$value"
        headers = {"Authorization": f"Bearer {access_token}"}

        session = requests.Session()
        session.headers.update(headers)

        bucket_name = "sentinel-2-data"
        logger.info(f"Using bucket name {bucket_name}")

        memory_stream = io.BytesIO()
        zipped_response = session.get(url, headers=headers, stream=True)

        for chunk in zipped_response.iter_content(chunk_size=8192):
            #logger.info(f"Wrote chunk of SAFE data for tile {row['Id']} to in-memory buffer")
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
    else:
        logger.info(f"No difference between the uploaded ids and the input geodataframe - updating the metadata file tracking all footprints")
        update_insert_geometry_parquet_metadata(unique_copernicus_catalog_gdf, static_client)

    return uploaded_copernicus_catalog_ids

def determine_unique_records(catalog_df: pd.DataFrame | gpd.GeoDataFrame, secrets: Secrets) -> pd.DataFrame | gpd.GeoDataFrame:
    try:
        current_catalog_ids = ",".join([catalog_id for catalog_id in catalog_df['Id'].to_list()])
        unique_catalog_response = requests.get(f"{secrets['neo4j_url']}/v1/api/exists", params={'post_ids': current_catalog_ids})
        unique_catalog_response.raise_for_status()
        unique_catalog_json: list[dict] = unique_catalog_response.json()
        logger.info(f"Response from unique query: \n")
        pprint.pprint(unique_catalog_json)

    except requests.HTTPError as exception:
        logger.exception(f"Error in checking existing catalog nodes from API {exception}")
        return None
    
    duplicate_ids: list[str] = [post["id"] for post in unique_catalog_json if post['exists'] == True]

    unique_catalog_df = catalog_df[~catalog_df['Id'].isin(duplicate_ids)]
    logger.info(f"Before checking the API there were {len(catalog_df)}. After checking the API there were {len(unique_catalog_df)}")

    return unique_catalog_df

def insert_uploaded_SAFE_tiles_to_graph(inserted_copernicus_catalog_gdf: gpd.GeoDataFrame, secrets: Secrets) -> dict:

    uploaded_SAFE_nodes = []
    for _, row in inserted_copernicus_catalog_gdf.iterrows():

        logger.info(f"Appending row {row['Id']} to the node lists")
        uploaded_SAFE_nodes.append({
                "type":"node",
                "query_type":"CREATE",
                "labels": ['Metadata', 'Record', "SAFE", "Sentinel", "Imagery"],
                "properties": {
                    "id": row["Id"],
                    "name": row["Name"],
                    "collection_start_date": row['ContentDate']['Start'],
                    "collection_end_date": row['ContentDate']['End'],
                    "content_type": row["ContentType"],
                    "content_length": row["ContentLength"],
                    "origin_date": row["OriginDate"],
                    "publication_date": row["PublicationDate"],
                    "eviction_date": row["EvictionDate"],
                    "modification_date": row["ModificationDate"],
                    "external_s3_path": row["S3Path"],
                    "footprint": row["Footprint"],
                    "local_blob_storage_path": f"{row['Id']}.SAFE.zip"
                }
            })

    try:
        pprint.pprint(uploaded_SAFE_nodes)
        copernicus_catalog_creation_response = requests.post(f"{secrets['neo4j_url']}/v1/api/run_query", json=uploaded_SAFE_nodes)
        copernicus_catalog_creation_response.raise_for_status()
        created_catalog_nodes = copernicus_catalog_creation_response.json()
        logger.info(f"Inserterd {len(created_catalog_nodes)} SAFE catalogs into the database")
        
        return created_catalog_nodes

    except requests.HTTPError as e:
        logger.error(
        f"""Unable to create catalog nodes. Request returned with error: {str(e)} \n
            - {copernicus_catalog_creation_response.content}  \n
        """)
        return None


def process_sentinel_tiles(copernicus_catalog_df: pd.DataFrame):

    env_secrets: Secrets = load_secrets(os.environ.get("ENV", "dev"))

    copernicus_catalog_df['geometry'] = copernicus_catalog_df['Footprint'].apply(lambda x: generate_geometry_from_catalogy_dict(x))
    copernicus_catalog_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(copernicus_catalog_df, geometry="geometry", crs=4326)
    
    unique_copernicus_catalog_gdf = determine_unique_records(copernicus_catalog_gdf, env_secrets)
    if unique_copernicus_catalog_gdf.empty:
        logger.info(f"No unique records found to insert. Ending")
        return

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

    # Insert all of the SAFE files to minio blob and a metadata parquet:
    unique_copernicus_catalog_gdf.drop(columns=['@odata.mediaContentType'], inplace=True)
    ingested_copernicus_ids: list[str] = ingest_raw_SAFE_files_blob(client, unique_copernicus_catalog_gdf, access_token)
    ingested_copernicus_catalog_gdf = unique_copernicus_catalog_gdf[unique_copernicus_catalog_gdf["Id"].isin(ingested_copernicus_ids)]
    logger.info(f"Copernicus datasets that have been ingested: {ingested_copernicus_ids}")
    logger.info(ingested_copernicus_catalog_gdf)

    # Uploading all records to the graph database:
    uploaded_SAFE_nodes_response: dict | None = insert_uploaded_SAFE_tiles_to_graph(ingested_copernicus_catalog_gdf, env_secrets)
    if uploaded_SAFE_nodes_response is None:
        logger.error(f"Unable to insert tracking nodes into graph database")
    else:
        logger.info(f"Uploaded all ingested SAFE nodes to graph database")
        pprint.pprint(uploaded_SAFE_nodes_response) 
