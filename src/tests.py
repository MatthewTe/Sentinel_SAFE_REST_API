import io
import pandas as pd
import geopandas as gpd

from minio import Minio
from dotenv import load_dotenv

from api import update_insert_geometry_parquet_metadata, generate_geometry_from_catalogy_dict
from load_secrets import load_secrets

def test_geoparquet_updating():
    load_dotenv("../config/dev_env.env")
    secrets = load_secrets("dev")

    test_footprints_df = pd.read_csv("../data/example_copernicus_sentinel_2_response.csv").iloc[0:10]
    test_footprints_df['geometry'] = test_footprints_df['Footprint'].apply(lambda x: generate_geometry_from_catalogy_dict(x))
    test_footprints_gdf = gpd.GeoDataFrame(test_footprints_df, geometry='geometry', crs=4326)

    print(test_footprints_gdf)
    
    client = Minio(secrets['minio_url'], secrets['minio_access_key'], secrets['minio_secret_key'], secure=False)

    update_insert_geometry_parquet_metadata(test_footprints_gdf, client)

    existing_parquet_file = client.get_object("sentinel-2-data", "sentinel_2_metadata/uploaded_sentinel_2_footprints.parquet")
    parquet_stream = io.BytesIO(existing_parquet_file.read())
    parquet_stream.seek(0)
    print("Read in parquet file into memory buffer")

    existing_parquet_gdf: gpd.GeoDataFrame = gpd.read_parquet(parquet_stream)
    print(existing_parquet_gdf)
    print(len(test_footprints_gdf), len(existing_parquet_gdf))

    test_second_footprints_df = pd.read_csv("../data/example_copernicus_sentinel_2_response.csv").iloc[10:20]
    test_second_footprints_df['geometry'] = test_second_footprints_df['Footprint'].apply(lambda x: generate_geometry_from_catalogy_dict(x))
    test_second_footprints_gdf = gpd.GeoDataFrame(test_second_footprints_df, geometry='geometry', crs=4326)

    update_insert_geometry_parquet_metadata(test_second_footprints_gdf, client)
    
    existing_parquet_file_two = client.get_object("sentinel-2-data", "sentinel_2_metadata/uploaded_sentinel_2_footprints.parquet")
    parquet_stream_two = io.BytesIO(existing_parquet_file_two.read())
    parquet_stream_two.seek(0)
    print("Read in parquet file into memory buffer")

    existing_parquet_gdf_two: gpd.GeoDataFrame = gpd.read_parquet(parquet_stream_two)
    print(existing_parquet_gdf_two)
    print(len(existing_parquet_gdf_two), len(existing_parquet_gdf), len(test_second_footprints_gdf))



if __name__ == "__main__":
    test_geoparquet_updating()
