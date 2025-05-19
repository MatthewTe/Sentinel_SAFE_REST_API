from pydantic import BaseModel
import sqlalchemy as sa
import shapely
import json
import numpy as np
import pandas as pd
import geopandas as gpd
import typing
from loguru import logger

from src.load_secrets import load_secrets, Secrets

class AOI_Extent(BaseModel):
    aoi_name: typing.Optional[str]
    aoi_wkt: typing.Optional[str]

def insert_aoi_extent(aoi_name: str, aoi_wkt: str, secrets: Secrets) -> int | None:

    df = pd.DataFrame.from_dict({'aoi_name': [aoi_name], 'aoi_wkt': [aoi_wkt]})

    SQLITE_ENGINE: sa.Engine = sa.create_engine(secrets['db_uri'])
    with SQLITE_ENGINE.connect() as conn, conn.begin():
        inserted_rows: int | None = df.to_sql(name="aoi_extents", con=conn, if_exists="append")

    logger.info(f"Inserted {aoi_name} into database. Response {inserted_rows}")

    return inserted_rows

def _check_aoi_exists(aoi_names: list[str], secrets: Secrets) -> pd.DataFrame:

    try:
        SQLITE_ENGINE: sa.Engine = sa.create_engine(secrets['db_uri'])
        with SQLITE_ENGINE.connect() as conn, conn.begin():

            unique_aoi_query = sa.text(
                "SELECT aoi_name, aoi_wkt FROM aoi_extents WHERE aoi_name IN :aoi_extents_to_check"
            ).bindparams(sa.bindparam('aoi_extents_to_check', expanding=True))

            df = pd.read_sql(unique_aoi_query, con=conn, params={"aoi_extents_to_check": aoi_names})

    except Exception as e:
        logger.warning(e)
        df = pd.DataFrame()

    return df


def get_convex_hulls_for_aoi_extent(aoi_wkt: str, secrets: Secrets) -> gpd.GeoDataFrame:
    
    aoi_geometry = shapely.from_wkt(aoi_wkt)
    minx, miny, maxx, maxy = aoi_geometry.bounds

    SQLITE_ENGINE: sa.Engine = sa.create_engine(secrets['db_uri'])
    with SQLITE_ENGINE.connect() as conn, conn.begin():

        tile_hulls_df = pd.read_sql_query(
            sa.text("SELECT id, collection_date, convex_hull FROM metadata"),
            con=conn
            #params={"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy},
            #dtype={
            #    "bounds_east": np.float64,
            #    "bounds_west": np.float64,
            #    "bounds_north": np.float64,
            #    "bounds_south": np.float64,
            #}
        )
        
    #tile_hulls_df['convex_hull'] = tile_hulls_df['convex_hull'].apply(json.loads)
    tile_hulls_df['geometry'] = tile_hulls_df['convex_hull'].apply(shapely.from_geojson)
    gdf = gpd.GeoDataFrame(tile_hulls_df, geometry="geometry", crs=4326)

    return gdf.cx[minx:maxx, miny:maxy]