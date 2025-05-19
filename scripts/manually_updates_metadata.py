import sqlalchemy as sa
import pandas as pd
from src.load_secrets import load_secrets, Secrets

from dotenv import load_dotenv
from loguru import logger

aoi_name = 'example_south_china_sea'
load_dotenv("/Users/matthewteelucksingh/Repos/Sentinel_SAFE_REST_API/config/dev_env.env")
secrets: Secrets = load_secrets("dev")

SQLITE_ENGINE: sa.Engine = sa.create_engine(secrets['db_uri'])

with SQLITE_ENGINE.connect() as conn, conn.begin():
    # Update the metadata column for all rows
    update_stmt = sa.text("""
        UPDATE metadata
        SET metadata = json(:json_str)
    """)
    conn.execute(update_stmt, {"json_str": f'{{"aoi_name": "{aoi_name}"}}'})

    # Optional: verify the update
    tile_hulls_df = pd.read_sql_query(
        sa.text("SELECT id, collection_date, convex_hull, metadata FROM metadata"),
        con=conn
    )