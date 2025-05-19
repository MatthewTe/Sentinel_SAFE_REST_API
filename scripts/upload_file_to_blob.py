from minio import Minio
import io

from src.load_secrets import load_secrets, Secrets

from dotenv import load_dotenv
from loguru import logger

load_dotenv("/Users/matthewteelucksingh/Repos/Sentinel_SAFE_REST_API/config/dev_env.env")
secrets: Secrets = load_secrets("dev")


if __name__ == "__main__":

    client = Minio(
        secrets["minio_url"], 
        access_key=secrets['minio_access_key'], 
        secret_key=secrets["minio_secret_key"], 
        secure=False
    )

    geospatial_bucket_found = client.bucket_exists("sentinel-2-data")
    if not geospatial_bucket_found:
        client.make_bucket("sentinel-2-data")
        logger.info("Created bucket geospatial_bucket")
    else:
        logger.info(f"Bucket sentinel-2-data already exists") 

    with open(
        "/Users/matthewteelucksingh/Repos/Sentinel_SAFE_REST_API/S2A_49QGC_20170102_0_L2A_2020-07-18_MGRS-49QGC_B04.tif",
        "rb"
    ) as f:
        data = f.read()       
        mem_stream = io.BytesIO(data)

        client.put_object(
            "sentinel-2-data",
            f"example.tif",
            mem_stream,
            length=mem_stream.getbuffer().nbytes,
            part_size=10*1024*1024,
        )