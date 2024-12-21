import os
from loguru import logger
from custom_types import Secrets

def load_secrets(env: str) -> Secrets:
    if env == "dev":
        logger.info("Loaded in dev secrets")
        secrets: Secrets = {
            "copernicus_username": os.environ.get("COPERNICUS_USERNAME_DEV"),
            "copernicus_password": os.environ.get("COPERNICUS_PWD_DEV"),
            "minio_access_key": os.environ.get("MINIO_ACCESS_KEY_DEV"),
            "minio_secret_key": os.environ.get("MINIO_SECRET_KEY_DEV"),
            "minio_url": os.environ.get("MINIO_URL_DEV"), 
            "sqlite_uri": os.environ.get("SQLITE_URL_DEV")
        }
    elif env == "prod":
        logger.info("Loaded in prod secrets")
        secrets: Secrets = {
            "copernicus_username": os.environ.get("COPERNICUS_USERNAME_PROD"),
            "copernicus_password": os.environ.get("COPERNICUS_PWD_PROD"),
            "minio_access_key": os.environ.get("MINIO_ACCESS_KEY_PROD"),
            "minio_secret_key": os.environ.get("MINIO_SECRET_KEY_PROD"),
            "minio_url": os.environ.get("MINIO_URL_PROD"), 
            "sqlite_uri": os.environ.get("SQLITE_URL_PROD")
        }

    return secrets
    