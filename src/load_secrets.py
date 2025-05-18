import os
from loguru import logger
from typing import TypedDict

class Secrets(TypedDict):
    minio_url: str
    minio_access_key: str
    minio_secret_key: str
    terracotta_db: str

def load_secrets(env: str) -> Secrets | Exception:
    if env == "dev":
        logger.info("Loaded in dev secrets")
        secrets: Secrets = {
            "minio_access_key": os.environ.get("MINIO_ACCESS_KEY_DEV"),
            "minio_secret_key": os.environ.get("MINIO_SECRET_KEY_DEV"),
            "minio_url": os.environ.get("MINIO_URL_DEV"), 
            "terracotta_db": os.environ.get("TERRACOTTA_DB_PATH_DEV")

        }
    elif env == "prod":
        logger.info("Loaded in prod secrets")
        secrets: Secrets = {
            "minio_access_key": os.environ.get("MINIO_ACCESS_KEY_PROD"),
            "minio_secret_key": os.environ.get("MINIO_SECRET_KEY_PROD"),
            "minio_url": os.environ.get("MINIO_URL_PROD"),
            "terracotta_db": os.environ.get("TERRACOTTA_DB_PATH_PROD")
        }

    else:
        return ValueError(f"Environment type provided was not dev or prod it was {env} which is unsupported")

    return secrets
    