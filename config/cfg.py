SCRIPT_RUN_ID = 'undefined'
from logging import Logger
from os import environ
from typing import Optional
from google.oauth2 import service_account
from google.cloud import secretmanager
from json import loads

from logs import get_logger

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


def secret_mgr_get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    project_id = environ.get("GCP_PROJECT_ID")
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    decoded = response.payload.data.decode("UTF-8")
    return decoded


def service_account_secret(secret_id: str) -> service_account.Credentials:
    """Retrieves a service account private key from the Secret Manager and creates
    Credentials from it.

    The private key is expected to be stored in the secret in plain, non-armored format.
    """
    sa_key = secret_mgr_get_secret(secret_id)
    sa_key_dict = loads(sa_key)
    result = service_account.Credentials.from_service_account_info(sa_key_dict)
    return result
