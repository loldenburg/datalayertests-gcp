SCRIPT_RUN_ID = 'undefined'
from logging import Logger
from os import environ
from typing import Optional

from google.cloud import secretmanager

from logs import get_logger

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


GCP_PROJECT = environ.get("GCP_PROJECT", "your-project-id")  # todo set your project id (not your project name!) for local runs
GCP_PROJECT_NUMBER = environ.get("GCP_PROJECT_NUMBER",
                                 "123456789")  # todo set your project number (for secret manager) for local runs


def secret_mgr_get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    project_id = GCP_PROJECT_NUMBER
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    decoded = response.payload.data.decode("UTF-8")
    return decoded
