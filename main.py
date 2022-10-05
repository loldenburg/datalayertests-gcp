import json
from datetime import datetime
from http import HTTPStatus
from logging import Logger
from typing import Optional
from os import environ
from flask import Request, jsonify

from config import cfg
from datalayer_tests.log_datalayer_error import run_script as log_datalayer_error
from logs import get_logger

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


def test_me(request: Request):
    """Just for testing if Cloud Function is working at all. Switch to main_handler later"""
    return jsonify({"message": "test_me ran successfully"}), HTTPStatus.OK


def main_handler(request: Request):
    """
    Barebone function. Receives a payload and routes it to a script of your choice
    """
    log().info("Starting main_handler")
    run_id = "R" + str(datetime.now().strftime("%y%m%d-%H%M%S-%f")[:-3])  # gets a timestamp with milliseconds
    cfg.SCRIPT_RUN_ID = run_id
    request_json = request.get_json()
    log().info(f'Received request: {request_json}')
    event_payload = request_json.get("eventPayload")
    if type(event_payload) is str:
        # convert to dict
        event_payload = json.loads(event_payload)

    script = event_payload.get("script")

    log().info(f"Received request for script {script}.")
    log().info(f"Request body: {request_json}")

    if request_json and "token" in request_json:
        token = environ.get("ERROR_LOG_TOKEN")
        if request_json.get("token") == token:
            if script == "log_datalayer_error":
                log_datalayer_error(payload=event_payload)
                log().info("Logged Error Successfully")
            return jsonify({"message": "Data Layer Test Handler successfully finished"}), HTTPStatus.OK
        else:
            log().info("Secret is incorrect")
            return jsonify({"message": "Secret is incorrect"}), HTTPStatus.UNAUTHORIZED

    return jsonify(status=HTTPStatus.UNAUTHORIZED)
