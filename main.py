import json
from datetime import datetime
from http import HTTPStatus
from logging import Logger
from typing import Optional

from flask import Request, jsonify

from config import cfg
from datalayer_tests.log_datalayer_error import run_script as log_datalayer_error
from datalayer_tests.update_event_map import run_script as update_event_map
from logs import get_logger

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


token = cfg.secret_mgr_get_secret("error-log-token")


def data_layer_collector(request: Request):
    """
    Barebone function
    """
    run_id = "R" + str(datetime.now().strftime("%y%m%d-%H%M%S-%f")[:-3])  # gets a timestamp with milliseconds
    cfg.SCRIPT_RUN_ID = run_id
    request_json = request.get_json()
    event_payload = request_json.get("eventPayload")
    if type(event_payload) is str:
        # convert to dict
        event_payload = json.loads(event_payload)

    script = event_payload.get("script")
    script_type = event_payload.get("scriptType")

    log().info(f"Received request for script {script} of type {script_type}!")
    log().info(f"Request body: {request_json}")

    if request_json and "token" in request_json:
        if request_json.get("token") == token:
            if script == "update_event_map":
                update_event_map(payload=event_payload)
                log().info("update_event_map ran successfully")
            elif script == "log_datalayer_error":
                log_datalayer_error(payload=event_payload)
                log().info("Logged Error Successfully")
            return jsonify({"message": "Secret is correct"}), HTTPStatus.OK
        else:
            log().info("Secret is incorrect")
            return jsonify({"message": "Secret is incorrect"}), HTTPStatus.UNAUTHORIZED

    log().info(f'Received call with payload: {request_json}')

    return jsonify(status=HTTPStatus.UNAUTHORIZED)
