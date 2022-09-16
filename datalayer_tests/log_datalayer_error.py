import time
from datetime import timedelta, timezone, date
from logging import Logger
from typing import Optional

from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.cloud import monitoring_v3
from config.cfg import GCP_PROJECT

from firestore import FireRef
from logs import get_logger
from util.helpers import safe_get_index

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


def run_script(payload=None):
    """
    triggered by Tealium Functions. Logs the data layer errors and error messages to GC Logging and Firestore
    """
    # put SCRIPT_RUN_ID here to avoid using a script run ID from previous runs due to cloud function caching
    from config.cfg import SCRIPT_RUN_ID
    log().info(f"Starting data layer error logging script")

    data_layer = payload.get("dataLayer")
    error_data = payload.get("errorData").get("data")
    event_name = payload.get("eventName", "event_name missing")
    user_id = data_layer.get("tealium_visitor_id",
                             "missing")  # todo change to your preferred user ID for debugging (by default: Tealium Cookie ID)
    url_full = data_layer.get("url_full", "url_full missing")  # todo change to the UDO variable that contains the URL
    prod_id = data_layer.get("prod_id",
                             [])  # todo change to the UDO variable that contains the product ID (or leave out)
    prod_id = safe_get_index(prod_id, 0, None)
    tealium_profile = data_layer.get("tealium_profile", "missing")
    logged_at = DatetimeWithNanoseconds.now(timezone.utc)
    # create ID that is decreasing over time so that the most recent errors are at the top of the list in Firestore
    jan1_2100 = time.mktime((date(2100, 1, 1)).timetuple())
    logged_at_ts = time.mktime(logged_at.timetuple())
    decreasing_ts = int(jan1_2100 - logged_at_ts)
    log_id = f"{decreasing_ts}-{SCRIPT_RUN_ID}"
    msg = f"Error Log ID: {log_id},\nEvent: {event_name},\nURL: {url_full},\nUser ID: {user_id},\n"
    error_types = []
    error_vars = []
    for error_type in error_data:
        msg += f"Errors of type: {error_type}\n"  # eg "populatedAndOfType"
        error_types.append(error_type)
        for error in error_data[error_type]:
            msg += f"{error.get('var', 'var missing')}: {error.get('message', 'message missing')}\n"
            error_vars.append(error.get('var', 'var missing'))

    log().info(msg)
    # log().info(f"Full Data Layer: \n{data_layer}")
    expire_at = logged_at + timedelta(
        days=4)  # together with TTL policy in Firestore, this will make the document get deleted after n days
    log().info("Error Logging completed. Now posting to Firestore")
    firedoc = {
        "dataLayer": data_layer,
        "errorData": error_data,
        "errorTypes": error_types,
        "errorVars": error_vars,
        "eventName": event_name,
        "id": SCRIPT_RUN_ID,
        "loggedAt": logged_at,
        "prod_id": prod_id,
        "url_full": url_full,
        "user_id": user_id,
        "tealium_profile": tealium_profile,
        "expireAt": expire_at
    }
    FireRef.collectionDynamic("dataLayerErrorLogs").document(log_id).set(firedoc)
    log().info(f"Stored Data Layer and Error Data to Firestore document ID {log_id}")

    # fire metric for Monitoring Dashboard
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{GCP_PROJECT_ID}"

    series = monitoring_v3.TimeSeries()
    series.metric.type = "custom.googleapis.com/datalayer_error"
    series.metric.labels["eventName"] = f"{event_name}"
    series.metric.labels["tealium_profile"] = f"{tealium_profile}"
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {"end_time": {"seconds": seconds, "nanos": nanos}}
    )
    point = monitoring_v3.Point({"interval": interval, "value": {"int64_value": 1}})
    series.points = [point]
    client.create_time_series(name=project_name, time_series=[series])

    return "done"


if __name__ == "__main__":
    run_script()
