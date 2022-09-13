from io import StringIO
from logging import Logger
from typing import Optional

from logs import get_logger
from storage import gcs

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


def run_script(payload=None):
    """
    updates the data layer event map in a GCS bucket
    """
    log().info(f"starting update_event_map script with payload: {payload}")

    event_map = payload.get("eventMap")
    datafile_buffer = StringIO()
    datafile_buffer.write(event_map)
    # version without cache for fast update, Mocha and Debugging in Tealium Functions
    blob_nocache = gcs.upload_file("automated_tests/eventMap.js", datafile_buffer,
                                   bucket_name="competec-analytics-prod-public",
                                   public_bucket=True, no_cache=True, file_encoding="utf-8")
    # version with caching active for Tealium Functions live - commented out because had to switch back to JS
    # blob_cached = gcs.upload_file("automated_tests/eventMap-cached.js", datafile_buffer, bucket_name="dim28public",
    #                              public_bucket=True, file_encoding="utf-8")

    msg = f"Event maps were updated at {blob_nocache}"  # and {blob_cached}"

    log().info(msg)
    return "done"


if __name__ == "__main__":
    run_script(force_run=True)
