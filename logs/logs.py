import logging
import os
import sys
from logging import Logger, LogRecord

from google.cloud.logging import Client as CloudLoggingClient

"""In order to add a logger to a new file, please copy and paste the following code snippet.

# start logger
from config.cfg import SCRIPT_RUN_ID
from typing import Optional
from logging import Logger
from logs import get_logger
_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER

"""

__LOG_FORMAT = "%(asctime)s %(levelname)s [%(processName)s] %(name)s - %(SCRIPT_RUN_ID)s - %(message)s"
__GCF_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(SCRIPT_RUN_ID)s - %(message)s"

logging.getLogger("google.cloud.logging_v2.handlers.transports.background_thread").setLevel(logging.WARN)
logging.getLogger("google.auth._default").setLevel(logging.WARN)
logging.getLogger("google.auth.transport._http_client").setLevel(logging.WARN)
logging.getLogger("google.auth.transport.requests").setLevel(logging.WARN)
logging.getLogger("urllib3.connectionpool").setLevel(
    logging.WARN)  # uncomment this row to keep getting logs from urllib
logging.getLogger("matplotlib").setLevel(logging.WARN)
logging.getLogger("matplotlib.font_manager").setLevel(logging.WARN)
logging.getLogger("aanalytics2").setLevel(logging.INFO)

logging.basicConfig(level=logging.DEBUG, format=__LOG_FORMAT)
delegate_log_record_factory = logging.getLogRecordFactory()


def log_record_factory(*args, **kwargs):
    """A custom log record factory that propagates the `SCRIPT_RUN_ID` into the log record.

    Uses the original `logging.getLogRecordFactory` to create the actual record instances.
    """

    from config.cfg import SCRIPT_RUN_ID

    record: LogRecord = delegate_log_record_factory(*args, **kwargs)
    record.SCRIPT_RUN_ID = SCRIPT_RUN_ID
    record.labels = {
        "script_run_id": SCRIPT_RUN_ID,
        "func_name": record.funcName,
        "process_name": record.processName
    }
    return record


logging.setLogRecordFactory(log_record_factory)

"""Indicates whether the Cloud Logging must be explicitly configured for GCF env.

We use Cloud Logging in GCF environment and `K_SERVICE` env is set while running in GCF.
"""
__SETUP_GCF_LOGGING = os.environ.get('GCF_LOCAL') is None and os.environ.get("K_SERVICE", None) is not None

"""Indicates whether the Cloud Logging must be explicitly configured for GCE env.
"""

def __setup_gcf_logging(logger: Logger):
    client = CloudLoggingClient()
    handler = client.get_default_handler()
    handler.setFormatter(logging.Formatter(__GCF_LOG_FORMAT))
    logger.handlers.clear()
    logger.addHandler(handler)


def get_logger(name: str) -> Logger:
    """Creates a pre-configured logger with the specified `name`."""
    result = logging.getLogger(name)
    result.setLevel(logging.DEBUG)
    result.propagate = False
    if __SETUP_GCF_LOGGING:
        __setup_gcf_logging(result)
    else:
        formatter = logging.Formatter(__LOG_FORMAT)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        result.addHandler(console_handler)
    return result


if __SETUP_GCF_LOGGING:
    logging.basicConfig(level=logging.DEBUG, format=__GCF_LOG_FORMAT)
    __setup_gcf_logging(logging.getLogger())
