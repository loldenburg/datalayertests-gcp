# TODO for local runs, configure the following environment variable in PyCharm or similar IDE:
#  GCP_PROJECT_ID=your_project_id
#  --
# TODO if using the Big Query integration:
#   See readme.md for instructions on how to set up the BQ table
#   Default for the BQ table ID: `{project_id}.datalayer_errors.datalayer_error_logs`
#   If you want to use another table ID, provide them in the environment variable `BQ_DATALAYER_ERRORS_TABLE_ID`.
#   Value has to be in format `your-project.your_data_set.your_table`.

big_query_enabled = True  # TODO set this to True after configuring the BQ integration

import time
from datetime import timedelta, timezone, date
from logging import Logger
from typing import Optional
from json import loads
from os import environ
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.cloud import bigquery
from firestore import FireRef
from logs import get_logger

_LOGGER: Optional[Logger] = None


def log() -> Logger:
    global _LOGGER
    if not _LOGGER:
        _LOGGER = get_logger(__name__)
    return _LOGGER


def safe_get_index(li: list = None, idx: int = None, default: object = None):
    """Safely gets a list index, e.g. listname[4] and returns the default if not found.
    :param li: list to check
    :param idx: index to check
    :param default: Default value to return if index doesn't exist
    :return: value of index or default
    """
    if isinstance(li, list) is False:
        return default
    try:
        return li[idx]
    except IndexError:
        return default


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
                             "missing")  # TODO change to your preferred user ID for debugging (by default: Tealium Cookie ID)
    url_full = data_layer.get("url_full", "url_full missing")  # todo change to the UDO variable that contains the URL
    prod_id = data_layer.get("prod_id",
                             [])  # TODO change to the UDO variable that contains the product ID (or leave out)
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

    # write some error meta info to BigQuery for Monitoring Dashboard

    project_id = environ.get("GCP_PROJECT_ID", "missing")
    table_id = environ.get("BQ_DATALAYER_ERRORS_TABLE_ID", f"{project_id}.datalayer_errors.datalayer_error_logs")

    # write to bigquery if enabled
    if big_query_enabled is False:
        return "done"

    client = bigquery.Client()

    sql_write = f"INSERT INTO `{table_id}` " \
                f"(event_id, event_name, error_types, error_vars, logged_at, url_full, user_id, tealium_profile) " \
                f"VALUES " \
                f"('{log_id}', '{event_name}', '{';'.join(error_types)}', '{';'.join(error_vars)}', '{logged_at}', " \
                f"'{url_full}', '{user_id}', '{tealium_profile}')"
    query_job_write = client.query(sql_write)
    query_job_write.result()
    log().info(f"Wrote to BigQuery table {table_id}: {sql_write}")

    return "done"


if __name__ == "__main__":
    # to test, copy payloads from Cloud Logging into the brackets of the "loads" command
    test_payload = loads(  # just an example payload, replace with your own at will
        '{"script": "log_datalayer_error", "scriptType": "data_layer_tests", "errorData": {"data": {"fullOrRegExMatch": [{"var": "url_pathNoLang", "event": "view__ecommerce__checkout_cart", "message": "url_pathNoLang --> Full or Regex Match failed: Searched for --> /^\\\\/(warenkorb|panier)$/, but found --> \\"/checkout/adresse\\"\\n"}]}}, "dataLayer": {"hh": "10", "_csubtotal": "", "ut.account": "someaccount", "year": "2022", "ut.visitor_id": "017d0dad08f8000c38d785f8ac7305072017906a00900", "weekday": "7", "ut.event": "view", "sugg_env": "c", "url_full_initial": "https://www.somesite.ch/warenkorb", "_cpromo": "", "_ctotal": "", "prod_idUniques": 1, "tealium_library_name": "utag.js", "_cpdisc": [], "_ccat": [], "tealium_datasource": "xxxxx", "prod_action": ["checkout_cart"], "prod_cat_l1Uniques": 1, "prod_cat_l2Uniques": 1, "prod_cat_l3Uniques": 1, "rand_pct": 0, "url_rootDomain": "somesite.ch", "url_pathNoLangSearchCleaned": "/checkout/adresse", "cart_value": "69.00", "tealium_timestamp_local": "2022-09-17T10:34:18.692", "url_pathNoLang": "/checkout/adresse", "tealium_environment": "prod", "prod_quan": ["1"], "window_innerWidth": 1368, "screen_resolution": "1368x912", "teal_geo_region": "BE", "prod_posTotal": ["1"], "_ctax": "", "oss_env": "f", "timing_deltaToUtagInit": 10351, "tealium_visitor_id": "017d0dad08f8000c38d785f8ac7305072017906a00900", "minmin": "34", "tealium_event": "view", "ut.env": "prod", "prod_categoriesUniques": 1, "page_type": "Cart", "timing.dom_interactive_to_complete": 0, "dbg_localStorage": "y", "pageViewCounter": "1", "component_subcategory": ["checkout_cart"], "prod_pos": ["1"], "sec": "18", "timing.front_end": 0, "referrer_path": "/some-referrer", "referrer_pathSearchStripped": "/some-referrer", "debug_info": "ut4.48.202209121632|prod|someRSID|y|200|na", "ut.domain": "somesite.ch", "min": "34", "page_instanceid": "26b0d51d-3f78-49c7-85a8-94eccac23080_20220917103418706", "timing.dom_loading_to_interactive": 6056, "_cprice": ["69.00"], "_corder": "", "AdobeAnalyticsReportSuiteId": "someRSID", "utagQPushTimestamp": "1663403658690", "url_host": "www.somesite.ch", "tealium_timestamp_utc": "2022-09-17T08:34:18.692Z", "prod_priceTotal": ["69.00"], "order_currency": "CHF", "prod_cat_l2": ["Telefonie & Kommunikation"], "timing.fetch_to_interactive": 9168, "prod_cat_l1": ["IT & Multimedia"], "prod_cat_l3": ["Mobiltelefone"], "timing.timestamp": 1663403640349, "timing.fetch_to_complete": 0, "tealium_profile": "main", "teal_geo": "CH:BE:BIEL", "prod_purchasableHasService": ["pu:y_se:_lb:"], "user_loggedin": "y", "oss_version_env": "rev3.0::f", "teal_geo_country": "CH", "hits_on_page": 1, "tealium_session_event_number": "21", "ut.version": "ut4.48.202209121632", "url_full": "https://www.somesite.ch/checkout/adresse", "tealium_library_version": "4.48.0", "dd": "17", "bdTeal_human": "y", "page_title": "Warenkorb", "client_id": "26b0d51d-3f78-49c7-85a8-94eccac23080", "prod_id": ["1419624"], "prod_cat_hierarchy": ["IT & Multimedia/Telefonie & Kommunikation/Mobiltelefone/Mobiltelefon"], "prod_cat_wg": ["Mobiltelefon"], "url_pathNoLangSearchStripped": "/checkout/adresse", "timing.pathname": "/somepath", "screen_height": 912, "ut.session_id": "1663403148630", "reco_version": "rev2.0", "toolAA_rsId": "someRSID", "_cstore": "web", "tealium_random": "0141921913067466", "secsec": "18", "timing.dns": 0, "day": "17", "tealium_session_id": "1663403148630", "mm": "09", "timing.load": 0, "screen_width": 1368, "_ccountry": "", "teal_geo_city": "BIEL", "user_accid": "df1fff500dd1034774ea9d5bd4bbfaa7", "_cprodname": ["1419624"], "server_code": "200", "url_pathSearchCleaned": "/checkout/adresse", "timing.connect": 0, "ddhhmin": "171034", "component_category": ["ecommerce"], "toolAT_atResponseReady": "y", "month": "9", "bdCust_puppeteer": "n", "sugg_version": "rev1.4", "reco_version_env": "rev2.0::b", "event_name": "view__ecommerce__checkout_cart", "tealium_account": "competec", "timing.response": 2275, "prod_actionStr_prodsOnly": "checkout_cart", "_ccustid": "ef50f020c21ceb2be39b2c70bdad7db2", "_ccat2": [], "virtualPageView": "y", "referrer_full": "https://www.somesite.ch/somereferrer", "oss_version": "rev3.0", "bdCust_screenAnomaly": "n", "_cstate": "", "prod_price": ["69.00"], "user_email2": "8cfb4fa6a4809e811731c7d392c273192db8163b9191f80bba2046632a82ade2", "sugg_version_env": "rev1.4::c", "ut.profile": "main", "platform": "somesite.ch", "tool_mochaTestFlag": "1", "_csku": ["1419624"], "window_innerHeight": 769, "url_pathNoLangHash": "/checkout/adresse", "hour": "10", "_czip": "", "referrer_host": "www.somesite.ch", "_ctype": "", "_cbrand": [], "reco_env": "b", "timing.query_string": "", "toolGA_tid": "UA-12345-1", "prod_purchasable": ["y"], "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36", "page_language": "de", "url_path": "/checkout/adresse", "tealium_firstparty_visitor_id": "017d0dad08f8000c38d785f8ac7305072017906a00900", "toolAA_UDH_events": ["event5", "event83", "event104", "event122", "event23", "event22", "event19", "event18", "event17", "event82", "event81", "event80", "event60", "scView", "event4", "event2"], "url_pathSearchStripped": "/checkout/adresse", "url_protocolHostPath": "https://www.somesite.ch/checkout/adresse", "prod_cat_wgUniques": 1, "prod_actionStr": "checkout_cart", "bdTeal_humanCrit": "H:not_suspicious", "_cship": "", "persistPageContext": "n", "firstProd_id": "1419624", "firstProd_action": "checkout_cart", "timing_load": 9327, "timing.domain": "www.somesite.ch", "url_fullStripped": "https://www.somesite.ch/checkout/adresse", "sumProd_price": 69, "tealium_timestamp_epoch": "1663403658", "timing.fetch_to_response": 2939, "url_pathSearchCleanedHash": "/checkout/adresse", "toolAA_mcid_or_teal_vis_id": "16818726493262324901185650879489513159", "_ccity": "", "_cquan": ["1"], "environment_platform": "prod:somesite.ch", "timing.time_to_first_byte": 2939, "yyyymmdd": "20220917", "_ccurrency": "CHF", "timing_server": 8271, "timing_dom": 8888, "tealium_session_number": "3", "firstProd_cat_hierarchy": "IT & Multimedia/Telefonie & Kommunikation/Mobiltelefone/Mobiltelefon", "milsec": "706", "request_id": "1398aefc-d664-44e7-9e22-bf27065c73c1", "prod_stock": ["17"], "_cprod": ["1419624"], "cp.ut_aa_mcid": "16818726493262324901185650879489513159", "cp.s_ecid": "MCMID|16818726493262324901185650879489513159", "cp.utag_main_dc_visit": "3", "cp.searchenv": "rev3.0-F", "cp._gcl_au": "1.1.316031270.1663394552", "cp._hjIncludedInSessionSample": "0", "cp.utag_main_rpct": "00", "cp.demdex": "35189039298808559284121653509085176681", "cp.utag_main__pn": "7", "cp.utag_main_ses_id": "1663403148630", "cp._ga": "GA1.2.2072676331.1663394551"}, "eventName": "view__ecommerce__checkout_cart"}')
    run_script(payload=test_payload)
