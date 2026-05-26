# %%
import traceback
import re
import base64, requests, time, pytz, logging, os, sys, dotenv, io, zipfile
from fitparse import FitFile, FitParseError
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from influxdb_client_3 import InfluxDBClient3, InfluxDBError
import xml.etree.ElementTree as ET
from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
)
garmin_obj = None
banner_text = """

*****  █▀▀ ▄▀█ █▀█ █▀▄▀█ █ █▄ █    █▀▀ █▀█ ▄▀█ █▀▀ ▄▀█ █▄ █ ▄▀█  *****
*****  █▄█ █▀█ █▀▄ █ ▀ █ █ █ ▀█    █▄█ █▀▄ █▀█ █▀  █▀█ █ ▀█ █▀█  *****

______________________________________________________________________

By Arpan Ghosh | Please consider supporting the project if you love it
______________________________________________________________________

"""
print(banner_text)

env_override = dotenv.load_dotenv("override-default-vars.env", override=True)
if env_override:
    logging.warning("System ENV variables are overridden with override-default-vars.env")

# %%
INFLUXDB_VERSION = os.getenv("INFLUXDB_VERSION",'1') # Your influxdb database version (accepted values are '1' or '3')
assert INFLUXDB_VERSION in ['1','3'], "Only InfluxDB version 1 or 3 is allowed - please ensure to set this value to either 1 or 3"
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST",'your.influxdb.hostname') # Required
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8086)) # Required
INFLUXDB_USERNAME = os.getenv("INFLUXDB_USERNAME", 'influxdb_username') # Required
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD", 'influxdb_access_password') # Required
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", 'GarminStats') # Required
INFLUXDB_V3_ACCESS_TOKEN = os.getenv("INFLUXDB_V3_ACCESS_TOKEN",'') # InfluxDB V3 Access token, required only for InfluxDB V3
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", 'default') # required only for InfluxDB V3 
TOKEN_DIR = os.getenv("TOKEN_DIR", "~/.garminconnect") # optional
GARMINCONNECT_EMAIL = os.environ.get("GARMINCONNECT_EMAIL", None) # optional, asks in prompt on run if not provided
GARMINCONNECT_PASSWORD = base64.b64decode(os.getenv("GARMINCONNECT_BASE64_PASSWORD")).decode("utf-8") if os.getenv("GARMINCONNECT_BASE64_PASSWORD") != None else None # optional, asks in prompt on run if not provided
GARMINCONNECT_IS_CN = True if os.getenv("GARMINCONNECT_IS_CN") in ['True', 'true', 'TRUE','t', 'T', 'yes', 'Yes', 'YES', '1'] else False # optional if you are using a Chinese account
GARMIN_DEVICENAME = os.getenv("GARMIN_DEVICENAME", "Unknown")  # optional, attempts to set the name automatically if not given
GARMIN_DEVICEID = os.getenv("GARMIN_DEVICEID", None)  # optional, attempts to set the id automatically if not given
AUTO_DATE_RANGE = False if os.getenv("AUTO_DATE_RANGE") in ['False','false','FALSE','f','F','no','No','NO','0'] else True # optional
MANUAL_START_DATE = os.getenv("MANUAL_START_DATE", None) # optional, in YYYY-MM-DD format, if you want to bulk update only from specific date
MANUAL_END_DATE = os.getenv("MANUAL_END_DATE", datetime.today().strftime('%Y-%m-%d')) # optional, in YYYY-MM-DD format, if you want to bulk update until a specific date
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO") # optional
FETCH_FAILED_WAIT_SECONDS = int(os.getenv("FETCH_FAILED_WAIT_SECONDS", 1800)) # optional
RATE_LIMIT_CALLS_SECONDS = int(os.getenv("RATE_LIMIT_CALLS_SECONDS", 5)) # optional
MAX_CONSECUTIVE_500_ERRORS = int(os.getenv("MAX_CONSECUTIVE_500_ERRORS", 10)) # optional, maximum consecutive HTTP 500 errors before continuing without retrying
INFLUXDB_ENDPOINT_IS_HTTP = False if os.getenv("INFLUXDB_ENDPOINT_IS_HTTP") in ['False','false','FALSE','f','F','no','No','NO','0'] else True # optional
GARMIN_DEVICENAME_AUTOMATIC = False if GARMIN_DEVICENAME != "Unknown" else True # optional
UPDATE_INTERVAL_SECONDS = int(os.getenv("UPDATE_INTERVAL_SECONDS", 300)) # optional
FETCH_SELECTION = os.getenv("FETCH_SELECTION", "daily_avg,sleep,steps,heartrate,stress,breathing,hrv,fitness_age,vo2,activity,race_prediction,body_composition,lifestyle,weather") # additional available values are lactate_threshold,training_status,training_readiness,hill_score,endurance_score,blood_pressure,hydration,solar_intensity which you can add to the list seperated by , without any space
ACTIVITY_TYPE_FILTER = [t.strip().lower() for t in os.getenv("ACTIVITY_TYPE_FILTER", "").split(",") if t.strip()] # optional, comma-separated list of activity typeKeys to import only specific activity types. Leave empty to import all. Known typeKeys: running,treadmill_running,indoor_running,cycling,indoor_cycling,road_biking,mountain_biking,walking,hiking,mountaineering,strength_training,hiit,indoor_cardio,elliptical,lap_swimming,open_water_swimming,rock_climbing,indoor_climbing,tennis_v2,kayaking_v2,boating_v2,multi_sport,other
LACTATE_THRESHOLD_SPORTS = os.getenv("LACTATE_THRESHOLD_SPORTS", "RUNNING").upper().split(",") # Garmin currently implements RUNNING, but has provisions for CYCLING, and SWIMMING
KEEP_FIT_FILES = True if os.getenv("KEEP_FIT_FILES") in ['True', 'true', 'TRUE','t', 'T', 'yes', 'Yes', 'YES', '1'] else False # optional
FIT_FILE_STORAGE_LOCATION = os.getenv("FIT_FILE_STORAGE_LOCATION", os.path.join(os.path.expanduser("~"), "fit_filestore"))
ALWAYS_PROCESS_FIT_FILES = True if os.getenv("ALWAYS_PROCESS_FIT_FILES") in ['True', 'true', 'TRUE','t', 'T', 'yes', 'Yes', 'YES', '1'] else False # optional, will process all FIT files for all activities including indoor ones lacking GPS data
REQUEST_INTRADAY_DATA_REFRESH = True if os.getenv("REQUEST_INTRADAY_DATA_REFRESH") in ['True', 'true', 'TRUE','t', 'T', 'yes', 'Yes', 'YES', '1'] else False # optional, This requests data refresh for the intraday data (older than 6 months) - see issue #77. Pauses the script for 24 hours when the daily limit is reached.
IGNORE_INTRADAY_DATA_REFRESH_DAYS = int(os.getenv("IGNORE_INTRADAY_DATA_REFRESH_DAYS", 30)) # optional, ignores the REQUEST_INTRADAY_DATA_REFRESH for the specified number of days from current date. 
TAG_MEASUREMENTS_WITH_USER_EMAIL = True if os.getenv("TAG_MEASUREMENTS_WITH_USER_EMAIL") in ['True', 'true', 'TRUE','t', 'T', 'yes', 'Yes', 'YES', '1'] else False # Adds an additional "User_ID" tag in each measurement for multi user database support - see #96
FORCE_REPROCESS_ACTIVITIES = False if os.getenv("FORCE_REPROCESS_ACTIVITIES") in ['False','false','FALSE','f','F','no','No','NO','0'] else True # optional, will enable re-processing of fit files when set to true, may skip activities if set to false (issue #30)
USER_TIMEZONE = os.getenv("USER_TIMEZONE", "") # optional, fetches timezone info from last activity automatically if left blank
PARSED_ACTIVITY_ID_LIST = []
IGNORE_ERRORS = True if os.getenv("IGNORE_ERRORS") in ['True', 'true', 'TRUE','t', 'T', 'yes', 'Yes', 'YES', '1'] else False

# %%
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# %%
try:
    if INFLUXDB_ENDPOINT_IS_HTTP:
        if INFLUXDB_VERSION == '1':
            influxdbclient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD)
            influxdbclient.switch_database(INFLUXDB_DATABASE)
        else:
            influxdbclient = InfluxDBClient3(
            host=f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}",
            token=INFLUXDB_V3_ACCESS_TOKEN,
            org=INFLUXDB_ORG,
            database=INFLUXDB_DATABASE
            )
    else:
        if INFLUXDB_VERSION == '1':
            influxdbclient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, ssl=True, verify_ssl=True)
            influxdbclient.switch_database(INFLUXDB_DATABASE)
        else:
            influxdbclient = InfluxDBClient3(
            host=f"https://{INFLUXDB_HOST}:{INFLUXDB_PORT}",
            token=INFLUXDB_V3_ACCESS_TOKEN,
            org=INFLUXDB_ORG,
            database=INFLUXDB_DATABASE
            )
    demo_point = {
    'measurement': 'DemoPoint',
    'time': (datetime.now(pytz.utc) - timedelta(minutes=1)).isoformat(timespec='seconds'),
    'tags': {'DemoTag': 'DemoTagValue'},
    'fields': {'DemoField': 0}
     }
    # The following code block tests the connection by writing/overwriting a demo point. raises error and aborts if connection fails. 
    if INFLUXDB_VERSION == '1':
        influxdbclient.write_points([demo_point])
    else:
        influxdbclient.write(record=[demo_point])
except (InfluxDBClientError, InfluxDBError) as err:
    logging.error("Unable to connect with influxdb database! Aborted")
    raise InfluxDBClientError("InfluxDB connection failed:" + str(err))

# %%
def iter_days(start_date: str, end_date: str):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    current = end

    while current >= start:
        yield current.strftime('%Y-%m-%d')
        current -= timedelta(days=1)


# %%
def garmin_login():
    token_store = TOKEN_DIR
    token_store_expanded = os.path.expanduser(TOKEN_DIR)
    if os.path.isfile(token_store_expanded) and (not token_store_expanded.endswith('.json')):
        # New native client treats non-.json token paths as directories.
        # If a legacy file exists at this path, use a dedicated directory instead.
        token_store = token_store_expanded + "_tokens"
        logging.warning(
            "TOKEN_DIR points to an existing file (%s). Using '%s' for native token storage compatibility",
            token_store_expanded,
            token_store,
        )

    try:
        logging.info(f"Trying to login to Garmin Connect using token data from '{token_store}'...")
        garmin = Garmin()
        result1, result2 = garmin.login(token_store)
        if result1 == "needs_mfa":
            raise GarminConnectAuthenticationError(
                "MFA is required but credentials are not configured for interactive login"
            )
        logging.info("Login to Garmin Connect successful using stored session tokens.")

    except (FileNotFoundError, GarminConnectAuthenticationError, GarminConnectConnectionError):
        logging.warning("Session is expired or login information not present/incorrect. You'll need to log in again...login with your Garmin Connect credentials to generate them.")
        try:
            user_email = GARMINCONNECT_EMAIL or input("Enter Garminconnect Login e-mail: ")
            user_password = GARMINCONNECT_PASSWORD or input("Enter Garminconnect password (characters will be visible): ")
            garmin = Garmin(
                email=user_email, password=user_password, is_cn=GARMINCONNECT_IS_CN, return_on_mfa=True
            )
            result1, result2 = garmin.login(token_store)
            if result1 == "needs_mfa":  # MFA is required
                mfa_code = input("MFA one-time code (via email or SMS): ")
                garmin.resume_login(result2, mfa_code)

            # With return_on_mfa=True, library login can return before its internal token auto-dump path.
            # Persist tokens explicitly so next run can restore from TOKEN_DIR.
            if hasattr(garmin, "client") and hasattr(garmin.client, "dump"):
                garmin.client.dump(token_store)
            else:
                raise GarminConnectConnectionError("Unable to persist Garmin session tokens: no supported dump method found")

            logging.info(f"Oauth tokens stored in '{token_store}' for future use")
            logging.info("login to Garmin Connect successful using credentials and MFA (if enabled). Continuing with current run")

        except (
            FileNotFoundError,
            GarminConnectConnectionError,
            GarminConnectAuthenticationError,
            GarminConnectTooManyRequestsError,
            requests.exceptions.HTTPError,
        ) as err:
            logging.error(str(err))
            raise Exception("Garmin login failed after credential/MFA attempt")

    return garmin


def _is_http_status_error(err, status_code):
    """Best-effort status matching for wrapped Garmin errors in different module versions."""
    if hasattr(err, "response") and getattr(err.response, "status_code", None) == status_code:
        return True
    if hasattr(err, "status_code") and getattr(err, "status_code", None) == status_code:
        return True
    return re.search(rf"\b{status_code}\b", str(err)) is not None

# %%
def write_points_to_influxdb(points):
    write_chunk_size = 20000
    try:
        if len(points) != 0:
            if TAG_MEASUREMENTS_WITH_USER_EMAIL:
                for item in points:
                    item['tags'].update({'User_ID': garmin_obj.client.profile.get('userName','Unknown')})
            # Write in chunks - Issue reported for large activities data containing >20000 points - Error 413 : payload too large
            for i in range(0, len(points), write_chunk_size):
                if INFLUXDB_VERSION == '1':
                    influxdbclient.write_points(points[i:i + write_chunk_size])
                else:
                    influxdbclient.write(record=points[i:i + write_chunk_size])
            logging.info("Success : updated influxDB database with new points")
    except (InfluxDBClientError, InfluxDBError) as err:
        logging.error("Write failed : Unable to connect with database! " + str(err))

# %%
def get_daily_stats(date_str):
    points_list = []
    stats_json = garmin_obj.get_stats(date_str)
    if stats_json['wellnessStartTimeGmt'] and datetime.strptime(date_str, "%Y-%m-%d") < datetime.today():
        points_list.append({
            "measurement":  "DailyStats",
            "time": pytz.timezone("UTC").localize(datetime.strptime(stats_json['wellnessStartTimeGmt'], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
            "tags": {
                "Device": GARMIN_DEVICENAME,
                "Database_Name": INFLUXDB_DATABASE
            },
            "fields": {
                "activeKilocalories": stats_json.get('activeKilocalories'),
                "bmrKilocalories": stats_json.get('bmrKilocalories'),

                'totalSteps': stats_json.get('totalSteps'),
                'totalDistanceMeters': stats_json.get('totalDistanceMeters'),

                "highlyActiveSeconds": stats_json.get("highlyActiveSeconds"),
                "activeSeconds": stats_json.get("activeSeconds"),
                "sedentarySeconds": stats_json.get("sedentarySeconds"),
                "sleepingSeconds": stats_json.get("sleepingSeconds"),
                "moderateIntensityMinutes": stats_json.get("moderateIntensityMinutes"),
                "vigorousIntensityMinutes": stats_json.get("vigorousIntensityMinutes"),

                "floorsAscendedInMeters": stats_json.get("floorsAscendedInMeters"),
                "floorsDescendedInMeters": stats_json.get("floorsDescendedInMeters"),
                "floorsAscended": stats_json.get("floorsAscended"),
                "floorsDescended": stats_json.get("floorsDescended"),

                "minHeartRate": stats_json.get("minHeartRate"),
                "maxHeartRate": stats_json.get("maxHeartRate"),
                "restingHeartRate": stats_json.get("restingHeartRate"),
                "minAvgHeartRate": stats_json.get("minAvgHeartRate"),
                "maxAvgHeartRate": stats_json.get("maxAvgHeartRate"),

                "avgSkinTempDeviationC": stats_json.get("avgSkinTempDeviationC"),
                "avgSkinTempDeviationF": stats_json.get("avgSkinTempDeviationF"),

                "stressDuration": stats_json.get("stressDuration"),
                "restStressDuration": stats_json.get("restStressDuration"),
                "activityStressDuration": stats_json.get("activityStressDuration"),
                "uncategorizedStressDuration": stats_json.get("uncategorizedStressDuration"),
                "totalStressDuration": stats_json.get("totalStressDuration"),
                "lowStressDuration": stats_json.get("lowStressDuration"),
                "mediumStressDuration": stats_json.get("mediumStressDuration"),
                "highStressDuration": stats_json.get("highStressDuration"),
                
                "stressPercentage": stats_json.get("stressPercentage"),
                "restStressPercentage": stats_json.get("restStressPercentage"),
                "activityStressPercentage": stats_json.get("activityStressPercentage"),
                "uncategorizedStressPercentage": stats_json.get("uncategorizedStressPercentage"),
                "lowStressPercentage": stats_json.get("lowStressPercentage"),
                "mediumStressPercentage": stats_json.get("mediumStressPercentage"),
                "highStressPercentage": stats_json.get("highStressPercentage"),
                
                "bodyBatteryChargedValue": stats_json.get("bodyBatteryChargedValue"),
                "bodyBatteryDrainedValue": stats_json.get("bodyBatteryDrainedValue"),
                "bodyBatteryHighestValue": stats_json.get("bodyBatteryHighestValue"),
                "bodyBatteryLowestValue": stats_json.get("bodyBatteryLowestValue"),
                "bodyBatteryDuringSleep": stats_json.get("bodyBatteryDuringSleep"),
                "bodyBatteryAtWakeTime": stats_json.get("bodyBatteryAtWakeTime"),
                
                "averageSpo2": stats_json.get("averageSpo2"),
                "lowestSpo2": stats_json.get("lowestSpo2"),
            }
        })
        if points_list:
            logging.info(f"Success : Fetching daily metrics for date {date_str}")
        return points_list
    else:
        logging.debug("No daily stat data available for the give date " + date_str)
        return []
    

# %%
def get_last_sync():
    global GARMIN_DEVICENAME
    global GARMIN_DEVICEID
    points_list = []
    sync_data = garmin_obj.get_device_last_used()
    if GARMIN_DEVICENAME_AUTOMATIC:
        GARMIN_DEVICENAME = sync_data.get('lastUsedDeviceName') or "Unknown"
        GARMIN_DEVICEID = sync_data.get('userDeviceId') or None
    points_list.append({
        "measurement":  "DeviceSync",
        "time": datetime.fromtimestamp(sync_data['lastUsedDeviceUploadTime']/1000, tz=pytz.timezone("UTC")).isoformat(),
        "tags": {
            "Device": GARMIN_DEVICENAME,
            "Database_Name": INFLUXDB_DATABASE
        },
        "fields": {
            "imageUrl": sync_data.get('imageUrl'),
            "Device_Name": GARMIN_DEVICENAME
        }
    })
    if points_list:
        logging.info(f"Success : Updated device last sync time")
    else:
        logging.warning("No associated/synced Garmin device found with your account")
    return points_list

# %%
def get_sleep_data(date_str):
    points_list = []
    all_sleep_data = garmin_obj.get_sleep_data(date_str)
    sleep_json = all_sleep_data.get("dailySleepDTO", None)
    if sleep_json["sleepEndTimestampGMT"]:
        points_list.append({
        "measurement":  "SleepSummary",
        "time": datetime.fromtimestamp(sleep_json["sleepEndTimestampGMT"]/1000, tz=pytz.timezone("UTC")).isoformat(),
        "tags": {
            "Device": GARMIN_DEVICENAME,
            "Database_Name": INFLUXDB_DATABASE
            },
        "fields": {
            "sleepTimeSeconds": sleep_json.get("sleepTimeSeconds"),
            "deepSleepSeconds": sleep_json.get("deepSleepSeconds"),
            "lightSleepSeconds": sleep_json.get("lightSleepSeconds"),
            "remSleepSeconds": sleep_json.get("remSleepSeconds"),
            "awakeSleepSeconds": sleep_json.get("awakeSleepSeconds"),
            "averageSpO2Value": sleep_json.get("averageSpO2Value"),
            "lowestSpO2Value": sleep_json.get("lowestSpO2Value"),
            "highestSpO2Value": sleep_json.get("highestSpO2Value"),
            "averageRespirationValue": sleep_json.get("averageRespirationValue"),
            "lowestRespirationValue": sleep_json.get("lowestRespirationValue"),
            "highestRespirationValue": sleep_json.get("highestRespirationValue"),
            "awakeCount": sleep_json.get("awakeCount"),
            "avgSleepStress": sleep_json.get("avgSleepStress"),
            "sleepScore": ((sleep_json.get("sleepScores") or {}).get("overall") or {}).get("value"),
            "restlessMomentsCount": all_sleep_data.get("restlessMomentsCount"),
            "avgOvernightHrv": all_sleep_data.get("avgOvernightHrv"),
            "bodyBatteryChange": all_sleep_data.get("bodyBatteryChange"),
            "restingHeartRate": all_sleep_data.get("restingHeartRate"),
            "avgSkinTempDeviationC": all_sleep_data.get("avgSkinTempDeviationC"),
            "avgSkinTempDeviationF": all_sleep_data.get("avgSkinTempDeviationF")
            }
        })
    sleep_movement_intraday = all_sleep_data.get("sleepMovement")
    if sleep_movement_intraday:
        for entry in sleep_movement_intraday:
            points_list.append({
                "measurement":  "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(entry["startGMT"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE
                },
                "fields": {
                    "SleepMovementActivityLevel": entry.get("activityLevel",-1),
                    "SleepMovementActivitySeconds": int((datetime.strptime(entry["endGMT"], "%Y-%m-%dT%H:%M:%S.%f") - datetime.strptime(entry["startGMT"], "%Y-%m-%dT%H:%M:%S.%f")).total_seconds())
                }
            })
    sleep_levels_intraday = all_sleep_data.get("sleepLevels")
    if sleep_levels_intraday:
        for entry in sleep_levels_intraday:
            if entry.get("activityLevel") or entry.get("activityLevel") == 0: # Include 0 for Deepsleep but not None - Refer to issue #43
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(entry["startGMT"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "SleepStageLevel": entry.get("activityLevel"),
                        "SleepStageSeconds": int((datetime.strptime(entry["endGMT"], "%Y-%m-%dT%H:%M:%S.%f") - datetime.strptime(entry["startGMT"], "%Y-%m-%dT%H:%M:%S.%f")).total_seconds())
                    }
                })
        # Add additional duplicate terminal data point (see issue #127)
        if entry.get("endGMT"):
            points_list.append({
                "measurement":  "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(entry["endGMT"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE
                },
                "fields": {"SleepStageLevel": entry.get("activityLevel")} # Duplicating last entry for visualization in Grafana
            })
    sleep_restlessness_intraday = all_sleep_data.get("sleepRestlessMoments")
    if sleep_restlessness_intraday:
        for entry in sleep_restlessness_intraday:
            if entry.get("value"):
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": datetime.fromtimestamp(entry["startGMT"]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "sleepRestlessValue": entry.get("value")
                    }
                })
    sleep_spo2_intraday = all_sleep_data.get("wellnessEpochSPO2DataDTOList")
    if sleep_spo2_intraday:
        for entry in sleep_spo2_intraday:
            if entry.get("spo2Reading"):
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(entry["epochTimestamp"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "spo2Reading": entry.get("spo2Reading")
                    }
                })
    sleep_respiration_intraday = all_sleep_data.get("wellnessEpochRespirationDataDTOList")
    if sleep_respiration_intraday:
        for entry in sleep_respiration_intraday:
            if entry.get("respirationValue"):
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": datetime.fromtimestamp(entry["startTimeGMT"]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "respirationValue": entry.get("respirationValue")
                    }
                })
    sleep_heart_rate_intraday = all_sleep_data.get("sleepHeartRate")
    if sleep_heart_rate_intraday:
        for entry in sleep_heart_rate_intraday:
            if entry.get("value"):
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": datetime.fromtimestamp(entry["startGMT"]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "heartRate": entry.get("value")
                    }
                })
    sleep_stress_intraday = all_sleep_data.get("sleepStress")
    if sleep_stress_intraday:
        for entry in sleep_stress_intraday:
            if entry.get("value"):
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": datetime.fromtimestamp(entry["startGMT"]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "stressValue": entry.get("value")
                    }
                })
    sleep_bb_intraday = all_sleep_data.get("sleepBodyBattery")
    if sleep_bb_intraday:
        for entry in sleep_bb_intraday:
            if entry.get("value"):
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": datetime.fromtimestamp(entry["startGMT"]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "bodyBattery": entry.get("value")
                    }
                })
    sleep_hrv_intraday = all_sleep_data.get("hrvData")
    if sleep_hrv_intraday:
        for entry in sleep_hrv_intraday:
            if entry.get("value"):
                points_list.append({
                    "measurement":  "SleepIntraday",
                    "time": datetime.fromtimestamp(entry["startGMT"]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "hrvData": entry.get("value")
                    }
                })
    if points_list:
        logging.info(f"Success : Fetching intraday sleep metrics for date {date_str}")
    return points_list

# %%
def get_intraday_hr(date_str):
    points_list = []
    hr_list = garmin_obj.get_heart_rates(date_str).get("heartRateValues") or []
    for entry in hr_list:
        if entry[1]:
            points_list.append({
                    "measurement":  "HeartRateIntraday",
                    "time": datetime.fromtimestamp(entry[0]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "HeartRate": entry[1]
                    }
                })
    if points_list:
        logging.info(f"Success : Fetching intraday Heart Rate for date {date_str}")
    return points_list

# %%
def get_intraday_steps(date_str):
    points_list = []
    steps_list = garmin_obj.get_steps_data(date_str)
    for entry in steps_list:
        if entry["steps"] or entry["steps"] == 0:
            points_list.append({
                    "measurement":  "StepsIntraday",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(entry['startGMT'], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "StepsCount": entry["steps"]
                    }
                })
    if points_list:
        logging.info(f"Success : Fetching intraday steps for date {date_str}")
    return points_list

# %%
def get_intraday_stress(date_str):
    points_list = []
    stress_list = garmin_obj.get_stress_data(date_str).get('stressValuesArray') or []
    for entry in stress_list:
        if entry[1] or entry[1] == 0:
            points_list.append({
                    "measurement":  "StressIntraday",
                    "time": datetime.fromtimestamp(entry[0]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "stressLevel": entry[1]
                    }
                })
    bb_list = garmin_obj.get_stress_data(date_str).get('bodyBatteryValuesArray') or []
    for entry in bb_list:
        if entry[2] or entry[2] == 0:
            points_list.append({
                    "measurement":  "BodyBatteryIntraday",
                    "time": datetime.fromtimestamp(entry[0]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "BodyBatteryLevel": entry[2]
                    }
                })
    if points_list:
        logging.info(f"Success : Fetching intraday stress and Body Battery values for date {date_str}")
    return points_list

# %%
def get_intraday_br(date_str):
    points_list = []
    br_list = garmin_obj.get_respiration_data(date_str).get('respirationValuesArray') or []
    for entry in br_list:
        if entry[1]:
            points_list.append({
                    "measurement":  "BreathingRateIntraday",
                    "time": datetime.fromtimestamp(entry[0]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "BreathingRate": entry[1]
                    }
                })
    if points_list:
        logging.info(f"Success : Fetching intraday Breathing Rate for date {date_str}")
    return points_list

# %%
def get_intraday_hrv(date_str):
    points_list = []
    hrv_data = garmin_obj.get_hrv_data(date_str) or {}
    hrv_list = hrv_data.get('hrvReadings') or []
    for entry in hrv_list:
        if entry.get('hrvValue'):
            points_list.append({
                    "measurement":  "HRV_Intraday",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(entry['readingTimeGMT'],"%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {
                        "hrvValue": entry.get('hrvValue')
                    }
                })
    if points_list:
        logging.info(f"Success : Fetching intraday HRV for date {date_str}")

    # --- HRV Status summary (weeklyAvg, baseline band, status) ---
    hrv_summary = hrv_data.get('hrvSummary')
    if hrv_summary and hrv_summary.get('calendarDate'):
        baseline = hrv_summary.get('baseline') or {}
        hrv_status_fields = {
            'weeklyAvg': hrv_summary.get('weeklyAvg'),
            'lastNightAvg': hrv_summary.get('lastNightAvg'),
            'lastNight5MinHigh': hrv_summary.get('lastNight5MinHigh'),
            'baselineLowUpper': baseline.get('lowUpper'),
            'baselineBalancedLow': baseline.get('balancedLow'),
            'baselineBalancedUpper': baseline.get('balancedUpper'),
            'status': hrv_summary.get('status'),
        }
        if any(v is not None for v in hrv_status_fields.values()):
            points_list.append({
                'measurement': 'HRVStatus',
                'time': datetime.strptime(hrv_summary['calendarDate'], '%Y-%m-%d').replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                'tags': {
                    'Device': GARMIN_DEVICENAME,
                    'Database_Name': INFLUXDB_DATABASE
                },
                'fields': hrv_status_fields
            })
            logging.info(f'Success : Fetching HRV Status for date {date_str}')

    return points_list

# %%
def get_body_composition(date_str):
    points_list = []
    weight_list_all = garmin_obj.get_weigh_ins(date_str, date_str).get('dailyWeightSummaries', [])
    if weight_list_all:
        weight_list = weight_list_all[0].get('allWeightMetrics', [])
        for weight_dict in weight_list:
            data_fields = {
                    "weight": weight_dict.get("weight"),
                    "bmi": weight_dict.get("bmi"),
                    "bodyFat": weight_dict.get("bodyFat"),
                    "bodyWater": weight_dict.get("bodyWater"),
                    "boneMass": weight_dict.get("boneMass"),
                    "muscleMass": weight_dict.get("muscleMass"),
                    "physiqueRating": weight_dict.get("physiqueRating"),
                    "visceralFat": weight_dict.get("visceralFat"),
                    # "metabolicAge": datetime.fromtimestamp(int(weight_dict.get("metabolicAge")/1000), tz=pytz.timezone("UTC")).isoformat() if weight_dict.get("metabolicAge") else None
                }
            if not all(value is None for value in data_fields.values()):
                points_list.append({
                    "measurement":  "BodyComposition",
                    "time": datetime.fromtimestamp((weight_dict['timestampGMT']/1000) , tz=pytz.timezone("UTC")).isoformat() if weight_dict['timestampGMT'] else datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(), # Use GMT 00:00 is timestamp is not available (issue #15)
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "Frequency" : "Intraday",
                        "SourceType" : weight_dict.get('sourceType', "Unknown")
                    },
                    "fields": data_fields
                })
        logging.info(f"Success : Fetching intraday Body Composition (Weight, BMI etc) for date {date_str}")
    return points_list

# %%
def get_activity_summary(date_str):
    points_list = []
    activity_with_gps_id_dict = {}
    strength_activity_id_dict = {}
    activity_weather_info = []
    activity_list = garmin_obj.get_activities_by_date(date_str, date_str)
    if ACTIVITY_TYPE_FILTER:
        activity_list = [a for a in activity_list if (a.get('activityType') or {}).get('typeKey', 'Unknown').lower() in ACTIVITY_TYPE_FILTER]
        logging.info(f"ACTIVITY_TYPE_FILTER active: kept {len(activity_list)} activities matching {ACTIVITY_TYPE_FILTER}")
    for activity in activity_list:
        activity_type_key = (activity.get('activityType') or {}).get('typeKey', "Unknown")
        if activity.get('hasPolyline') or ALWAYS_PROCESS_FIT_FILES: # will process FIT files lacking GPS data if ALWAYS_PROCESS_FIT_FILES is set to True
            if not activity.get('hasPolyline'):
                logging.warning(f"Activity ID {activity.get('activityId')} got no GPS data - yet, activity FIT file data will be processed as ALWAYS_PROCESS_FIT_FILES is on")
            activity_with_gps_id_dict[activity.get('activityId')] = activity_type_key
            # Capture start location for weather enrichment
            _start_lat = activity.get('startLatitude') or activity.get('beginLatitude')
            _start_lon = activity.get('startLongitude') or activity.get('beginLongitude')
            if _start_lat and _start_lon:
                _sel = datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC).strftime('%Y%m%dT%H%M%SUTC-') + activity_type_key
                activity_weather_info.append({
                    "selector": _sel,
                    "start_time_utc": datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC).isoformat(),
                    "lat": float(_start_lat),
                    "lon": float(_start_lon),
                })
        # Collect strength training activities for API-based exercise set fetching
        if 'strength' in activity_type_key.lower() and activity.get('startTimeGMT'):
            strength_activity_id_dict[activity.get('activityId')] = {
                'typeKey': activity_type_key,
                'startTimeGMT': activity.get('startTimeGMT'),
                'activityName': activity.get('activityName'),
            }
        if "startTimeGMT" in activity: # "startTimeGMT" should be available for all activities (fix #13)
            activity_id = activity.get('activityId')
            hr_zones_data = garmin_obj.get_activity_hr_in_timezones(activity_id)
            hr_zone_boundaries = [None] * 5
            if hr_zones_data:
                for zone in hr_zones_data:
                    hr_zone_boundaries[int(zone.get('zoneNumber')) - 1] = zone.get('zoneLowBoundary')
            else:
                logging.warning(f"No HR zone data found for activity: {activity_id}")

            points_list.append({
                "measurement":  "ActivitySummary",
                "time": datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC).isoformat(),
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE,
                    "ActivityID": activity.get('activityId'),
                    "ActivitySelector": datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC).strftime('%Y%m%dT%H%M%SUTC-') + (activity.get('activityType') or {}).get('typeKey', "Unknown")
                },
                "fields": {
                    "Activity_ID": activity_id,
                    'Device_ID': activity.get('deviceId'),
                    'activityName': activity.get('activityName'),
                    'description': activity.get('description'),
                    'activityType': (activity.get('activityType') or {}).get('typeKey',None),
                    'distance': activity.get('distance'),
                    'elevationGain': activity.get('elevationGain'),
                    'elevationLoss': activity.get('elevationLoss'),
                    'elapsedDuration': activity.get('elapsedDuration') if activity.get('elapsedDuration') else activity.get('duration'),
                    'movingDuration': activity.get('movingDuration'),
                    'averageSpeed': activity.get('averageSpeed'),
                    'maxSpeed': activity.get('maxSpeed'),
                    'calories': activity.get('calories'),
                    'bmrCalories': activity.get('bmrCalories'),
                    'averageHR': activity.get('averageHR'),
                    'maxHR': activity.get('maxHR'),
                    'vO2MaxValue': activity.get('vO2MaxValue'),
                    'locationName': activity.get('locationName'),
                    'lapCount': activity.get('lapCount'),
                    'hrTimeInZone_1': int(val) if (val := activity.get('hrTimeInZone_1')) is not None else None,
                    'hrTimeInZone_2': int(val) if (val := activity.get('hrTimeInZone_2')) is not None else None,
                    'hrTimeInZone_3': int(val) if (val := activity.get('hrTimeInZone_3')) is not None else None,
                    'hrTimeInZone_4': int(val) if (val := activity.get('hrTimeInZone_4')) is not None else None,
                    'hrTimeInZone_5': int(val) if (val := activity.get('hrTimeInZone_5')) is not None else None,
                    'hrZoneLowBoundary_1': hr_zone_boundaries[0],
                    'hrZoneLowBoundary_2': hr_zone_boundaries[1],
                    'hrZoneLowBoundary_3': hr_zone_boundaries[2],
                    'hrZoneLowBoundary_4': hr_zone_boundaries[3],
                    'hrZoneLowBoundary_5': hr_zone_boundaries[4],
                    'aerobicTrainingEffect': activity.get('aerobicTrainingEffect'),
                    'anaerobicTrainingEffect': activity.get('anaerobicTrainingEffect'),
                    'activityTrainingLoad': activity.get('activityTrainingLoad'),
                    'trainingEffectLabel': activity.get('trainingEffectLabel'),
                    'moderateIntensityMinutes': activity.get('moderateIntensityMinutes'),
                    'vigorousIntensityMinutes': activity.get('vigorousIntensityMinutes'),
                }
            })
            points_list.append({
                "measurement":  "ActivitySummary",
                "time": (datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC) + timedelta(seconds=int(activity.get('elapsedDuration', activity.get('duration', 0))))).isoformat(),
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE,
                    "ActivityID": activity.get('activityId'),
                    "ActivitySelector": datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC).strftime('%Y%m%dT%H%M%SUTC-') + (activity.get('activityType') or {}).get('typeKey', "Unknown")
                },
                "fields": {
                    "Activity_ID": activity.get('activityId'),
                    'Device_ID': activity.get('deviceId'),
                    'activityName': "END",
                    'activityType': "No Activity",
                }
            })
            logging.info(f"Success : Fetching Activity summary with id {activity.get('activityId')} for date {date_str}")
        else:
            logging.warning(f"Skipped : Start Timestamp missing for activity id {activity.get('activityId')} for date {date_str}")
    return points_list, activity_with_gps_id_dict, strength_activity_id_dict, activity_weather_info

# %%
def get_strength_training_data(strength_activity_id_dict):
    """Fetch strength training exercise sets and HR zones from Garmin Connect API.
    Uses API data (not FIT files) to get corrected exercise names and details.
    See: https://github.com/arpanghosh8453/garmin-grafana/issues/189
    """
    points_list = []
    for activity_id, activity_info in strength_activity_id_dict.items():
        activity_type = activity_info['typeKey']
        start_time_str = activity_info['startTimeGMT']
        activity_start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        activity_selector = activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
        activity_name = activity_info.get('activityName', activity_type)

        try:
            exercise_sets_data = garmin_obj.get_activity_exercise_sets(activity_id)
            exercises = exercise_sets_data.get('exerciseSets', []) or []
            set_counter = 0
            for exercise in exercises:
                set_type = exercise.get('setType', '')
                if set_type == 'REST':
                    continue
                set_counter += 1
                exercise_info = (exercise.get('exercises') or [{}])[0]
                category = exercise_info.get('category', 'UNKNOWN')
                exercise_name = exercise_info.get('name', '')
                exercise_label = f"{category}/{exercise_name}" if exercise_name else category
                weight_g = float(exercise.get('weight', 0) or 0)
                weight_kg = weight_g / 1000.0
                duration_s = float(exercise.get('duration', 0) or 0)
                start_ts = exercise.get('startTime')
                if start_ts:
                    set_time = datetime.strptime(start_ts.split('.')[0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.UTC).isoformat()
                else:
                    set_time = (activity_start_time + timedelta(seconds=set_counter)).isoformat()

                data_fields = {
                    "Activity_ID": activity_id,
                    "ActivityName": activity_name,
                    "SetOrder": int(exercise.get('setOrder', set_counter)),
                    "SetType": set_type,
                    "Reps": int(exercise.get('repetitionCount', 0)),
                    "Weight_kg": weight_kg,
                    "Duration_s": duration_s,
                }
                points_list.append({
                    "measurement": "StrengthExerciseSet",
                    "time": set_time,
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "ActivityID": activity_id,
                        "ActivitySelector": activity_selector,
                        "ExerciseCategory": category,
                        "ExerciseLabel": exercise_label,
                    },
                    "fields": data_fields
                })
            logging.info(f"Success : Fetching {set_counter} strength exercise sets for activity {activity_id}")
        except Exception as err:
            logging.warning(f"Failed to fetch exercise sets for activity {activity_id}: {err}")

        try:
            hr_zones_data = garmin_obj.get_activity_hr_in_timezones(activity_id)
            for zone_info in hr_zones_data:
                zone_number = zone_info.get('zoneNumber', zone_info.get('zone'))
                if zone_number is None:
                    continue
                data_fields = {
                    "Activity_ID": activity_id,
                    "ActivityName": activity_name,
                    "ZoneNumber": int(zone_number),
                    "SecsInZone": zone_info.get('secsInZone'),
                    "ZoneLowBoundary": zone_info.get('zoneLowBoundary'),
                }
                points_list.append({
                    "measurement": "StrengthHRZones",
                    "time": (activity_start_time + timedelta(milliseconds=int(zone_number))).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "ActivityID": activity_id,
                        "ActivitySelector": activity_selector,
                    },
                    "fields": data_fields
                })
            logging.info(f"Success : Fetching strength HR zones for activity {activity_id}")
        except Exception as err:
            logging.warning(f"Failed to fetch HR zones for activity {activity_id}: {err}")

    return points_list

# %%

# %%
# --- Weather enrichment (Open-Meteo Archive API) ---
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_HOURLY_VARS = "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,wind_speed_10m,wind_gusts_10m,wind_direction_10m,precipitation,cloud_cover,pressure_msl,shortwave_radiation"

import math as _math

def _compute_wbgt(temp_c, humidity_pct):
    """Simplified outdoor WBGT estimate (Liljegren approximation, no globe thermometer)."""
    e = (humidity_pct / 100.0) * 6.105 * _math.exp(17.27 * temp_c / (237.7 + temp_c))
    return round(0.567 * temp_c + 0.393 * e + 3.94, 1)


def fetch_activity_weather(activity_info_list):
    """
    Fetch weather from Open-Meteo for each outdoor activity and return InfluxDB points.

    Parameters
    ----------
    activity_info_list : list of dict
        Each dict has: selector (ActivitySelector tag), start_time_utc (ISO str),
        lat (float), lon (float).

    Returns
    -------
    list of InfluxDB point dicts (measurement=ActivityWeather).
    """
    points_list = []
    for info in activity_info_list:
        selector = info["selector"]
        lat = info.get("lat")
        lon = info.get("lon")
        start_iso = info["start_time_utc"]

        if not lat or not lon:
            logging.debug(f"Weather: no GPS for {selector}, skipping")
            continue

        try:
            start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        except Exception:
            logging.warning(f"Weather: bad timestamp for {selector}: {start_iso}")
            continue

        date_str = start_dt.strftime("%Y-%m-%d")
        hour_str = start_dt.strftime("%Y-%m-%dT%H:00")

        try:
            resp = requests.get(OPEN_METEO_URL, params={
                "latitude": round(lat, 4),
                "longitude": round(lon, 4),
                "start_date": date_str,
                "end_date": date_str,
                "hourly": OPEN_METEO_HOURLY_VARS,
                "timezone": "UTC",
            }, timeout=30)
            resp.raise_for_status()
            weather = resp.json()
        except Exception as exc:
            logging.warning(f"Weather: Open-Meteo API error for {selector}: {exc}")
            continue

        hourly = weather.get("hourly", {})
        times = hourly.get("time", [])
        idx = times.index(hour_str) if hour_str in times else 0 if times else None
        if idx is None:
            logging.warning(f"Weather: no hourly data for {selector}")
            continue

        def _val(key):
            arr = hourly.get(key, [])
            return float(arr[idx]) if idx < len(arr) and arr[idx] is not None else None

        temp = _val("temperature_2m")
        humidity = _val("relative_humidity_2m")

        fields = {}
        field_map = {
            "temperature_c": "temperature_2m",
            "humidity_pct": "relative_humidity_2m",
            "dew_point_c": "dew_point_2m",
            "apparent_temp_c": "apparent_temperature",
            "wind_speed_kmh": "wind_speed_10m",
            "wind_gust_kmh": "wind_gusts_10m",
            "wind_direction_deg": "wind_direction_10m",
            "precipitation_mm": "precipitation",
            "cloud_cover_pct": "cloud_cover",
            "pressure_hpa": "pressure_msl",
            "solar_radiation_wm2": "shortwave_radiation",
        }
        for field_name, api_key in field_map.items():
            v = _val(api_key)
            if v is not None:
                fields[field_name] = v

        if temp is not None and humidity is not None:
            fields["wbgt_estimated"] = float(_compute_wbgt(temp, humidity))

        if not fields:
            continue

        points_list.append({
            "measurement": "ActivityWeather",
            "time": start_iso,
            "tags": {
                "ActivitySelector": selector,
                "Device": GARMIN_DEVICENAME,
                "Database_Name": INFLUXDB_DATABASE,
            },
            "fields": fields,
        })
        logging.info(f"Weather: {selector} -> {temp}°C, {humidity}% humidity, WBGT {fields.get('wbgt_estimated', '?')}")

    return points_list


def fetch_activity_GPS(activityIDdict): # Uses FIT file by default, falls back to TCX
    points_list = []
    for activityID in activityIDdict.keys():
        activity_type = activityIDdict[activityID]
        if (activityID in PARSED_ACTIVITY_ID_LIST) and (not FORCE_REPROCESS_ACTIVITIES):
            logging.info(f"Skipping : Activity ID {activityID} has already been processed within current runtime")
            return []
        if (activityID in PARSED_ACTIVITY_ID_LIST) and (FORCE_REPROCESS_ACTIVITIES):
            logging.info(f"Re-processing : Activity ID {activityID} (FORCE_REPROCESS_ACTIVITIES is on)")
        try:
            zip_data = garmin_obj.download_activity(activityID, dl_fmt=garmin_obj.ActivityDownloadFormat.ORIGINAL)
            logging.info(f"Processing : Activity ID {activityID} FIT file data - this may take a while...")
            zip_buffer = io.BytesIO(zip_data)
            with zipfile.ZipFile(zip_buffer) as zip_ref:
                fit_filename = next((f for f in zip_ref.namelist() if f.endswith('.fit')), None)
                if not fit_filename:
                    raise FileNotFoundError(f"No FIT file found in the downloaded zip archive for Activity ID {activityID}")
                else:
                    fit_data = zip_ref.read(fit_filename)
                    fit_file_buffer = io.BytesIO(fit_data)
                    fitfile = FitFile(fit_file_buffer)
                    fitfile.parse()
                    all_records_list = [record.get_values() for record in fitfile.get_messages('record')]
                    all_sessions_list = [record.get_values() for record in fitfile.get_messages('session')]
                    all_lengths_list = [record.get_values() for record in fitfile.get_messages('length')]
                    all_laps_list = [record.get_values() for record in fitfile.get_messages('lap')]
                    all_workout_steps_list = [record.get_values() for record in fitfile.get_messages('workout_step')]  # Patch: extraction prescription steps
                    if len(all_records_list) == 0:
                        raise FileNotFoundError(f"No records found in FIT file for Activity ID {activityID} - Discarding FIT file")
                    else:
                        activity_start_time = all_records_list[0]['timestamp'].replace(tzinfo=pytz.UTC)
                    for parsed_record in all_records_list:
                        if parsed_record.get('timestamp'):
                            point = {
                                "measurement": "ActivityGPS",
                                "time": parsed_record['timestamp'].replace(tzinfo=pytz.UTC).isoformat(), 
                                "tags": {
                                    "Device": GARMIN_DEVICENAME,
                                    "Database_Name": INFLUXDB_DATABASE,
                                    "ActivityID": activityID,
                                    "ActivitySelector": activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
                                },
                                "fields": {
                                    "ActivityName": activity_type,
                                    "Activity_ID": activityID,
                                    "Latitude": int(parsed_record['position_lat']) * ( 180 / 2**31 ) if parsed_record.get('position_lat') else None,
                                    "Longitude": int(parsed_record['position_long']) * ( 180 / 2**31 ) if parsed_record.get('position_long') else None,
                                    "Altitude": parsed_record.get('enhanced_altitude', None) or parsed_record.get('altitude', None),
                                    "Distance": parsed_record.get('distance', None),
                                    "DurationSeconds": (parsed_record['timestamp'].replace(tzinfo=pytz.UTC) - activity_start_time).total_seconds(),
                                    "HeartRate": float(parsed_record.get('heart_rate', None)) if parsed_record.get('heart_rate', None) else None,
                                    "Speed": parsed_record.get('enhanced_speed', None) or parsed_record.get('speed', None),
                                    "GradeAdjustedSpeed": (parsed_record.get("unknown_140") / 1000.0) if parsed_record.get("unknown_140") else None,
                                    "RunningEfficiency": ((parsed_record.get("unknown_140") / 1000.0)/parsed_record.get('heart_rate')) if (parsed_record.get("unknown_140") and parsed_record.get('heart_rate')) else None,
                                    "Cadence": parsed_record.get('cadence', None),
                                    "Fractional_Cadence": parsed_record.get('fractional_cadence', None),
                                    "Temperature": parsed_record.get('temperature', None),
                                    "Accumulated_Power": parsed_record.get('accumulated_power', None),
                                    "Power": parsed_record.get('power', None),
                                    "Vertical_Oscillation": parsed_record.get('vertical_oscillation', None),
                                    "Stance_Time": parsed_record.get('stance_time', None),
                                    "Vertical_Ratio": parsed_record.get('vertical_ratio', None),
                                    "Step_Length": parsed_record.get('step_length', None)
                                }
                            }
                            points_list.append(point)
                    for session_record in all_sessions_list:
                        if session_record.get('start_time') or session_record.get('timestamp'):
                            point = {
                                "measurement": "ActivitySession",
                                "time": session_record['start_time'].replace(tzinfo=pytz.UTC).isoformat() or session_record['timestamp'].replace(tzinfo=pytz.UTC).isoformat(), 
                                "tags": {
                                    "Device": GARMIN_DEVICENAME,
                                    "Database_Name": INFLUXDB_DATABASE,
                                    "ActivityID": activityID,
                                    "ActivitySelector": activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
                                },
                                "fields": {
                                    "Index": int(session_record.get('message_index', -1)) + 1,
                                    "ActivityName": activity_type,
                                    "Activity_ID": activityID,
                                    "Sport": str(session_record.get('sport', None)), # Avoid partial write error 400 see #152#issuecomment-3084539416
                                    "Sub_Sport": str(session_record.get('sub_sport', None)),
                                    "Pool_Length": session_record.get('pool_length', None),
                                    "Pool_Length_Unit": session_record.get('pool_length_unit', None),
                                    "Lengths": session_record.get('num_laps', None),
                                    "Laps": session_record.get('num_lengths', None),
                                    "Aerobic_Training": session_record.get('total_training_effect', None),
                                    "Anaerobic_Training": session_record.get('total_anaerobic_training_effect', None),
                                    "Primary_Benefit": session_record.get('primary_benefit', None),
                                    "Recovery_Time": session_record.get('recovery_time', None)
                                }
                            }
                            points_list.append(point)
                    for length_record in all_lengths_list:
                        if length_record.get('start_time') or length_record.get('timestamp'):
                            point = {
                                "measurement": "ActivityLength",
                                "time": length_record['start_time'].replace(tzinfo=pytz.UTC).isoformat() or length_record['timestamp'].replace(tzinfo=pytz.UTC).isoformat(), 
                                "tags": {
                                    "Device": GARMIN_DEVICENAME,
                                    "Database_Name": INFLUXDB_DATABASE,
                                    "ActivityID": activityID,
                                    "ActivitySelector": activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
                                },
                                "fields": {
                                    "Index": int(length_record.get('message_index', -1)) + 1,
                                    "ActivityName": activity_type,
                                    "Activity_ID": activityID,
                                    "Elapsed_Time": length_record.get('total_elapsed_time', None),
                                    "Strokes": length_record.get('total_strokes', None),
                                    "Swim_Stroke": length_record.get('swim_stroke', None),
                                    "Avg_Speed": length_record.get('avg_speed', None),
                                    "Calories": length_record.get('total_calories', None),
                                    "Avg_Cadence": length_record.get('avg_swimming_cadence', None)
                                }
                            }
                            points_list.append(point)
                    for lap_record in all_laps_list:
                        if lap_record.get('start_time') or lap_record.get('timestamp'):
                            point = {
                                "measurement": "ActivityLap",
                                "time": lap_record['start_time'].replace(tzinfo=pytz.UTC).isoformat() or lap_record['timestamp'].replace(tzinfo=pytz.UTC).isoformat(), 
                                "tags": {
                                    "Device": GARMIN_DEVICENAME,
                                    "Database_Name": INFLUXDB_DATABASE,
                                    "ActivityID": activityID,
                                    "ActivitySelector": activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
                                },
                                "fields": {
                                    "Index": int(lap_record.get('message_index', -1)) + 1,
                                    "ActivityName": activity_type,
                                    "Activity_ID": activityID,
                                    "Elapsed_Time": lap_record.get('total_elapsed_time', None),
                                    "Sport": lap_record.get('sport', None),
                                    "Lengths": lap_record.get('num_lengths', None),
                                    "Length_Index": lap_record.get('first_length_index', None),
                                    "Distance": lap_record.get('total_distance', None),
                                    "Ascent": lap_record.get('total_ascent', None),
                                    "Descent": lap_record.get('total_descent', None),
                                    "Cycles": lap_record.get('total_cycles', None),
                                    "Avg_Stroke_Distance": lap_record.get('avg_stroke_distance', None),
                                    "Moving_Duration": lap_record.get('total_moving_time', None),
                                    "Standing_Duration": lap_record.get('time_standing', None),
                                    "Avg_Speed": lap_record.get('enhanced_avg_speed', None),
                                    "Max_Speed": lap_record.get('enhanced_max_speed', None),
                                    "Calories": lap_record.get('total_calories', None),
                                    "Avg_Power": lap_record.get('avg_power', None),
                                    "Avg_HR": lap_record.get('avg_heart_rate', None),
                                    "Max_HR": lap_record.get('max_heart_rate', None),
                                    "Avg_Cadence": lap_record.get('avg_cadence', None),
                                    "Avg_Temperature": lap_record.get('avg_temperature', None),
                                    "Avg_Vertical_Oscillation": lap_record.get('avg_vertical_oscillation', None),
                                    "Avg_Stance_Time": lap_record.get('avg_stance_time', None),
                                    "Avg_Vertical_Ratio": lap_record.get('avg_vertical_ratio', None),
                                    "Avg_Step_Length": lap_record.get('avg_step_length', None),
                                    # Patch: workout step linkage (None pour activites sans workout structure)
                                    "Intensity": str(lap_record.get('intensity')) if lap_record.get('intensity') is not None else None,
                                    "LapTrigger": str(lap_record.get('lap_trigger')) if lap_record.get('lap_trigger') is not None else None,
                                    "WktStepIndex": int(lap_record['wkt_step_index']) if lap_record.get('wkt_step_index') is not None else None,
                                }
                            }
                            points_list.append(point)
                    # Patch: ecrire WorkoutStep measurement avec la prescription
                    # de chaque step. Les bornes HR (custom_target_heart_rate_low/high) sont en bpm
                    # absolu si > 100 (encoding fit-tool). target_hr_zone donne le numero de zone
                    # tel que prescrit a l'upload. Pour relier au temps de l'activite, on calcule
                    # step_start_offset_s en cherchant le premier lap avec wkt_step_index matchant.
                    if all_workout_steps_list:
                        step_starts_utc = {}
                        step_ends_utc = {}
                        for lap_record in all_laps_list:
                            wsi = lap_record.get('wkt_step_index')
                            st = lap_record.get('start_time')
                            et = lap_record.get('total_elapsed_time') or 0
                            if wsi is None or st is None:
                                continue
                            st_utc = st.replace(tzinfo=pytz.UTC)
                            end_utc = st_utc + timedelta(seconds=et)
                            if wsi not in step_starts_utc or st_utc < step_starts_utc[wsi]:
                                step_starts_utc[wsi] = st_utc
                            if wsi not in step_ends_utc or end_utc > step_ends_utc[wsi]:
                                step_ends_utc[wsi] = end_utc
                        for step_record in all_workout_steps_list:
                            step_idx = step_record.get('message_index')
                            if step_idx is None:
                                continue
                            step_start = step_starts_utc.get(step_idx)
                            step_end = step_ends_utc.get(step_idx)
                            # Patch: multi-target (HR / Power / Cadence)
                            # Garmin Connect re-encode le workout avant download. Selon
                            # target_type, les bornes peuvent etre dans le field
                            # specifique (custom_target_heart_rate_low/high) OU dans le
                            # generique (custom_target_value_low/high). Verifie sur les
                            # workouts Laurent : pour target_type=power_3s, Garmin met
                            # les W dans custom_target_value_low/high SANS offset 1000.
                            target_low_bpm = target_high_bpm = None
                            target_low_w = target_high_w = None
                            target_low_rpm = target_high_rpm = None
                            tt_str = str(step_record.get('target_type') or '')
                            val_low = step_record.get('custom_target_value_low')
                            val_high = step_record.get('custom_target_value_high')
                            if tt_str == 'heart_rate':
                                hr_low = step_record.get('custom_target_heart_rate_low')
                                if hr_low is None:
                                    hr_low = val_low
                                hr_high = step_record.get('custom_target_heart_rate_high')
                                if hr_high is None:
                                    hr_high = val_high
                                target_low_bpm = int(hr_low) if hr_low is not None and hr_low > 0 else None
                                target_high_bpm = int(hr_high) if hr_high is not None and hr_high > 0 else None
                            elif 'power' in tt_str:
                                pw_low = step_record.get('custom_target_power_low')
                                if pw_low is None:
                                    pw_low = val_low
                                pw_high = step_record.get('custom_target_power_high')
                                if pw_high is None:
                                    pw_high = val_high
                                # Si valeur >= 1000 = encoding offset (W = val - 1000).
                                # Sinon = W direct (Garmin re-encode comme ca au download).
                                if pw_low is not None and pw_low > 0:
                                    target_low_w = int(pw_low - 1000) if pw_low >= 1000 else int(pw_low)
                                if pw_high is not None and pw_high > 0:
                                    target_high_w = int(pw_high - 1000) if pw_high >= 1000 else int(pw_high)
                            elif tt_str == 'cadence':
                                cad_low = step_record.get('custom_target_cadence_low')
                                if cad_low is None:
                                    cad_low = val_low
                                cad_high = step_record.get('custom_target_cadence_high')
                                if cad_high is None:
                                    cad_high = val_high
                                target_low_rpm = int(cad_low) if cad_low is not None and cad_low > 0 else None
                                target_high_rpm = int(cad_high) if cad_high is not None and cad_high > 0 else None
                            duration_value = step_record.get('duration_time')
                            if duration_value is None:
                                duration_value = step_record.get('duration_value')
                            point_time = step_start.isoformat() if step_start else activity_start_time.isoformat()
                            step_start_offset_s = (step_start - activity_start_time).total_seconds() if step_start else None
                            step_actual_duration_s = (step_end - step_start).total_seconds() if (step_start and step_end) else None
                            point = {
                                "measurement": "WorkoutStep",
                                "time": point_time,
                                "tags": {
                                    "Device": GARMIN_DEVICENAME,
                                    "Database_Name": INFLUXDB_DATABASE,
                                    "ActivityID": activityID,
                                    "ActivitySelector": activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
                                },
                                "fields": {
                                    "ActivityName": activity_type,
                                    "Activity_ID": activityID,
                                    "StepIndex": int(step_idx),
                                    "IntensityType": str(step_record.get('intensity')) if step_record.get('intensity') is not None else None,
                                    "DurationType": str(step_record.get('duration_type')) if step_record.get('duration_type') is not None else None,
                                    "DurationValueS": float(duration_value) if duration_value is not None else None,
                                    "TargetType": str(step_record.get('target_type')) if step_record.get('target_type') is not None else None,
                                    "TargetHRZone": int(step_record['target_hr_zone']) if step_record.get('target_hr_zone') is not None else None,
                                    "TargetLowBPM": target_low_bpm,
                                    "TargetHighBPM": target_high_bpm,
                                    "TargetPowerZone": int(step_record['target_power_zone']) if step_record.get('target_power_zone') is not None else None,
                                    "TargetLowW": target_low_w,
                                    "TargetHighW": target_high_w,
                                    "TargetLowRPM": target_low_rpm,
                                    "TargetHighRPM": target_high_rpm,
                                    "StepStartOffsetS": step_start_offset_s,
                                    "StepActualDurationS": step_actual_duration_s,
                                    "Notes": str(step_record.get('notes'))[:500] if step_record.get('notes') else None,
                                }
                            }
                            points_list.append(point)
                        # Patch: ecrire WorkoutTarget per-seconde pour
                        # rendering de la bande prescription dans dashboard J. Une row
                        # par seconde d'activite, avec target_low/high de l'etape active.
                        # Steps OPEN -> pas de rows (gap automatique en query InfluxDB).
                        # Permet a Grafana de tracer une bande continue qui s'arrete pile
                        # aux frontieres OPEN/non-OPEN sans line extension artefact.
                        # Patch: on garde aussi les OPEN steps avec sentinel
                        # -1 pour forcer un gap visuel dans le panel HR du dashboard J. Sans
                        # rows pendant les OPEN, Grafana relie en continu via stepAfter (les
                        # rows manquantes != NULL, spanNulls ne s'applique pas). Avec axis
                        # min=90, les rows a -1 sont clippees -> vrai gap visible.
                        # Patch: multi-target. step_intervals stocke
                        # un interval PAR LAP (gere les workouts avec repeat : 3x12'
                        # power -> 3 intervals distincts au lieu d'un seul couvrant le
                        # span entier). Le decoding de target depuis step_records_by_idx
                        # (1 step_record par message_index, partage entre repetitions).
                        # Sentinel -1 par metrique pour gap visuel sur le panel non
                        # concerne (ex: step power -> hr_low/high a -1).
                        step_records_by_idx = {sr.get('message_index'): sr for sr in all_workout_steps_list if sr.get('message_index') is not None}
                        def _decode_step_targets(sr):
                            """Retourne (hr_low, hr_high, w_low, w_high, rpm_low, rpm_high)
                            depuis un step_record FIT, en gerant l'ambiguite Garmin
                            re-encoding (custom_target_value_low/high generique) et
                            l'offset 1000 pour power (uniquement si valeur >= 1000)."""
                            if sr is None:
                                return None, None, None, None, None, None
                            tt_s = str(sr.get('target_type') or '')
                            v_low = sr.get('custom_target_value_low')
                            v_high = sr.get('custom_target_value_high')
                            if tt_s == 'heart_rate':
                                lo = sr.get('custom_target_heart_rate_low')
                                if lo is None: lo = v_low
                                hi = sr.get('custom_target_heart_rate_high')
                                if hi is None: hi = v_high
                                return (int(lo) if lo and lo > 0 else None,
                                        int(hi) if hi and hi > 0 else None,
                                        None, None, None, None)
                            if 'power' in tt_s:
                                lo = sr.get('custom_target_power_low')
                                if lo is None: lo = v_low
                                hi = sr.get('custom_target_power_high')
                                if hi is None: hi = v_high
                                w_lo = (int(lo - 1000) if lo >= 1000 else int(lo)) if lo and lo > 0 else None
                                w_hi = (int(hi - 1000) if hi >= 1000 else int(hi)) if hi and hi > 0 else None
                                return None, None, w_lo, w_hi, None, None
                            if tt_s == 'cadence':
                                lo = sr.get('custom_target_cadence_low')
                                if lo is None: lo = v_low
                                hi = sr.get('custom_target_cadence_high')
                                if hi is None: hi = v_high
                                return (None, None, None, None,
                                        int(lo) if lo and lo > 0 else None,
                                        int(hi) if hi and hi > 0 else None)
                            return None, None, None, None, None, None

                        # Build intervals : on merge les laps CONSECUTIFS avec le
                        # meme wkt_step_index en 1 interval. Cela gere :
                        # - L'auto-lap par distance Garmin qui decoupe un step en
                        #   plusieurs laps (ex: warmup 20 min decoupe en 12+8 min
                        #   par auto-lap a 5km). Sans merge, le step warmup avait
                        #   2 plateaux StepAvgHR differents alors qu'il est UN
                        #   step logique.
                        # - Les repetitions de step par repeat_until_steps_cmplt :
                        #   3x effort 12' donnent 3 occurrences distinctes du
                        #   meme wsi=2, separees par des laps wsi=3 (recup) ;
                        #   le merge ne les fusionne PAS car ils ne sont pas
                        #   consecutifs dans le temps.
                        sorted_laps = sorted(
                            (l for l in all_laps_list if l.get('start_time') is not None and l.get('wkt_step_index') is not None),
                            key=lambda l: l['start_time']
                        )
                        step_intervals = []
                        current = None  # interval en cours d'agregation (laps consecutifs meme wsi)
                        for lap_record in sorted_laps:
                            wsi = lap_record.get('wkt_step_index')
                            st = lap_record.get('start_time')
                            et = lap_record.get('total_elapsed_time') or 0
                            if et <= 0:
                                continue
                            st_utc = st.replace(tzinfo=pytz.UTC)
                            end_utc = st_utc + timedelta(seconds=et)
                            if current is not None and current['_wsi'] == int(wsi):
                                # Meme wsi consecutif -> on etend l'interval courant
                                current['end'] = (end_utc - activity_start_time).total_seconds()
                                continue
                            # Nouveau wsi (ou premier) -> flush l'ancien si existant
                            if current is not None:
                                current.pop('_wsi')
                                step_intervals.append(current)
                            sr = step_records_by_idx.get(wsi)
                            tlow_bpm, thigh_bpm, tlow_w, thigh_w, tlow_rpm, thigh_rpm = _decode_step_targets(sr)
                            # Patch: on laisse les valeurs None
                            # quand la metrique n'est pas la cible du step (au
                            # lieu du sentinel -1 historique). Avec None, le row
                            # InfluxDB ne stocke pas le field -> apparait NULL
                            # dans le SELECT cote dashboard -> Grafana brise net
                            # ligne et fillBelowTo polygon, sans drop visuel
                            # vers -1 (cas observe sur Target middle qui plongait
                            # vers -1 quand (-1+-1)/2 etait calcule pendant les
                            # steps wrong-target). Le RowMarker = 1 (cf. plus
                            # bas) garantit que la row reste presente dans le
                            # result set malgre le NULL.
                            start_off = (st_utc - activity_start_time).total_seconds()
                            end_off = (end_utc - activity_start_time).total_seconds()
                            current = {
                                'start': start_off, 'end': end_off,
                                'sidx': int(wsi),
                                'occ_idx': len(step_intervals),  # unique par occurrence (merged)
                                'hr_low': tlow_bpm, 'hr_high': thigh_bpm,
                                'pwr_low': tlow_w, 'pwr_high': thigh_w,
                                'cad_low': tlow_rpm, 'cad_high': thigh_rpm,
                                '_wsi': int(wsi),  # interne, retire avant append
                            }
                        if current is not None:
                            current.pop('_wsi')
                            step_intervals.append(current)
                        if step_intervals:
                            # Patch: pre-compute moyennes HR/Power/Cadence
                            # PAR OCCURRENCE (occ_idx unique) : utile pour comparer le 1er
                            # bloc 12' vs le 3e (degradation effort). Sinon les 3 reps
                            # auraient la meme moyenne (cas precedent par sidx).
                            # Patch: 7 metriques accumulees par step :
                            # hr/pwr/cad (deja existant) + stride/vr/alt/spd (nouveau,
                            # pour overlays "Moy / step" sur les panels Stride/VR/
                            # Elevation/Pace du dashboard 03). Pace = 1000/spd cote
                            # query Grafana (spd stocke en m/s, comme Speed dans
                            # ActivityGPS). Stride en mm (comme Step_Length), VR en
                            # %, alt en m. Filtres positifs : meme criteres que les
                            # extra_filter des panels (skip 0/None pour eviter de
                            # tirer les means vers le bas pendant warmup walk).
                            step_metric_acc = {iv['occ_idx']: {
                                'hr': [0.0, 0], 'pwr': [0.0, 0], 'cad': [0.0, 0],
                                'stride': [0.0, 0], 'vr': [0.0, 0],
                                'alt': [0.0, 0], 'spd': [0.0, 0],
                            } for iv in step_intervals}
                            for parsed_rec_avg in all_records_list:
                                ts_rec_avg = parsed_rec_avg.get('timestamp')
                                if ts_rec_avg is None:
                                    continue
                                ts_utc_avg = ts_rec_avg.replace(tzinfo=pytz.UTC)
                                offset_avg = (ts_utc_avg - activity_start_time).total_seconds()
                                hr_v = parsed_rec_avg.get('heart_rate')
                                pwr_v = parsed_rec_avg.get('power')
                                cad_v = parsed_rec_avg.get('cadence')
                                stride_v = parsed_rec_avg.get('step_length')
                                vr_v = parsed_rec_avg.get('vertical_ratio')
                                alt_v = parsed_rec_avg.get('enhanced_altitude')
                                if alt_v is None:
                                    alt_v = parsed_rec_avg.get('altitude')
                                spd_v = parsed_rec_avg.get('enhanced_speed')
                                if spd_v is None:
                                    spd_v = parsed_rec_avg.get('speed')
                                for iv in step_intervals:
                                    if iv['start'] <= offset_avg < iv['end']:
                                        acc = step_metric_acc[iv['occ_idx']]
                                        if hr_v is not None and hr_v > 0:
                                            acc['hr'][0] += float(hr_v); acc['hr'][1] += 1
                                        if pwr_v is not None and pwr_v > 0:
                                            acc['pwr'][0] += float(pwr_v); acc['pwr'][1] += 1
                                        if cad_v is not None and cad_v > 0:
                                            acc['cad'][0] += float(cad_v); acc['cad'][1] += 1
                                        if stride_v is not None and stride_v > 0:
                                            acc['stride'][0] += float(stride_v); acc['stride'][1] += 1
                                        if vr_v is not None and vr_v > 0:
                                            acc['vr'][0] += float(vr_v); acc['vr'][1] += 1
                                        if alt_v is not None:
                                            acc['alt'][0] += float(alt_v); acc['alt'][1] += 1
                                        # Speed > 0.5 m/s (~7:00/km min walking pace
                                        # threshold) : evite que les pauses tirent la
                                        # mean speed vers 0 -> pace mean reflete le
                                        # vrai mouvement.
                                        if spd_v is not None and spd_v > 0.5:
                                            acc['spd'][0] += float(spd_v); acc['spd'][1] += 1
                                        break
                            step_avg = {oid: {
                                k: (a[0] / a[1] if a[1] > 0 else None)
                                for k, a in acc.items()
                            } for oid, acc in step_metric_acc.items()}
                            for parsed_record in all_records_list:
                                ts_rec = parsed_record.get('timestamp')
                                if ts_rec is None:
                                    continue
                                ts_utc = ts_rec.replace(tzinfo=pytz.UTC)
                                offset_s = (ts_utc - activity_start_time).total_seconds()
                                for iv in step_intervals:
                                    if iv['start'] <= offset_s < iv['end']:
                                        avgs = step_avg.get(iv['occ_idx'], {})
                                        # StepAvg* : TOUJOURS la mean reelle du
                                        # step (peu importe target type). Permet
                                        # a l'overlay "Moy / step" du dashboard
                                        # d'afficher la HR moyenne meme pendant
                                        # un step Power-only (ex: Velo a ~150
                                        # bpm) -> staircase visuelle continue
                                        # sur tous les steps du workout, en
                                        # plus du target prescrit (white band)
                                        # qui lui n'apparait qu'aux steps right-
                                        # target.
                                        avg_hr = float(avgs['hr']) if avgs.get('hr') is not None else None
                                        avg_pwr = float(avgs['pwr']) if avgs.get('pwr') is not None else None
                                        avg_cad = float(avgs['cad']) if avgs.get('cad') is not None else None
                                        avg_stride = float(avgs['stride']) if avgs.get('stride') is not None else None
                                        avg_vr = float(avgs['vr']) if avgs.get('vr') is not None else None
                                        avg_alt = float(avgs['alt']) if avgs.get('alt') is not None else None
                                        avg_spd = float(avgs['spd']) if avgs.get('spd') is not None else None
                                        wt_point = {
                                            "measurement": "WorkoutTarget",
                                            "time": ts_utc.isoformat(),
                                            "tags": {
                                                "Device": GARMIN_DEVICENAME,
                                                "Database_Name": INFLUXDB_DATABASE,
                                                "ActivityID": activityID,
                                                "ActivitySelector": activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
                                            },
                                            "fields": {
                                                # Target* : None si la metrique
                                                # n'est pas la cible du step.
                                                # Cote dashboard -> NULL dans le
                                                # SELECT -> Grafana brise net la
                                                # ligne/le fillBelowTo polygon
                                                # entre la fin du dernier right-
                                                # target step et le debut du
                                                # prochain. Pas besoin de boundary
                                                # marker first-row : la transition
                                                # value->None entre 2 rows
                                                # consecutives suffit.
                                                "TargetLowBPM": iv['hr_low'],
                                                "TargetHighBPM": iv['hr_high'],
                                                "TargetLowW": iv['pwr_low'],
                                                "TargetHighW": iv['pwr_high'],
                                                "TargetLowRPM": iv['cad_low'],
                                                "TargetHighRPM": iv['cad_high'],
                                                "DurationSeconds": offset_s,
                                                "StepIndex": iv['sidx'],
                                                "StepAvgHR": avg_hr,
                                                "StepAvgPower": avg_pwr,
                                                "StepAvgCadence": avg_cad,
                                                # Patch: means par step
                                                # pour Stride/VR/Altitude/Speed -> overlay
                                                # "Moy / step" staircase sur les panels
                                                # correspondants (pas de target band, juste
                                                # la realisation moyenne par step).
                                                # Stride en mm (cf. Step_Length ActivityGPS,
                                                # divise par 1000 cote query pour metres).
                                                # VR en %. Alt en m. Spd en m/s -> pace =
                                                # 1000/spd cote query (s/km).
                                                "StepAvgStride": avg_stride,
                                                "StepAvgVR": avg_vr,
                                                "StepAvgAltitude": avg_alt,
                                                "StepAvgSpeed": avg_spd,
                                                # RowMarker = 1 sur TOUTES les
                                                # rows. InfluxDB v1 omet les rows
                                                # ou TOUS les SELECT fields sont
                                                # NULL, donc les rows ou seul le
                                                # Target* est NULL (ex: pendant
                                                # un step wrong-target) seraient
                                                # exclues du result set. En
                                                # SELECTant aussi RowMarker dans
                                                # la query du dashboard, InfluxDB
                                                # garde ces rows et reporte NULL
                                                # pour le Target field principal
                                                # -> Grafana voit le NULL et
                                                # brise net ligne / polygon.
                                                "RowMarker": 1,
                                            }
                                        }
                                        points_list.append(wt_point)
                                        break
                    if KEEP_FIT_FILES:
                        os.makedirs(FIT_FILE_STORAGE_LOCATION, exist_ok=True)
                        fit_path = os.path.join(FIT_FILE_STORAGE_LOCATION, activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type + ".fit")
                        with open(fit_path, "wb") as f:
                            f.write(fit_data)
                        logging.info(f"Success : Activity ID {activityID} stored in output file {fit_path}")
        except (FileNotFoundError, FitParseError) as err:
            logging.error(err)
            logging.warning(f"Fallback : Failed to use FIT file for activityID {activityID} - Trying TCX file...")
            
            ns = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2", "ns3": "http://www.garmin.com/xmlschemas/ActivityExtension/v2"}
            try:
                tcx_file_data = garmin_obj.download_activity(activityID, dl_fmt=garmin_obj.ActivityDownloadFormat.TCX).decode("UTF-8")
                root = ET.fromstring(tcx_file_data)
                if KEEP_FIT_FILES:
                    os.makedirs(FIT_FILE_STORAGE_LOCATION, exist_ok=True)
                    activity_start_time = datetime.fromisoformat(root.findall("tcx:Activities/tcx:Activity", ns)[0].find("tcx:Id", ns).text.strip("Z"))
                    tcx_path = os.path.join(FIT_FILE_STORAGE_LOCATION, activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type + ".tcx")
                    with open(tcx_path, "w") as f:
                        f.write(tcx_file_data)
                    logging.info(f"Success : Activity ID {activityID} stored in output file {tcx_path}")
            except requests.exceptions.Timeout as err:
                logging.warning(f"Request timeout for fetching large activity record {activityID} - skipping record")
                return []
            except Exception as err:
                logging.exception(f"Unable to fetch TCX for activity record {activityID} : skipping record")
                return []

            for activity in root.findall("tcx:Activities/tcx:Activity", ns):
                activity_start_time = datetime.fromisoformat(activity.find("tcx:Id", ns).text.strip("Z"))
                lap_index = 1
                for lap in activity.findall("tcx:Lap", ns):
                    lap_start_time = datetime.fromisoformat(lap.attrib.get("StartTime").strip("Z"))
                    for tp in lap.findall(".//tcx:Trackpoint", ns):
                        time_obj = datetime.fromisoformat(tp.findtext("tcx:Time", default=None, namespaces=ns).strip("Z"))
                        lat = tp.findtext("tcx:Position/tcx:LatitudeDegrees", default=None, namespaces=ns)
                        lon = tp.findtext("tcx:Position/tcx:LongitudeDegrees", default=None, namespaces=ns)
                        alt = tp.findtext("tcx:AltitudeMeters", default=None, namespaces=ns)
                        dist = tp.findtext("tcx:DistanceMeters", default=None, namespaces=ns)
                        hr = tp.findtext("tcx:HeartRateBpm/tcx:Value", default=None, namespaces=ns)
                        speed = tp.findtext("tcx:Extensions/ns3:TPX/ns3:Speed", default=None, namespaces=ns)

                        try: lat = float(lat)
                        except: lat = None
                        try: lon = float(lon)
                        except: lon = None
                        try: alt = float(alt)
                        except: alt = None
                        try: dist = float(dist)
                        except: dist = None
                        try: hr = float(hr)
                        except: hr = None
                        try: speed = float(speed)
                        except: speed = None

                        point = {
                            "measurement": "ActivityGPS",
                            "time": time_obj.isoformat(), 
                            "tags": {
                                "Device": GARMIN_DEVICENAME,
                                "Database_Name": INFLUXDB_DATABASE,
                                "ActivityID": activityID,
                                "ActivitySelector": activity_start_time.strftime('%Y%m%dT%H%M%SUTC-') + activity_type
                            },
                            "fields": {
                                "ActivityName": activity_type,
                                "Activity_ID": activityID,
                                "Latitude": lat,
                                "Longitude": lon,
                                "Altitude": alt,
                                "Distance": dist,
                                "DurationSeconds": (time_obj - activity_start_time).total_seconds(),
                                "HeartRate": hr,
                                "Speed": speed,
                                "lap": lap_index
                            }
                        }
                        points_list.append(point)
                    
                    lap_index += 1
        logging.info(f"Success : Fetching detailed activity for Activity ID {activityID}")
        PARSED_ACTIVITY_ID_LIST.append(activityID)
    return points_list

def get_zones_definition():
    """Fetches current HR + Power zones from Garmin. Patch.

    The API returns the CURRENT state only (no history). This function writes 1
    snapshot per UTC day per sport ; multiple writes within the same day overwrite
    the same record (same timestamp + tags = upsert).

    Called once per fetch_write_bulk() invocation, NOT per date (zones are not
    date-keyed - calling per date would just rewrite the same data N times).
    """
    points_list = []
    today_iso = datetime.now(tz=pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    # HR zones - returns list of dicts (one per sport: DEFAULT, RUNNING, etc.)
    try:
        hr_data = garmin_obj.connectapi("/biometric-service/heartRateZones")
        for entry in hr_data or []:
            sport = entry.get("sport", "DEFAULT")
            fields = {}
            if entry.get("trainingMethod") is not None:
                fields["trainingMethod"] = str(entry["trainingMethod"])
            for src_key, dst_key in [
                ("restingHeartRateUsed", "restingHeartRate"),
                ("lactateThresholdHeartRateUsed", "lactateThresholdHeartRate"),
                ("maxHeartRateUsed", "maxHeartRate"),
                ("zone1Floor", "zone1Floor"),
                ("zone2Floor", "zone2Floor"),
                ("zone3Floor", "zone3Floor"),
                ("zone4Floor", "zone4Floor"),
                ("zone5Floor", "zone5Floor"),
            ]:
                v = entry.get(src_key)
                if v is not None:
                    fields[dst_key] = float(v)
            if fields:
                points_list.append({
                    "measurement": "HRZones",
                    "time": today_iso,
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "sport": sport,
                    },
                    "fields": fields,
                })
                logging.info(f"Success : Fetching HR zones for sport {sport}")
    except Exception as e:
        logging.warning(f"Failed to fetch HR zones: {e}")

    # Power zones - per sport. Try RUNNING (CYCLING endpoint may not exist for this user).
    for sport in ("RUNNING",):
        try:
            pwr_data = garmin_obj.connectapi(f"/biometric-service/powerZones/sport/{sport}")
            if not pwr_data:
                continue
            fields = {}
            for src_key in ["functionalThresholdPower", "zone1Floor", "zone2Floor",
                            "zone3Floor", "zone4Floor", "zone5Floor"]:
                v = pwr_data.get(src_key)
                if v is not None:
                    fields[src_key] = float(v)
            if fields:
                points_list.append({
                    "measurement": "PowerZones",
                    "time": today_iso,
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "sport": sport,
                    },
                    "fields": fields,
                })
                logging.info(f"Success : Fetching Power zones for sport {sport}")
        except Exception as e:
            logging.warning(f"Failed to fetch Power zones for sport {sport}: {e}")

    return points_list


def get_lactate_threshold(date_str):
    points_list = []
    endpoints = {}
    
    for ltsport in LACTATE_THRESHOLD_SPORTS:
        endpoints[f"SpeedThreshold_{ltsport}"] = f"/biometric-service/stats/lactateThresholdSpeed/range/{date_str}/{date_str}?aggregation=daily&sport={ltsport}"
        endpoints[f"HeartRateThreshold_{ltsport}"] = f"/biometric-service/stats/lactateThresholdHeartRate/range/{date_str}/{date_str}?aggregation=daily&sport={ltsport}"

    for label, endpoint in endpoints.items():
        lt_list_all = garmin_obj.connectapi(endpoint)
        if lt_list_all:
            for lt_dict in lt_list_all:
                value = lt_dict.get("value")
                if value is not None:
                    points_list.append({
                        "measurement": "LactateThreshold",
                        "time": datetime.fromtimestamp(datetime.strptime(date_str, "%Y-%m-%d").timestamp(), tz=pytz.timezone("UTC")).isoformat(),
                        "tags": {
                            "Device": GARMIN_DEVICENAME,
                            "Database_Name": INFLUXDB_DATABASE
                        },
                        "fields": {f"{label}": value}
                    })
                    logging.info(f"Success : Fetching {label} for date {date_str}")

    return points_list
    
def get_training_status(date_str):
    points_list = []
    ts_list_all = garmin_obj.get_training_status(date_str)
    ts_training_data_all = (ts_list_all.get("mostRecentTrainingStatus") or {}).get("latestTrainingStatusData", {})
    lb_data_all = (ts_list_all.get("mostRecentTrainingLoadBalance") or {}).get("metricsTrainingLoadBalanceDTOMap", {}) or {}

    if ts_training_data_all:
        for device_id, ts_dict in ts_training_data_all.items():
            logging.info(f"Success : Processing Training Status for Device {device_id}")
            lb_dict = lb_data_all.get(str(device_id)) or {}
            data_fields = {
                "trainingStatus": ts_dict.get("trainingStatus"),
                "trainingStatusFeedbackPhrase": ts_dict.get("trainingStatusFeedbackPhrase"),
                "weeklyTrainingLoad": ts_dict.get("weeklyTrainingLoad"),
                "fitnessTrend": ts_dict.get("fitnessTrend"),
                "acwrPercent": (ts_dict.get("acuteTrainingLoadDTO") or {}).get("acwrPercent"),
                "dailyTrainingLoadAcute": (ts_dict.get("acuteTrainingLoadDTO") or {}).get("dailyTrainingLoadAcute"),
                "dailyTrainingLoadChronic": (ts_dict.get("acuteTrainingLoadDTO") or {}).get("dailyTrainingLoadChronic"),
                "maxTrainingLoadChronic": (ts_dict.get("acuteTrainingLoadDTO") or {}).get("maxTrainingLoadChronic"),
                "minTrainingLoadChronic": (ts_dict.get("acuteTrainingLoadDTO") or {}).get("minTrainingLoadChronic"),
                "dailyAcuteChronicWorkloadRatio": (ts_dict.get("acuteTrainingLoadDTO") or {}).get("dailyAcuteChronicWorkloadRatio"),
                "monthlyLoadAerobicLow": lb_dict.get("monthlyLoadAerobicLow"),
                "monthlyLoadAerobicHigh": lb_dict.get("monthlyLoadAerobicHigh"),
                "monthlyLoadAnaerobic": lb_dict.get("monthlyLoadAnaerobic"),
                "monthlyLoadAerobicLowTargetMin": float(v) if (v := lb_dict.get("monthlyLoadAerobicLowTargetMin")) is not None else None,
                "monthlyLoadAerobicLowTargetMax": float(v) if (v := lb_dict.get("monthlyLoadAerobicLowTargetMax")) is not None else None,
                "monthlyLoadAerobicHighTargetMin": float(v) if (v := lb_dict.get("monthlyLoadAerobicHighTargetMin")) is not None else None,
                "monthlyLoadAerobicHighTargetMax": float(v) if (v := lb_dict.get("monthlyLoadAerobicHighTargetMax")) is not None else None,
                "monthlyLoadAnaerobicTargetMin": float(v) if (v := lb_dict.get("monthlyLoadAnaerobicTargetMin")) is not None else None,
                "monthlyLoadAnaerobicTargetMax": float(v) if (v := lb_dict.get("monthlyLoadAnaerobicTargetMax")) is not None else None,
                "trainingBalanceFeedbackPhrase": lb_dict.get("trainingBalanceFeedbackPhrase"),
            }
            if ts_dict.get("timestamp") and any(value is not None for value in data_fields.values()):
                points_list.append({
                    "measurement": "TrainingStatus",
                    "time": datetime.fromtimestamp(ts_dict["timestamp"]/1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": data_fields
                })
                logging.info(f"Success : Fetching Training Status for date {date_str}")

    # --- Heat & Altitude Acclimation (from mostRecentVO2Max in same API response) ---
    ha_data = (ts_list_all.get('mostRecentVO2Max') or {}).get('heatAltitudeAcclimation')
    if ha_data and ha_data.get('calendarDate'):
        ha_fields = {
            'heatAcclimationPercentage': ha_data.get('heatAcclimationPercentage'),
            'previousHeatAcclimationPercentage': ha_data.get('previousHeatAcclimationPercentage'),
            'heatTrend': ha_data.get('heatTrend'),
            'altitudeAcclimation': ha_data.get('altitudeAcclimation'),
            'previousAltitudeAcclimation': ha_data.get('previousAltitudeAcclimation'),
            'altitudeTrend': ha_data.get('altitudeTrend'),
            'currentAltitude': ha_data.get('currentAltitude'),
            'acclimationPercentage': ha_data.get('acclimationPercentage'),
        }
        if any(v is not None for v in ha_fields.values()):
            points_list.append({
                'measurement': 'HeatAltitudeAcclimation',
                'time': datetime.strptime(ha_data['calendarDate'], '%Y-%m-%d').replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                'tags': {
                    'Device': GARMIN_DEVICENAME,
                    'Database_Name': INFLUXDB_DATABASE
                },
                'fields': ha_fields
            })
            logging.info(f'Success : Fetching Heat/Altitude Acclimation for date {date_str}')

    return points_list

# Contribution from PR #17 by @arturgoms 
def get_training_readiness(date_str):
    points_list = []
    tr_list_all = garmin_obj.get_training_readiness(date_str)
    if tr_list_all:
        for tr_dict in tr_list_all:
            data_fields = {
                    "level": tr_dict.get("level"),
                    "score": tr_dict.get("score"),
                    "sleepScore": tr_dict.get("sleepScore"),
                    "sleepScoreFactorPercent": tr_dict.get("sleepScoreFactorPercent"),
                    "recoveryTime": tr_dict.get("recoveryTime"),
                    "recoveryTimeFactorPercent": tr_dict.get("recoveryTimeFactorPercent"),
                    "acwrFactorPercent": tr_dict.get("acwrFactorPercent"),
                    "acuteLoad": tr_dict.get("acuteLoad"),
                    "stressHistoryFactorPercent": tr_dict.get("stressHistoryFactorPercent"),
                    "hrvFactorPercent": tr_dict.get("hrvFactorPercent"),
                }
            if (not all(value is None for value in data_fields.values())) and tr_dict.get('timestamp'):
                points_list.append({
                    "measurement":  "TrainingReadiness",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(tr_dict['timestamp'],"%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": data_fields
                })
                logging.info(f"Success : Fetching Training Readiness for date {date_str}")
    return points_list

# Contribution from PR #17 by @arturgoms 
def get_hillscore(date_str):
    points_list = []
    hill = garmin_obj.get_hill_score(date_str)
    if hill:
        data_fields = {
            "strengthScore": hill.get("strengthScore"),
            "enduranceScore": hill.get("enduranceScore"),
            "hillScoreClassificationId": hill.get("hillScoreClassificationId"),
            "overallScore": hill.get("overallScore"),
            "hillScoreFeedbackPhraseId": hill.get("hillScoreFeedbackPhraseId"),
            "vo2MaxPreciseValue": hill.get("vo2MaxPreciseValue")
        }
        if not all(value is None for value in data_fields.values()):
            points_list.append({
                "measurement":  "HillScore",
                "time": datetime.strptime(date_str,"%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(), # Use GMT 00:00 for daily record
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE
                },
                "fields": data_fields
            })
            logging.info(f"Success : Fetching Hill Score for date {date_str}")
    return points_list

# Contribution from PR #17 by @arturgoms 
def get_race_predictions(date_str):
    points_list = []
    rp_all_list = garmin_obj.get_race_predictions(startdate=date_str, enddate=date_str, _type="daily")
    rp_all = rp_all_list[0] if len(rp_all_list) > 0 else {}
    if rp_all:
        data_fields = {
            "time5K": rp_all.get("time5K"),
            "time10K": rp_all.get("time10K"),
            "timeHalfMarathon": rp_all.get("timeHalfMarathon"),
            "timeMarathon": rp_all.get("timeMarathon"),
        }
        if not all(value is None for value in data_fields.values()):
            points_list.append({
                "measurement":  "RacePredictions",
                "time": datetime.strptime(date_str,"%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(), # Use GMT 00:00 for daily record
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE
                },
                "fields": data_fields
            })
            logging.info(f"Success : Fetching Race Predictions for date {date_str}")
    return points_list

def get_fitness_age(date_str):
    points_list = []
    fitness_age = garmin_obj.get_fitnessage_data(date_str)

    if fitness_age:
            data_fields = {
                "chronologicalAge": float(fitness_age.get("chronologicalAge")) if fitness_age.get("chronologicalAge") else None,
                "fitnessAge": fitness_age.get("fitnessAge"),
                "achievableFitnessAge": fitness_age.get("achievableFitnessAge"),
            }

            if not all(value is None for value in data_fields.values()):
                points_list.append({
                    "measurement": "FitnessAge",
                    "time": datetime.strptime(date_str,"%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(), # Use GMT 00:00 for daily record
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": data_fields
                })
                logging.info(f"Success : Fetching Fitness Age for date {date_str}")
    return points_list

def get_vo2_max(date_str):
    points_list = []
    max_metrics = garmin_obj.get_max_metrics(date_str)
    try:
        if max_metrics:
            vo2_max_value = (max_metrics[0].get("generic") or {}).get("vo2MaxPreciseValue", None)
            vo2_max_value_cycling = (max_metrics[0].get("cycling") or {}).get("vo2MaxPreciseValue", None)
            if vo2_max_value or vo2_max_value_cycling:
                points_list.append({
                    "measurement":  "VO2_Max",
                    "time": datetime.strptime(date_str,"%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(), # Use GMT 00:00 for daily record
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": {"VO2_max_value" : vo2_max_value, "VO2_max_value_cycling" : vo2_max_value_cycling}
                })
                logging.info(f"Success : Fetching VO2-max for date {date_str}")
        return points_list
    except AttributeError as err:
        return []

def get_endurance_score(date_str):
    points_list = []
    endurance_dict = garmin_obj.get_endurance_score(date_str)
    if endurance_dict:
        if endurance_dict.get("overallScore"):
            points_list.append({
                "measurement":  "EnduranceScore",
                "time": pytz.timezone("UTC").localize(datetime.strptime(date_str,"%Y-%m-%d")).isoformat(), # Use GMT 00:00 is timestamp is not available
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE
                },
                "fields": {
                    "EnduranceScore": endurance_dict.get("overallScore")
                    }
            })
            logging.info(f"Success : Fetching Endurance Score for date {date_str}")
    return points_list

def get_blood_pressure(date_str):
    points_list = []
    bp_all = garmin_obj.get_blood_pressure(date_str, date_str).get('measurementSummaries',[])
    if len(bp_all) > 0:
        bp_list = bp_all[0].get('measurements',[])
        for bp_measurement in bp_list:
            data_fields = {
                'Systolic': bp_measurement.get('systolic', None),
                "Diastolic": bp_measurement.get('diastolic', None),
                "Pulse": bp_measurement.get('pulse', None)
            }
            if not all(value is None for value in data_fields.values()) and 'measurementTimestampGMT' in bp_measurement:
                points_list.append({
                    "measurement":  "BloodPressure",
                    "time": pytz.UTC.localize(datetime.strptime(bp_measurement['measurementTimestampGMT'], '%Y-%m-%dT%H:%M:%S.%f')),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "Source": bp_measurement.get('sourceType', None)
                    },
                    "fields": data_fields
                })
        logging.info(f"Success : Fetching Blood Pressure for date {date_str}")
    return points_list

def get_hydration(date_str):
    points_list = []
    hydration_dict = garmin_obj.get_hydration_data(date_str)
    data_fields = {
        'ValueInML': hydration_dict.get('valueInML', None),
        "SweatLossInML": hydration_dict.get('sweatLossInML', None),
        "GoalInML": hydration_dict.get('goalInML', None),
        "ActivityIntakeInML": hydration_dict.get('activityIntakeInML', None)
    }
    if not all(value is None for value in data_fields.values()):
        points_list.append({
            "measurement":  "Hydration",
            "time": datetime.strptime(date_str,"%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(), # Use GMT 00:00 for daily record
            "tags": {
                "Device": GARMIN_DEVICENAME,
                "Database_Name": INFLUXDB_DATABASE
            },
            "fields": data_fields
        })
        logging.info(f"Success : Fetching Hydration data for date {date_str}")
    return points_list


def get_solar_intensity(date_str):
    points_list = []

    if not GARMIN_DEVICEID:
        logging.warning("Skipping Solar Intensity data fetch as GARMIN_DEVICEID is not set.")
        return points_list

    si_all = garmin_obj.get_device_solar_data(GARMIN_DEVICEID, date_str) or {}
    if len(si_all.get('solarDailyDataDTOs', [])) > 0:
        si_list = si_all['solarDailyDataDTOs'][0].get('solarInputReadings', [])
        for si_measurement in si_list:
            data_fields = {
                'solarUtilization': si_measurement.get('solarUtilization', None),
                'activityTimeGainMs': si_measurement.get('activityTimeGainMs', None),
            }
            if not all(value is None for value in data_fields.values()) and 'readingTimestampGmt' in si_measurement:
                points_list.append({
                    "measurement":  "SolarIntensity",
                    "time": pytz.UTC.localize(datetime.strptime(si_measurement['readingTimestampGmt'], '%Y-%m-%dT%H:%M:%S.%f')),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE
                    },
                    "fields": data_fields
                })
        logging.info(f"Success : Fetching Solar Intensity data for date {date_str}")
    if len(points_list) == 0:
        logging.warning(f"No Solar Intensity data available for date {date_str}")
    return points_list

# %%
def get_lifestyle_data(date_str):
    points_list = []
    try:
        logging.info(f"Fetching Lifestyle Journaling data for date {date_str}")
        journal_data = garmin_obj.get_lifestyle_logging_data(date_str)
        
        daily_logs = journal_data.get('dailyLogsReport', [])
        
        for log in daily_logs:
            behavior_name = log.get('name') or log.get('behavior')
            if not behavior_name:
                continue

            category = log.get('category', 'UNKNOWN')
            log_status = log.get('logStatus')
            details = log.get('details', [])
            
            # status: 1 for YES, 0 for NO
            status = 1 if log_status == "YES" else 0
            
            # value: sum of detail amounts if available, else 0.0
            value = 0.0
            if details:
                for detail in details:
                    amount = detail.get('amount')
                    if amount is not None:
                        value += float(amount)

            fields = {
                "status": status,
                "value": value
            }

            points_list.append({
                "measurement": "LifestyleJournal",
                "time": pytz.timezone("UTC").localize(datetime.strptime(date_str, "%Y-%m-%d")).isoformat(),
                "tags": {
                    "Device": GARMIN_DEVICENAME,
                    "Database_Name": INFLUXDB_DATABASE,
                    "behavior": behavior_name,
                    "category": category
                },
                "fields": fields
            })
            
        logging.info(f"Success : Fetching Lifestyle Journaling data for date {date_str}")

    except Exception as e:
        logging.warning(f"Failed to fetch Lifestyle Journaling data for date {date_str}: {e}")
    
    return points_list


# %%
def daily_fetch_write(date_str):
    if REQUEST_INTRADAY_DATA_REFRESH and (datetime.strptime(date_str, "%Y-%m-%d") <= (datetime.today() - timedelta(days=IGNORE_INTRADAY_DATA_REFRESH_DAYS))):
        data_refresh_response = garmin_obj.connectapi(f"wellness-service/wellness/epoch/request/{date_str}", method="POST").get("status", "Unknown")
        logging.info(f"Intraday data refresh request status: {data_refresh_response}")
        if data_refresh_response == "SUBMITTED":
            logging.info(f"Waiting 10 seconds for refresh request to process...")
            time.sleep(10)
        elif data_refresh_response == "COMPLETE":
            logging.info(f"Data for date {date_str} is already available")
        elif data_refresh_response == "NO_FILES_FOUND":
            logging.info(f"No Data is available for date {date_str} to refresh")
            return None
        elif data_refresh_response == "DENIED":
            logging.info(f"Daily refresh limit reached. Pausing script for 24 hours to ensure Intraday data fetching. Disable REQUEST_INTRADAY_DATA_REFRESH to avoid this!")
            time.sleep(86500)
            data_refresh_response = garmin_obj.connectapi(f"wellness-service/wellness/epoch/request/{date_str}", method="POST").get("status", "Unknown")
            logging.info(f"Intraday data refresh request status: {data_refresh_response}")
            logging.info(f"Waiting 10 seconds...")
            time.sleep(10)
        else:
            logging.info(f"Refresh response is unknown!")
            time.sleep(5)
    if 'daily_avg' in FETCH_SELECTION:
        write_points_to_influxdb(get_daily_stats(date_str))
    if 'sleep' in FETCH_SELECTION:
        write_points_to_influxdb(get_sleep_data(date_str))
    if 'steps' in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_steps(date_str))
    if 'heartrate' in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_hr(date_str))
    if 'stress' in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_stress(date_str))
    if 'breathing' in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_br(date_str))
    if 'hrv' in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_hrv(date_str))
    if 'fitness_age' in FETCH_SELECTION:
        write_points_to_influxdb(get_fitness_age(date_str))
    if 'vo2' in FETCH_SELECTION:
        write_points_to_influxdb(get_vo2_max(date_str))
    if 'race_prediction' in FETCH_SELECTION:
        write_points_to_influxdb(get_race_predictions(date_str))
    if 'body_composition' in FETCH_SELECTION:
        write_points_to_influxdb(get_body_composition(date_str))
    if 'lactate_threshold' in FETCH_SELECTION:
        write_points_to_influxdb(get_lactate_threshold(date_str))
    if 'training_status' in FETCH_SELECTION:
        write_points_to_influxdb(get_training_status(date_str))
    if 'training_readiness' in FETCH_SELECTION:
        write_points_to_influxdb(get_training_readiness(date_str))
    if 'hill_score' in FETCH_SELECTION:
        write_points_to_influxdb(get_hillscore(date_str))
    if 'endurance_score' in FETCH_SELECTION:
        write_points_to_influxdb(get_endurance_score(date_str))
    if 'blood_pressure' in FETCH_SELECTION:
        write_points_to_influxdb(get_blood_pressure(date_str))
    if 'hydration' in FETCH_SELECTION:
        write_points_to_influxdb(get_hydration(date_str))
    if 'activity' in FETCH_SELECTION:
        activity_summary_points_list, activity_with_gps_id_dict, strength_activity_id_dict, activity_weather_info = get_activity_summary(date_str)
        write_points_to_influxdb(activity_summary_points_list)
        write_points_to_influxdb(fetch_activity_GPS(activity_with_gps_id_dict))
        if strength_activity_id_dict:
            write_points_to_influxdb(get_strength_training_data(strength_activity_id_dict))
        if 'weather' in FETCH_SELECTION and activity_weather_info:
            weather_points = fetch_activity_weather(activity_weather_info)
            if weather_points:
                write_points_to_influxdb(weather_points)

    if 'solar_intensity' in FETCH_SELECTION:
        write_points_to_influxdb(get_solar_intensity(date_str))
    if 'lifestyle' in FETCH_SELECTION:
        write_points_to_influxdb(get_lifestyle_data(date_str))


# %%
def fetch_write_bulk(start_date_str, end_date_str):
    global garmin_obj
    consecutive_500_errors = 0
    logging.info("Fetching data for the given period in reverse chronological order")
    time.sleep(3)
    write_points_to_influxdb(get_last_sync())
    # Zones HR + Power : snapshot 1x par cycle (API ne renvoie que l'etat courant)
    if 'zones' in FETCH_SELECTION:
        try:
            write_points_to_influxdb(get_zones_definition())
        except Exception as e:
            logging.warning(f"Zones snapshot failed: {e}")
    for current_date in iter_days(start_date_str, end_date_str):
        repeat_loop = True
        while repeat_loop:
            try:
                daily_fetch_write(current_date)
                # Reset consecutive 500 error counter on successful fetch
                if consecutive_500_errors > 0:
                    logging.info(f"Successfully fetched data after {consecutive_500_errors} consecutive 500 errors - resetting error counter")
                    consecutive_500_errors = 0
                logging.info(f"Success : Fetched all available health metrics for date {current_date} (skipped any if unavailable)")
                if RATE_LIMIT_CALLS_SECONDS > 0:
                    logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds")
                    time.sleep(RATE_LIMIT_CALLS_SECONDS)
                repeat_loop = False
            except GarminConnectTooManyRequestsError as err:
                logging.error(err)
                logging.info(f"Too many requests (429) : Failed to fetch one or more metrics - will retry for date {current_date}")
                logging.info(f"Waiting : for {FETCH_FAILED_WAIT_SECONDS} seconds")
                time.sleep(FETCH_FAILED_WAIT_SECONDS)
                repeat_loop = True
            except (requests.exceptions.HTTPError, GarminConnectConnectionError) as err:
                # Check if this is a 500 error
                is_500_error = _is_http_status_error(err, 500)
                
                if is_500_error:
                    consecutive_500_errors += 1
                    logging.error(f"HTTP 500 error ({consecutive_500_errors}/{MAX_CONSECUTIVE_500_ERRORS}) for date {current_date}: {err}")
                    if consecutive_500_errors >= MAX_CONSECUTIVE_500_ERRORS:
                        logging.warning(f"Received {consecutive_500_errors} consecutive HTTP 500 errors. Logging error and continuing backward in time to fetch remaining data.")
                        logging.warning(f"Skipping date {current_date} due to persistent 500 errors from Garmin API")
                        logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds before continuing")
                        time.sleep(RATE_LIMIT_CALLS_SECONDS)
                        repeat_loop = False
                    else:
                        logging.info(f"HTTP 500 error encountered - will retry for date {current_date} (attempt {consecutive_500_errors}/{MAX_CONSECUTIVE_500_ERRORS})")
                        logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds before retry")
                        time.sleep(RATE_LIMIT_CALLS_SECONDS)
                        repeat_loop = True
                else:
                    # Non-500 HTTP errors - handle as before
                    logging.error(err)
                    logging.info(f"HTTP Error (non-500) : Failed to fetch one or more metrics - skipping date {current_date}")
                    logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds")
                    time.sleep(RATE_LIMIT_CALLS_SECONDS)
                    repeat_loop = False
            except (
                    GarminConnectConnectionError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout
                    ) as err:
                logging.error(err)
                logging.info(f"Connection Error : Failed to fetch one or more metrics - skipping date {current_date}")
                logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds")
                time.sleep(RATE_LIMIT_CALLS_SECONDS)
                repeat_loop = False
            except GarminConnectAuthenticationError as err:
                logging.error(err)
                logging.info(f"Authentication Failed : Retrying login with given credentials (won't work automatically for MFA/2FA enabled accounts)")
                garmin_obj = garmin_login()
                time.sleep(5)
                repeat_loop = True
            except Exception as err:
                if IGNORE_ERRORS:
                    logging.warning("IGNORE_ERRORS Enabled >> Failed to process %s:", current_date)
                    logging.exception(err)
                    repeat_loop = False
                else:
                    raise err


if __name__ == "__main__":
    garmin_obj = garmin_login()

    # %%
    if MANUAL_START_DATE:
        fetch_write_bulk(MANUAL_START_DATE, MANUAL_END_DATE)
        logging.info(f"Bulk update success : Fetched all available health metrics for date range {MANUAL_START_DATE} to {MANUAL_END_DATE}")
        exit(0)
    else:
        try:
            if INFLUXDB_VERSION == "1":
                last_influxdb_sync_time_UTC = pytz.utc.localize(datetime.strptime(list(influxdbclient.query(f"SELECT * FROM HeartRateIntraday ORDER BY time DESC LIMIT 1").get_points())[0]['time'],"%Y-%m-%dT%H:%M:%SZ"))
            else:
                last_influxdb_sync_time_UTC = pytz.utc.localize(influxdbclient.query(query="SELECT * FROM HeartRateIntraday ORDER BY time DESC LIMIT 1", language="influxql").to_pylist()[0]['time'])
        except Exception as err:
            logging.error(err)
            logging.warning("No previously synced data found in local InfluxDB database, defaulting to 7 day initial fetching. Use specific start date ENV variable to bulk update past data")
            last_influxdb_sync_time_UTC = (datetime.today() - timedelta(days=7)).astimezone(pytz.timezone("UTC"))
        try:
            if USER_TIMEZONE: # If provided by user, using that. 
                local_timediff = datetime.now(tz=pytz.timezone(USER_TIMEZONE)).utcoffset()
            else: # otherwise try to set automatically
                last_activity_dict = garmin_obj.get_last_activity() # (very unlineky event that this will be empty given Garmin's userbase, everyone should have at least one activity)
                local_timediff = datetime.strptime(last_activity_dict['startTimeLocal'], '%Y-%m-%d %H:%M:%S') - datetime.strptime(last_activity_dict['startTimeGMT'], '%Y-%m-%d %H:%M:%S')
            if local_timediff >= timedelta(0):
                logging.info("Using user's local timezone as UTC+" + str(local_timediff))
            else:
                logging.info("Using user's local timezone as UTC-" + str(-local_timediff))
        except (KeyError, TypeError) as err:
            logging.warning(f"Unable to determine user's timezone - Defaulting to UTC. Consider providing TZ identifier with USER_TIMEZONE environment variable")
            local_timediff = timedelta(hours=0)
        
        while True:
            last_watch_sync_time_UTC = datetime.fromtimestamp(int(garmin_obj.get_device_last_used().get('lastUsedDeviceUploadTime')/1000)).astimezone(pytz.timezone("UTC"))
            if last_influxdb_sync_time_UTC < last_watch_sync_time_UTC:
                logging.info(f"Update found : Current watch sync time is {last_watch_sync_time_UTC} UTC")
                fetch_write_bulk((last_influxdb_sync_time_UTC + local_timediff).strftime('%Y-%m-%d'), (last_watch_sync_time_UTC + local_timediff).strftime('%Y-%m-%d')) # Using local dates for deciding which dates to fetch in current iteration (see issue #25)
                last_influxdb_sync_time_UTC = last_watch_sync_time_UTC
            else:
                logging.info(f"No new data found : Current watch and influxdb sync time is {last_watch_sync_time_UTC} UTC")
            logging.info(f"waiting for {UPDATE_INTERVAL_SECONDS} seconds before next automatic update calls")
            time.sleep(UPDATE_INTERVAL_SECONDS)
