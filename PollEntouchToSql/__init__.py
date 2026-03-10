import os
import json
import uuid
import datetime
import logging
import requests
import pyodbc

BASE_URL = "https://api.entouchgo.com"


def utc_now():
    return datetime.datetime.utcnow()


def get_session_token():
    api_key = os.environ["ENTOUCH_API_KEY"]

    url = f"{BASE_URL}/tokens"
    payload = {"ApiKey": api_key, "RememberMe": False}

    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()

    data = r.json()
    token = data.get("SessionToken") or data.get("sessionToken") or data.get("token")
    if not token:
        raise RuntimeError(f"Session token missing. Response: {data}")

    return token


def entouch_get(session_token, rel_path):
    url = f"{BASE_URL}{rel_path}"
    headers = {
        "API-Session-Token": session_token,
        "Accept": "application/hal+json"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def get_sql_connection():
    conn_str = os.environ["SQL_CONNECTION_STRING"]
    return pyodbc.connect(conn_str)


def main(mytimer) -> None:
    logging.info("SQL timer triggered")

    customer_id = os.environ["ENTOUCH_CUSTOMER_ID"]
    facility_id = os.environ["ENTOUCH_FACILITY_ID"]

    snapshot_ts = utc_now()
    snapshot_id = str(uuid.uuid4())

    token = get_session_token()

    list_path = f"/customers/{customer_id}/facilities/{facility_id}/hvac-controllers"
    list_raw = entouch_get(token, list_path)

    list_json = json.loads(list_raw)
    controllers = list_json.get("_embedded", {}).get("resource:hvac-controllers", [])

    controller_ids = []
    for c in controllers:
        cid = c.get("Id")
        if cid is not None:
            controller_ids.append(int(cid))

    conn = None
    cursor = None

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO EntouchSnapshot
            (SnapshotId, SnapshotTsUtc, CustomerId, FacilityId, HvacListJson)
            VALUES (?, ?, ?, ?, ?)
            """,
            snapshot_id,
            snapshot_ts,
            str(customer_id),
            str(facility_id),
            list_raw
        )

        inserted_count = 0

        for cid in controller_ids:
            detail_path = f"/customers/{customer_id}/facilities/{facility_id}/hvac-controllers/{cid}"
            detail_raw = entouch_get(token, detail_path)

            cursor.execute(
                """
                INSERT INTO EntouchControllerDetail
                (SnapshotId, SnapshotTsUtc, ControllerId, DetailJson)
                VALUES (?, ?, ?, ?)
                """,
                snapshot_id,
                snapshot_ts,
                cid,
                detail_raw
            )
            inserted_count += 1

        conn.commit()
        logging.info("Inserted snapshot and %s controller detail rows", inserted_count)

    except Exception:
        if conn:
            conn.rollback()
        logging.exception("Failed writing EnTouch data to Azure SQL")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()