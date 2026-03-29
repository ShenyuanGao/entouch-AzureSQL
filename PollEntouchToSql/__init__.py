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
        "Accept": "application/hal+json",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def get_sql_connection():
    conn_str = os.environ["SQL_CONNECTION_STRING"]
    return pyodbc.connect(conn_str)


def f_to_c(value):
    if value is None:
        return None
    return round((float(value) - 32.0) * 5.0 / 9.0, 1)


def evaluate_status(temp_f, heat_f, cool_f):
    temp_c = f_to_c(temp_f)
    heat_c = f_to_c(heat_f)
    cool_c = f_to_c(cool_f)

    if temp_c is None:
        return "NO_DATA", temp_c, heat_c, cool_c

    if temp_c <= 0:
        return "CRITICAL", temp_c, heat_c, cool_c

    if heat_c is not None and temp_c < heat_c - 3:
        return "WARNING", temp_c, heat_c, cool_c

    if cool_c is not None and temp_c > cool_c + 3:
        return "WARNING", temp_c, heat_c, cool_c

    return "NORMAL", temp_c, heat_c, cool_c


def should_send_again(last_sent_at, cooldown_minutes):
    if last_sent_at is None:
        return True
    return utc_now() >= (last_sent_at + datetime.timedelta(minutes=cooldown_minutes))


def build_payload(
    event_type,
    status,
    controller_id,
    controller_name,
    last_comm_utc,
    temp_c,
    heat_c,
    cool_c,
    recipients,
):
    subject = f"[HVAC Alert][{status}] {controller_name} ({controller_id})"

    body_html = f"""
    <p><strong>Event:</strong> {event_type}</p>
    <p><strong>Status:</strong> {status}</p>
    <p><strong>Controller:</strong> {controller_name} ({controller_id})</p>
    <p><strong>Reading time:</strong> {last_comm_utc}</p>
    <p><strong>Temperature:</strong> {temp_c} C</p>
    <p><strong>Heat setpoint:</strong> {heat_c} C</p>
    <p><strong>Cool setpoint:</strong> {cool_c} C</p>
    """

    return {
        "event_type": event_type,
        "status": status,
        "controller_id": controller_id,
        "controller_name": controller_name,
        "reading_time_utc": str(last_comm_utc) if last_comm_utc else None,
        "temp_c": temp_c,
        "heat_setpoint_c": heat_c,
        "cool_setpoint_c": cool_c,
        "recipients": recipients,
        "subject": subject,
        "body_html": body_html,
    }


def call_logic_app(payload):
    logic_app_url = os.environ["LOGIC_APP_URL"]
    r = requests.post(logic_app_url, json=payload, timeout=30)
    r.raise_for_status()


def get_recipients(cursor, controller_id):
    cursor.execute(
        """
        SELECT RecipientEmail
        FROM AlertRecipients
        WHERE Enabled = 1
          AND (ControllerId = ? OR ControllerId IS NULL)
        """,
        controller_id,
    )
    return [row[0] for row in cursor.fetchall()]


def get_alert_state(cursor, controller_id):
    cursor.execute(
        """
        SELECT ControllerId, CurrentStatus, LastSentAtUtc
        FROM AlertState
        WHERE ControllerId = ?
        """,
        controller_id,
    )
    return cursor.fetchone()


def insert_alert_event(
    cursor,
    controller_id,
    controller_name,
    event_type,
    status,
    event_time_utc,
    temp_c,
    heat_c,
    cool_c,
    recipients,
    payload,
):
    cursor.execute(
        """
        INSERT INTO AlertEvents (
            ControllerId,
            ControllerName,
            EventType,
            Status,
            EventTimeUtc,
            TempC,
            HeatSetPointC,
            CoolSetPointC,
            EmailTo,
            PayloadJson
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        controller_id,
        controller_name,
        event_type,
        status,
        event_time_utc,
        temp_c,
        heat_c,
        cool_c,
        ";".join(recipients) if recipients else None,
        json.dumps(payload),
    )


def upsert_alert_state(
    cursor,
    controller_id,
    controller_name,
    status,
    now_utc,
    temp_c,
    heat_c,
    cool_c,
    update_last_sent,
):
    cursor.execute(
        """
        SELECT ControllerId
        FROM AlertState
        WHERE ControllerId = ?
        """,
        controller_id,
    )
    exists = cursor.fetchone() is not None

    if exists:
        if update_last_sent:
            cursor.execute(
                """
                UPDATE AlertState
                SET ControllerName = ?,
                    CurrentStatus = ?,
                    LastSeenAtUtc = ?,
                    LastSentAtUtc = ?,
                    LastTempC = ?,
                    LastHeatSetPointC = ?,
                    LastCoolSetPointC = ?
                WHERE ControllerId = ?
                """,
                controller_name,
                status,
                now_utc,
                now_utc,
                temp_c,
                heat_c,
                cool_c,
                controller_id,
            )
        else:
            cursor.execute(
                """
                UPDATE AlertState
                SET ControllerName = ?,
                    CurrentStatus = ?,
                    LastSeenAtUtc = ?,
                    LastTempC = ?,
                    LastHeatSetPointC = ?,
                    LastCoolSetPointC = ?
                WHERE ControllerId = ?
                """,
                controller_name,
                status,
                now_utc,
                temp_c,
                heat_c,
                cool_c,
                controller_id,
            )
    else:
        cursor.execute(
            """
            INSERT INTO AlertState (
                ControllerId,
                ControllerName,
                CurrentStatus,
                OpenedAtUtc,
                LastSeenAtUtc,
                LastSentAtUtc,
                ClearedAtUtc,
                LastTempC,
                LastHeatSetPointC,
                LastCoolSetPointC
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            controller_id,
            controller_name,
            status,
            now_utc,
            now_utc,
            now_utc if update_last_sent else None,
            None,
            temp_c,
            heat_c,
            cool_c,
        )


def clear_alert_state(cursor, controller_id, now_utc, temp_c, heat_c, cool_c):
    cursor.execute(
        """
        UPDATE AlertState
        SET CurrentStatus = 'NORMAL',
            LastSeenAtUtc = ?,
            ClearedAtUtc = ?,
            LastTempC = ?,
            LastHeatSetPointC = ?,
            LastCoolSetPointC = ?
        WHERE ControllerId = ?
        """,
        now_utc,
        now_utc,
        temp_c,
        heat_c,
        cool_c,
        controller_id,
    )


def process_alert_for_controller(cursor, detail_json):
    cooldown_minutes = int(os.environ.get("ALERT_COOLDOWN_MINUTES", "60"))
    send_cleared_email = os.environ.get("SEND_CLEARED_EMAIL", "true").lower() == "true"

    controller_id = detail_json.get("Id")
    controller_name = detail_json.get("Name")
    temp_f = detail_json.get("Temperature")
    heat_f = detail_json.get("HeatSetPoint")
    cool_f = detail_json.get("CoolSetPoint")
    last_comm_utc = detail_json.get("LastCommUtc")
    is_online = detail_json.get("IsOnline")

    if controller_id is None:
        logging.info("Skipping controller with missing Id")
        return

    if is_online is False:
        logging.info("Skipping offline controller %s", controller_id)
        return

    status, temp_c, heat_c, cool_c = evaluate_status(temp_f, heat_f, cool_f)

    if status == "NO_DATA":
        logging.info("Skipping controller %s because temperature is missing", controller_id)
        return

    recipients = get_recipients(cursor, controller_id)
    if not recipients:
        logging.info("No recipients configured for controller %s", controller_id)
        return

    state = get_alert_state(cursor, controller_id)
    now_utc = utc_now()

    previous_status = None if state is None else state[1]
    last_sent_at = None if state is None else state[2]

    if status in ("WARNING", "CRITICAL"):
        if state is None or previous_status == "NORMAL":
            payload = build_payload(
                "OPENED",
                status,
                controller_id,
                controller_name,
                last_comm_utc,
                temp_c,
                heat_c,
                cool_c,
                recipients,
            )
            call_logic_app(payload)
            upsert_alert_state(
                cursor,
                controller_id,
                controller_name,
                status,
                now_utc,
                temp_c,
                heat_c,
                cool_c,
                update_last_sent=True,
            )
            insert_alert_event(
                cursor,
                controller_id,
                controller_name,
                "OPENED",
                status,
                now_utc,
                temp_c,
                heat_c,
                cool_c,
                recipients,
                payload,
            )
            logging.info("Opened alert for controller %s", controller_id)
            return

        if should_send_again(last_sent_at, cooldown_minutes):
            payload = build_payload(
                "REMINDER",
                status,
                controller_id,
                controller_name,
                last_comm_utc,
                temp_c,
                heat_c,
                cool_c,
                recipients,
            )
            call_logic_app(payload)
            upsert_alert_state(
                cursor,
                controller_id,
                controller_name,
                status,
                now_utc,
                temp_c,
                heat_c,
                cool_c,
                update_last_sent=True,
            )
            insert_alert_event(
                cursor,
                controller_id,
                controller_name,
                "REMINDER",
                status,
                now_utc,
                temp_c,
                heat_c,
                cool_c,
                recipients,
                payload,
            )
            logging.info("Sent reminder for controller %s", controller_id)
            return

        upsert_alert_state(
            cursor,
            controller_id,
            controller_name,
            status,
            now_utc,
            temp_c,
            heat_c,
            cool_c,
            update_last_sent=False,
        )
        logging.info("Alert still active and cooldown not expired for controller %s", controller_id)
        return

    if status == "NORMAL":
        if state is not None and previous_status in ("WARNING", "CRITICAL"):
            if send_cleared_email:
                payload = build_payload(
                    "CLEARED",
                    "NORMAL",
                    controller_id,
                    controller_name,
                    last_comm_utc,
                    temp_c,
                    heat_c,
                    cool_c,
                    recipients,
                )
                call_logic_app(payload)
                insert_alert_event(
                    cursor,
                    controller_id,
                    controller_name,
                    "CLEARED",
                    "NORMAL",
                    now_utc,
                    temp_c,
                    heat_c,
                    cool_c,
                    recipients,
                    payload,
                )

            clear_alert_state(cursor, controller_id, now_utc, temp_c, heat_c, cool_c)
            logging.info("Cleared alert for controller %s", controller_id)


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
    for controller in controllers:
        cid = controller.get("Id")
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
            list_raw,
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
                detail_raw,
            )

            detail_json = json.loads(detail_raw)
            process_alert_for_controller(cursor, detail_json)

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
