import os
import json
import datetime
import logging
import requests
import pyodbc

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def utc_now():
    return datetime.datetime.utcnow()


def get_sql_connection():
    conn_str = os.environ["SQL_CONNECTION_STRING"]
    return pyodbc.connect(conn_str)


def main(mytimer) -> None:
    logging.info("Weather timer triggered")

    latitude = 43.7284
    longitude = -79.6077

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m",
        "timezone": "America/Toronto",
    }

    conn = None
    cursor = None

    try:
        response = requests.get(
            OPEN_METEO_URL,
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()

        outdoor_temp_c = data.get("current", {}).get("temperature_2m")
        weather_time = data.get("current", {}).get("time")

        logging.info(
            "Outdoor temp from Open Meteo: %s C at %s",
            outdoor_temp_c,
            weather_time,
        )

        conn = get_sql_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO dbo.WeatherSnapshot (
                SnapshotTsUtc,
                LocationName,
                Latitude,
                Longitude,
                OutdoorTempC,
                WeatherTimeLocal,
                Source,
                RawJson
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            utc_now(),
            "Toronto / Purolator Area",
            latitude,
            longitude,
            outdoor_temp_c,
            weather_time,
            "Open-Meteo",
            json.dumps(data),
        )

        conn.commit()
        logging.info("Weather snapshot inserted successfully")

    except Exception:
        if conn:
            conn.rollback()
        logging.exception("Failed to pull or insert weather data")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()