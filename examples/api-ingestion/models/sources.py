"""Source models — fetch weather data from the Open-Meteo REST API.

Demonstrates: API client, retry policies, caching, scheduling, cursor-based
incremental ingestion, and side-effect models.

Open-Meteo (https://open-meteo.com/) is a free weather API that requires no
authentication, making it ideal for learning API ingestion patterns.
"""

from datetime import date, timedelta

import ibis

from interlace import API, get_logger, model
from interlace.core.retry.policy import API_RETRY_POLICY

logger = get_logger("interlace.examples.api_ingestion")

# Shared API client — all models share the same rate limiter and session pool.
api = API(base_url="https://api.open-meteo.com/v1", max_concurrent=10)

# London coordinates
LAT = 51.5074
LON = -0.1278


# ---------------------------------------------------------------------------
# 1. weather_current — simple GET with retry + cache
# ---------------------------------------------------------------------------
@model(
    name="weather_current",
    strategy="replace",
    retry_policy=API_RETRY_POLICY,
    cache={"ttl": "1h"},
    description="Current weather conditions for London via Open-Meteo",
    tags=["source", "api"],
)
async def weather_current():
    """Fetch current temperature, wind speed, and humidity for London.

    Uses API_RETRY_POLICY (5 attempts, exponential backoff) so transient
    network errors are handled automatically. The 1-hour cache avoids
    redundant calls when re-running the pipeline within the same hour.
    """
    async with api:
        data = await api.get(
            "forecast",
            params={
                "latitude": LAT,
                "longitude": LON,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            },
            data_attribute=None,
            dataframe=False,
        )

    current = data["current"]
    return ibis.memtable(
        [
            {
                "location": "London",
                "temperature_c": current["temperature_2m"],
                "humidity_pct": current["relative_humidity_2m"],
                "wind_speed_kmh": current["wind_speed_10m"],
                "measured_at": current["time"],
            }
        ]
    )


# ---------------------------------------------------------------------------
# 2. weather_forecast — scheduled hourly refresh
# ---------------------------------------------------------------------------
@model(
    name="weather_forecast",
    strategy="replace",
    schedule={"every_s": "3600"},
    description="7-day daily forecast for London, refreshed hourly",
    tags=["source", "api"],
)
async def weather_forecast():
    """Fetch a 7-day daily forecast including min/max temperature,
    precipitation probability, and max wind speed.

    The schedule ensures this model re-executes every hour when the
    pipeline is running via ``interlace serve``.
    """
    async with api:
        data = await api.get(
            "forecast",
            params={
                "latitude": LAT,
                "longitude": LON,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max,wind_speed_10m_max",
                "timezone": "Europe/London",
                "forecast_days": 7,
            },
            data_attribute=None,
            dataframe=False,
        )

    daily = data["daily"]
    rows = []
    for i in range(len(daily["time"])):
        rows.append(
            {
                "date": daily["time"][i],
                "temp_max_c": daily["temperature_2m_max"][i],
                "temp_min_c": daily["temperature_2m_min"][i],
                "precip_prob_pct": daily["precipitation_probability_max"][i],
                "wind_max_kmh": daily["wind_speed_10m_max"][i],
            }
        )

    return ibis.memtable(rows)


# ---------------------------------------------------------------------------
# 3. weather_historical — cursor-based incremental ingestion
# ---------------------------------------------------------------------------
@model(
    name="weather_historical",
    strategy="append",
    cursor="date",
    description="Historical daily temperatures for London, incrementally appended",
    tags=["source", "api", "incremental"],
)
async def weather_historical():
    """Fetch the last 30 days of historical weather data.

    The ``cursor="date"`` parameter tells the executor to track the most
    recent date that has been materialised. On subsequent runs only rows
    with ``date > last_cursor_value`` are appended, avoiding duplicates
    and unnecessary API calls for data already ingested.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    async with api:
        data = await api.get(
            "forecast",
            params={
                "latitude": LAT,
                "longitude": LON,
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "Europe/London",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "past_days": 0,
            },
            data_attribute=None,
            dataframe=False,
        )

    daily = data["daily"]
    rows = []
    for i in range(len(daily["time"])):
        rows.append(
            {
                "date": daily["time"][i],
                "temp_max_c": daily["temperature_2m_max"][i],
                "temp_min_c": daily["temperature_2m_min"][i],
            }
        )

    return ibis.memtable(rows)


# ---------------------------------------------------------------------------
# 4. weather_alerts — side-effect model (materialise="none")
# ---------------------------------------------------------------------------
@model(
    name="weather_alerts",
    materialise="none",
    dependencies=["weather_forecast"],
    description="Log warnings for extreme forecast temperatures (side-effect only)",
    tags=["alerts", "side-effect"],
)
def weather_alerts(weather_forecast: ibis.Table):
    """Check the 7-day forecast for extreme temperatures and log warnings.

    This model does not persist any output (``materialise="none"``). It
    exists purely for its side effect: emitting log warnings when the
    forecast contains dangerously high (>35C) or low (<-10C) temperatures.
    """
    df = weather_forecast.execute()

    for _, row in df.iterrows():
        if row["temp_max_c"] > 35:
            logger.warning(
                f"Extreme heat warning for {row['date']}: {row['temp_max_c']}C"
            )
        if row["temp_min_c"] < -10:
            logger.warning(
                f"Extreme cold warning for {row['date']}: {row['temp_min_c']}C"
            )

    alert_count = len(df[(df["temp_max_c"] > 35) | (df["temp_min_c"] < -10)])
    logger.info(f"Weather alerts check complete: {alert_count} extreme day(s) found")
