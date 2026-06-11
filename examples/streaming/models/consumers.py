"""Consumer models — process stream data into analytics tables."""

import ibis

from interlace import get_logger, model

logger = get_logger("interlace.examples.streaming")


@model(
    name="sensor_hourly_avg",
    strategy="append",
    cursor="rowid",
    description="Hourly average temperature and humidity per sensor",
    tags=["iot", "aggregation"],
)
def sensor_hourly_avg(sensor_readings: ibis.Table) -> ibis.Table:
    """Aggregate sensor readings into hourly averages.

    Groups readings by sensor_id and hour, computing the mean
    temperature and humidity plus a count of readings in each bucket.
    """
    hourly = sensor_readings.mutate(
        hour=sensor_readings.timestamp.cast("string").substr(0, 13),
    )
    return hourly.group_by(["sensor_id", "hour"]).agg(
        avg_temperature=hourly.temperature.mean(),
        avg_humidity=hourly.humidity.mean(),
        reading_count=hourly.count(),
    )


@model(
    name="sensor_anomalies",
    materialise="none",
    description="Detect and log anomalous sensor readings",
    tags=["iot", "alerting"],
)
def sensor_anomalies(sensor_readings: ibis.Table) -> None:
    """Detect anomalous readings: temp > 45, temp < -20, or humidity > 95.

    This model materialises as 'none' — it runs as a side-effect only,
    logging each anomaly it finds. Useful for alerting pipelines.
    """
    anomalies = sensor_readings.filter(
        (sensor_readings.temperature > 45)
        | (sensor_readings.temperature < -20)
        | (sensor_readings.humidity > 95)
    )
    df = anomalies.execute()
    for _, row in df.iterrows():
        logger.warning(
            f"ANOMALY: sensor={row['sensor_id']} "
            f"temp={row['temperature']}C humidity={row['humidity']}%"
        )
    return None


@model(
    name="sensor_dashboard",
    materialise="view",
    description="Latest reading per sensor — always-current dashboard view",
    tags=["iot", "dashboard"],
)
def sensor_dashboard(sensor_readings: ibis.Table) -> ibis.Table:
    """Get the most recent reading for each sensor.

    Uses a window function to rank readings by timestamp within each
    sensor_id, then filters to the latest row. Materialised as a view
    so queries always reflect the current state.
    """
    ranked = sensor_readings.mutate(
        row_num=ibis.row_number().over(
            ibis.window(group_by="sensor_id", order_by=ibis.desc("timestamp"))
        )
    )
    latest = ranked.filter(ranked.row_num == 0)
    return latest.select("sensor_id", "temperature", "humidity", "timestamp")
