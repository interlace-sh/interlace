"""Stream definitions — IoT sensor data ingestion endpoints."""

from interlace import stream


@stream(
    name="sensor_readings",
    fields={
        "sensor_id": "string",
        "temperature": "float64",
        "humidity": "float64",
        "timestamp": "string",
    },
    validate_schema=True,
    retention={"max_age_days": 30},
    description="Temperature and humidity readings from IoT sensors",
    tags=["iot", "sensors"],
)
def sensor_readings():
    """Stream endpoint for IoT sensor data."""
    pass


@stream(
    name="system_events",
    fields={
        "event_type": "string",
        "sensor_id": "string",
        "message": "string",
        "severity": "string",
        "timestamp": "string",
    },
    auth={"type": "api_key", "header": "X-API-Key"},
    rate_limit={"requests_per_second": 100, "burst": 200},
    description="System diagnostic events",
    tags=["iot", "diagnostics"],
)
def system_events():
    """Stream endpoint for system events. Requires API key auth."""
    pass
