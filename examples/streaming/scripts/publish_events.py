#!/usr/bin/env python3
"""Simulate IoT sensor data by publishing to the sensor_readings stream."""

import random
from datetime import datetime

from interlace import publish_sync
from models.streams import sensor_readings

SENSORS = ["sensor-001", "sensor-002", "sensor-003", "sensor-004", "sensor-005"]


def generate_reading(sensor_id):
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(15.0, 35.0), 1),
        "humidity": round(random.uniform(30.0, 80.0), 1),
        "timestamp": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    for _ in range(20):
        sensor = random.choice(SENSORS)
        reading = generate_reading(sensor)
        result = publish_sync(sensor_readings, reading)
        print(f"Published: {reading['sensor_id']} temp={reading['temperature']}C humidity={reading['humidity']}%")

    print(f"\nPublished 20 readings. Run 'interlace run' to process them.")
