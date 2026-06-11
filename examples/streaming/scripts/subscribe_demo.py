#!/usr/bin/env python3
"""Subscribe to sensor_readings and filter for high temperatures."""

import asyncio

from interlace import subscribe


async def main():
    print("Listening for high-temperature readings (> 30C)...")
    print("Press Ctrl+C to stop.\n")

    async for event in subscribe(
        "sensor_readings",
        batch_size=1,
        timeout=30.0,
        filter_fn=lambda e: e.get("temperature", 0) > 30.0,
    ):
        print(f"ALERT: {event['sensor_id']} — {event['temperature']}C at {event['timestamp']}")


if __name__ == "__main__":
    asyncio.run(main())
