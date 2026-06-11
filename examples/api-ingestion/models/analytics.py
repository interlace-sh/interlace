"""Analytics models — aggregate and analyse weather data.

Demonstrates: dependency injection via function parameters, ibis aggregations,
and the replace strategy for computed tables.
"""

import ibis

from interlace import model


@model(
    name="temperature_analysis",
    strategy="replace",
    description="Daily temperature statistics derived from the 7-day forecast",
    tags=["analytics"],
)
def temperature_analysis(weather_forecast: ibis.Table) -> ibis.Table:
    """Compute min, max, and average temperatures per forecast day.

    Receives the ``weather_forecast`` table automatically via dependency
    injection. The ``replace`` strategy ensures the analysis table always
    reflects the latest forecast data.
    """
    return weather_forecast.select(
        weather_forecast.date,
        temp_min_c=weather_forecast.temp_min_c,
        temp_max_c=weather_forecast.temp_max_c,
        temp_avg_c=((weather_forecast.temp_max_c + weather_forecast.temp_min_c) / 2).round(1),
        temp_range_c=(weather_forecast.temp_max_c - weather_forecast.temp_min_c).round(1),
        precip_prob_pct=weather_forecast.precip_prob_pct,
        wind_max_kmh=weather_forecast.wind_max_kmh,
    )
