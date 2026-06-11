"""Output models — scheduled aggregations and exports.

Demonstrates: scheduled execution (cron), Parquet export.
"""

import ibis

from interlace import model


@model(
    name="weekly_report",
    schedule={"cron": "0 9 * * 1"},
    export={"format": "parquet", "path": "output/weekly.parquet"},
    description="Weekly event aggregation by feature, exported to Parquet every Monday at 9am",
    tags=["output", "scheduled"],
)
def weekly_report(user_events: ibis.Table) -> ibis.Table:
    """Aggregate events by week and feature. Scheduled weekly, exported to Parquet."""
    return (
        user_events.mutate(
            week=user_events.event_date.cast("string").substr(0, 7),
        )
        .group_by(["week", "feature"])
        .agg(
            event_count=user_events.event_id.count(),
            unique_users=user_events.user_id.nunique(),
            logins=user_events.event_type.cases(
                (ibis.literal("login"), ibis.literal(1)),
                else_=ibis.literal(0),
            ).sum(),
            purchases=user_events.event_type.cases(
                (ibis.literal("purchase"), ibis.literal(1)),
                else_=ibis.literal(0),
            ).sum(),
        )
    )
