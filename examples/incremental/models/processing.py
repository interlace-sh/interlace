"""Processing models — enrich, aggregate, and react to data.

Demonstrates: incremental config (key-based), cursor-based append,
side-effect models with materialise="none".
"""

import ibis

from interlace import get_logger, model

logger = get_logger("incremental.processing")


@model(
    name="event_enrichment",
    incremental={"type": "key", "key_column": "event_id"},
    description="Events enriched with feature flag status, processed incrementally by event_id",
    tags=["processing"],
)
def event_enrichment(user_events: ibis.Table, feature_flags: ibis.Table) -> ibis.Table:
    """Join events with feature flags. Only new event_ids are processed on repeat runs."""
    # Map event features to the closest matching flag feature
    # feature_flags tracks features like 'new_dashboard' -> we join on a shared key
    flags = feature_flags.select(
        flag_feature=feature_flags.feature,
        flag_enabled=feature_flags.enabled,
        rollout_pct=feature_flags.rollout_pct,
    )

    return user_events.cross_join(flags).filter(
        # Match dashboard -> new_dashboard, reports -> advanced_reports, etc.
        # For simplicity, just carry all flags — real pipelines would have a proper mapping
        ibis.literal(True)
    ).select(
        user_events.event_id,
        user_events.user_id,
        user_events.event_type,
        user_events.feature,
        user_events.event_date,
        flags.flag_feature,
        flags.flag_enabled,
        flags.rollout_pct,
    )


@model(
    name="daily_active_users",
    cursor="event_date",
    strategy="append",
    description="Daily active user counts, appended incrementally by event_date cursor",
    tags=["processing", "metrics"],
)
def daily_active_users(user_events: ibis.Table) -> ibis.Table:
    """Count distinct active users per day. Cursor ensures only new dates are processed."""
    return user_events.group_by("event_date").agg(
        active_users=user_events.user_id.nunique(),
        total_events=user_events.event_id.count(),
    )


@model(
    name="usage_notifications",
    materialise="none",
    cursor="event_id",
    dependencies=["user_events"],
    description="Side-effect model: logs notifications for purchase events, no table output",
    tags=["processing", "side-effect"],
)
def usage_notifications(user_events: ibis.Table) -> None:
    """Filter purchase events and log notifications. Returns None — no materialisation."""
    # Execute the query to get purchase events
    purchases = user_events.filter(user_events.event_type == "purchase")
    result = purchases.execute()

    for _, row in result.iterrows():
        logger.info(
            "Purchase notification: user=%s feature=%s date=%s",
            row["user_id"],
            row["feature"],
            row["event_date"],
        )

    logger.info("Sent %d purchase notifications", len(result))
    return None
