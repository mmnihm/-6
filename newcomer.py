"""How long someone still counts as a newcomer after joining the member group."""

from datetime import datetime, timezone

DEFAULT_HOURS = 24
MAX_HOURS = 168


def clamp_hours(hours):
    """Keep a configured window inside a sane range."""
    try:
        value = float(hours)
    except (TypeError, ValueError):
        return float(DEFAULT_HOURS)
    return max(0.0, min(float(MAX_HOURS), value))


def window_active(joined_at_iso, now, hours=DEFAULT_HOURS):
    """True while the join timestamp is inside the newcomer window."""
    hours = clamp_hours(hours)
    if not joined_at_iso or hours <= 0:
        return False
    try:
        joined = datetime.fromisoformat(str(joined_at_iso))
    except (TypeError, ValueError):
        return False
    if joined.tzinfo is None:
        joined = joined.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - joined).total_seconds() < hours * 3600
