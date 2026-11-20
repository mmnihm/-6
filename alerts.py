"""Throttling for operator alerts.

Kept apart from the bot so the rules can be tested without Telegram or a database.
"""


class AlertThrottle:
    """Allow at most one alert per key per cooldown, and a global hourly cap."""

    def __init__(self, cooldown_seconds=900, hourly_limit=6):
        self.cooldown_seconds = cooldown_seconds
        self.hourly_limit = hourly_limit
        self._last_per_key = {}
        self._recent = []

    def allow(self, key, now):
        last = self._last_per_key.get(key)
        if last is not None and now - last < self.cooldown_seconds:
            return False
        self._recent = [at for at in self._recent if now - at < 3600]
        if len(self._recent) >= self.hourly_limit:
            return False
        self._last_per_key[key] = now
        self._recent.append(now)
        return True

    def reset(self):
        self._last_per_key.clear()
        self._recent.clear()
