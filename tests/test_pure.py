import io
import unittest

from aiohttp.test_utils import TestClient, TestServer

import web_catalog
from alerts import AlertThrottle
from captcha_image import CODE_LENGTH, OPTION_COUNT, build_captcha


class CaptchaImageTests(unittest.TestCase):
    def test_options_are_six_unique_four_digit_codes(self):
        for _ in range(20):
            image, options, answer_index = build_captcha()
            self.assertEqual(len(options), OPTION_COUNT)
            self.assertEqual(len(set(options)), OPTION_COUNT)
            for option in options:
                self.assertEqual(len(option), CODE_LENGTH)
                self.assertTrue(option.isdigit(), option)
            self.assertTrue(0 <= answer_index < OPTION_COUNT)

    def test_image_is_a_readable_png(self):
        image, _, _ = build_captcha()
        self.assertIsInstance(image, io.BytesIO)
        self.assertEqual(image.getvalue()[:8], b"\x89PNG\r\n\x1a\n")


class CatalogDateTests(unittest.TestCase):
    def test_utc_timestamp_is_shifted_to_beijing_time(self):
        # 2026-01-01 15:30 UTC is 2026-01-01 23:30 in Beijing.
        self.assertEqual(
            web_catalog.format_catalog_date("2026-01-01T15:30:00+00:00"),
            "2026-01-01",
        )
        # 2026-01-01 17:00 UTC is already the next day in Beijing.
        self.assertEqual(
            web_catalog.format_catalog_date("2026-01-01T17:00:00+00:00"),
            "2026-01-02",
        )

    def test_naive_timestamp_is_treated_as_utc(self):
        self.assertEqual(
            web_catalog.format_catalog_date("2026-01-01T17:00:00"),
            "2026-01-02",
        )

    def test_unparseable_value_falls_back_to_first_ten_characters(self):
        self.assertEqual(web_catalog.format_catalog_date("2026/01/01 junk"), "2026/01/01")
        self.assertEqual(web_catalog.format_catalog_date(""), "")


class FakeCatalogDatabase:
    def __init__(self):
        self.calls = 0
        self.healthy = True

    async def ping(self):
        return self.healthy

    async def public_catalog(self, query="", category="", limit=30, offset=0):
        self.calls += 1
        rows = [{
            "file_key": "key1",
            "file_name": "示例.jpg",
            "media_type": "photo",
            "category": "图片",
            "created_at": "2026-01-01T17:00:00+00:00",
        }]
        return rows, ["图片"], 1


class CatalogCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        web_catalog._page_cache.clear()
        web_catalog._rate_buckets.clear()
        web_catalog._health.update({"checked_at": 0.0, "ok": True})
        self._saved_max = web_catalog.RATE_LIMIT_MAX
        self.database = FakeCatalogDatabase()
        app = web_catalog.create_catalog_app(self.database, lambda: "testbot")
        self.server = TestServer(app)
        self.client = TestClient(self.server)
        await self.client.start_server()

    async def asyncTearDown(self):
        web_catalog.RATE_LIMIT_MAX = self._saved_max
        await self.client.close()
        web_catalog._page_cache.clear()
        web_catalog._rate_buckets.clear()

    async def test_repeat_request_is_served_from_cache(self):
        first = await self.client.get("/catalog")
        self.assertEqual(first.status, 200)
        self.assertIn("max-age=", first.headers.get("Cache-Control", ""))
        etag = first.headers.get("ETag")
        self.assertTrue(etag)

        second = await self.client.get("/catalog")
        self.assertEqual(second.status, 200)
        self.assertEqual(self.database.calls, 1)

    async def test_matching_etag_returns_304(self):
        first = await self.client.get("/catalog")
        etag = first.headers["ETag"]
        revalidated = await self.client.get("/catalog", headers={"If-None-Match": etag})
        self.assertEqual(revalidated.status, 304)
        self.assertEqual(self.database.calls, 1)

    async def test_different_query_bypasses_cache(self):
        await self.client.get("/catalog")
        await self.client.get("/catalog?q=猫")
        self.assertEqual(self.database.calls, 2)

    async def test_flood_gets_429_with_retry_after(self):
        web_catalog.RATE_LIMIT_MAX = 2
        for _ in range(2):
            self.assertEqual((await self.client.get("/catalog")).status, 200)
        blocked = await self.client.get("/catalog")
        self.assertEqual(blocked.status, 429)
        self.assertTrue(blocked.headers.get("Retry-After"))

    async def test_health_reports_failure_when_database_is_down(self):
        healthy = await self.client.get("/healthz")
        self.assertEqual(healthy.status, 200)

        self.database.healthy = False
        web_catalog._health.update({"checked_at": 0.0, "ok": True})
        degraded = await self.client.get("/healthz")
        self.assertEqual(degraded.status, 503)


if __name__ == "__main__":
    unittest.main()


class AlertThrottleTests(unittest.TestCase):
    def test_same_key_is_suppressed_inside_cooldown(self):
        throttle = AlertThrottle(cooldown_seconds=900)
        self.assertTrue(throttle.allow("task:backup", 1000.0))
        self.assertFalse(throttle.allow("task:backup", 1001.0))
        self.assertTrue(throttle.allow("task:backup", 1901.0))

    def test_different_keys_are_independent(self):
        throttle = AlertThrottle(cooldown_seconds=900)
        self.assertTrue(throttle.allow("task:a", 1000.0))
        self.assertTrue(throttle.allow("task:b", 1001.0))

    def test_hourly_cap_stops_a_storm(self):
        throttle = AlertThrottle(cooldown_seconds=1, hourly_limit=3)
        for index in range(3):
            self.assertTrue(throttle.allow(f"key{index}", 1000.0 + index))
        self.assertFalse(throttle.allow("key3", 1005.0))
        # An hour later the window has slid past the earlier alerts.
        self.assertTrue(throttle.allow("key3", 5000.0))
