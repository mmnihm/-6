import asyncio
import os
import tempfile
import unittest
import faulthandler

faulthandler.dump_traceback_later(30, repeat=True)

import database
from database import Database, PostgresCompat


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        # Count assertions here expect an immediate recount; the production
        # throttle is exercised in its own test below.
        self._saved_interval = database.CATEGORY_STATS_MIN_INTERVAL
        database.CATEGORY_STATS_MIN_INTERVAL = 0.0
        self.db = Database(os.path.join(self.temp_dir.name, "test.db"))
        await self.db.initialize()

    async def asyncTearDown(self):
        for task in list(self.db._category_stats_pending.values()):
            task.cancel()
        database.CATEGORY_STATS_MIN_INTERVAL = self._saved_interval
        self.temp_dir.cleanup()

    async def test_postgres_sql_adapter_translates_sqlite_syntax(self):
        sql = PostgresCompat.adapt_sql(
            "INSERT OR IGNORE INTO files(file_key) VALUES(?)"
        )
        self.assertEqual(
            sql,
            "INSERT INTO files(file_key) VALUES(%s) ON CONFLICT DO NOTHING",
        )
        search_sql = PostgresCompat.adapt_sql(
            "SELECT * FROM files WHERE file_name LIKE ? ORDER BY category COLLATE NOCASE"
        )
        self.assertIn("file_name ILIKE %s", search_sql)
        self.assertIn("ORDER BY lower(category)", search_sql)

    async def test_postgres_adapter_preserves_literal_percent_without_params(self):
        class FakeConnection:
            def __init__(self):
                self.calls = []

            async def execute(self, *args):
                self.calls.append(args)
                return "ok"

        connection = FakeConnection()
        adapter = PostgresCompat(connection)
        await adapter.execute(
            "SELECT * FROM files WHERE lower(file_name) LIKE '%.jpg'"
        )
        self.assertEqual(len(connection.calls), 1)
        self.assertEqual(len(connection.calls[0]), 1)
        self.assertIn("LIKE '%.jpg'", connection.calls[0][0])

        await adapter.execute("SELECT * FROM files WHERE file_key=?", ("abc",))
        self.assertEqual(connection.calls[1][1], ("abc",))

    async def test_only_one_first_admin_can_claim(self):
        self.assertTrue(await self.db.claim_first_admin(101))
        self.assertFalse(await self.db.claim_first_admin(202))
        self.assertEqual(await self.db.get_admin_id(), 101)

    async def test_manual_membership_and_revocation(self):
        await self.db.grant_member(303, None, 101)
        self.assertTrue(await self.db.has_manual_membership(303))
        await self.db.revoke_member(303)
        self.assertFalse(await self.db.has_manual_membership(303))

    async def test_file_insert_and_category_update(self):
        await self.db.add_file("abc123", 101, "sample.mp4", "video", 11, 22)
        row = await self.db.get_file("abc123")
        self.assertEqual(row["primary_message_id"], 11)
        self.assertEqual(row["backup_message_id"], 22)
        self.assertTrue(await self.db.set_file_category("abc123", "电影"))
        row = await self.db.get_file("abc123")
        self.assertEqual(row["category"], "电影")

    async def test_duplicate_source_fingerprint_is_ignored(self):
        first = await self.db.add_file(
            "firstkey", 101, "sample.mp4", "video", 11, 22,
            "电影", "telegram-file-unique-id"
        )
        second = await self.db.add_file(
            "secondkey", 101, "sample-copy.mp4", "video", 12, 23,
            "电影", "telegram-file-unique-id"
        )
        self.assertTrue(first)
        self.assertFalse(second)
        row = await self.db.get_file_by_fingerprint("telegram-file-unique-id")
        self.assertEqual(row["file_key"], "firstkey")

    async def test_history_scan_pairs_primary_and_backup_messages(self):
        created = "2026-10-30T10:00:00+00:00"
        matched, inserted = await self.db.save_scanned_message(
            100, 200, 100, 501, "same-file-unique-id",
            "movie.mp4", "video", "电影", created, 101, "scan-key-1"
        )
        self.assertFalse(matched)
        self.assertTrue(inserted)

        row = await self.db.get_file_by_fingerprint("same-file-unique-id")
        self.assertEqual(row["primary_message_id"], 501)
        self.assertEqual(row["backup_message_id"], 0)

        matched, inserted = await self.db.save_scanned_message(
            200, 100, 100, 801, "same-file-unique-id",
            "movie-copy.mp4", "video", "视频", created, 101, "scan-key-2"
        )
        self.assertTrue(matched)
        self.assertFalse(inserted)

        row = await self.db.get_file_by_fingerprint("same-file-unique-id")
        self.assertEqual(row["primary_message_id"], 501)
        self.assertEqual(row["backup_message_id"], 801)
        self.assertEqual(row["file_name"], "movie.mp4")
        self.assertEqual(row["category"], "电影")

        progress = await self.db.get_scan_progress(100)
        self.assertEqual(progress["last_message_id"], 501)
        await self.db.mark_scan_completed(100)
        self.assertEqual((await self.db.get_scan_progress(100))["completed"], 1)
        scanned_folder = next(
            row for row in await self.db.list_categories()
            if row["category"] == "电影"
        )
        self.assertEqual(scanned_folder["file_count"], 1)

    async def test_primary_only_scan_is_published_without_backup(self):
        created = "2026-10-30T10:00:00+00:00"
        matched, inserted = await self.db.save_scanned_message(
            100, 200, 100, 501, "primary-only-id",
            "primary.mp4", "video", "视频", created, 101, "scan-key-primary"
        )
        self.assertFalse(matched)
        self.assertTrue(inserted)
        row = await self.db.get_file_by_fingerprint("primary-only-id")
        self.assertEqual(row["primary_message_id"], 501)
        self.assertEqual(row["backup_message_id"], 0)
        rows, _, total = await self.db.public_catalog()
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["file_name"], "primary.mp4")

    async def test_primary_catalog_backfills_legacy_scan_records(self):
        created = "2026-10-30T10:00:00+00:00"
        async with self.db.connect() as connection:
            await connection.execute("""
                INSERT INTO scanner_messages(
                    channel_id, message_id, fingerprint, file_name,
                    media_type, category, created_at
                ) VALUES(100, 601, 'legacy-primary-id', 'legacy.mp4',
                         'video', '视频', ?)
            """, (created,))
            await connection.commit()
        self.assertEqual(await self.db.publish_primary_catalog(100, 101), 1)
        self.assertEqual(await self.db.publish_primary_catalog(100, 101), 0)
        row = await self.db.get_file_by_fingerprint("legacy-primary-id")
        self.assertEqual(row["primary_message_id"], 601)
        self.assertEqual(row["backup_message_id"], 0)
    async def test_folder_delivery_progress_is_persistent_and_monotonic(self):
        self.assertEqual(
            await self.db.get_category_delivery_progress(303, "folder-key"),
            0,
        )
        await self.db.set_category_delivery_progress(303, "folder-key", 20)
        self.assertEqual(
            await self.db.get_category_delivery_progress(303, "folder-key"),
            20,
        )
        # A stale callback must not move the saved cursor backwards.
        await self.db.set_category_delivery_progress(303, "folder-key", 10)
        self.assertEqual(
            await self.db.get_category_delivery_progress(303, "folder-key"),
            20,
        )
        # Progress is isolated per user and folder.
        self.assertEqual(
            await self.db.get_category_delivery_progress(404, "folder-key"),
            0,
        )
        self.assertEqual(
            await self.db.get_category_delivery_progress(303, "other-folder"),
            0,
        )

    async def test_empty_categories_are_visible_to_members(self):
        await self.db.save_category("空目录")
        await self.db.save_category("有资源的目录")
        categories = await self.db.list_categories()
        self.assertEqual(
            [row["category"] for row in categories],
            ["有资源的目录", "空目录"],
        )
        empty = next(row for row in categories if row["category"] == "空目录")
        self.assertEqual(empty["file_count"], 0)
        self.assertTrue(empty["share_key"])

        await self.db.add_file(
            "visible-folder-file", 101, "sample.mp4", "video", 11, 22,
            "有资源的目录"
        )
        categories = await self.db.list_categories()
        self.assertEqual(
            [row["category"] for row in categories],
            ["有资源的目录", "空目录"],
        )

    async def test_cleanup_empty_categories_removes_related_user_state(self):
        await self.db.save_category("待清理空目录")
        folder = next(
            row for row in await self.db.list_categories()
            if row["category"] == "待清理空目录"
        )
        share_key = folder["share_key"]
        async with self.db.connect() as connection:
            await connection.execute(
                "INSERT INTO category_favorites(user_id, share_key, created_at) "
                "VALUES(303, ?, ?)",
                (share_key, "2026-10-31T00:00:00+00:00"),
            )
            await connection.execute(
                "INSERT INTO category_delivery_progress(user_id, share_key, next_offset, updated_at) "
                "VALUES(303, ?, 20, ?)",
                (share_key, "2026-10-31T00:00:00+00:00"),
            )
            await connection.commit()

        self.assertEqual(await self.db.cleanup_empty_categories(), 1)
        self.assertEqual(await self.db.cleanup_empty_categories(), 0)
        async with self.db.connect() as connection:
            favorites = await (await connection.execute(
                "SELECT COUNT(*) AS count FROM category_favorites WHERE share_key=?",
                (share_key,),
            )).fetchone()
            progress = await (await connection.execute(
                "SELECT COUNT(*) AS count FROM category_delivery_progress WHERE share_key=?",
                (share_key,),
            )).fetchone()
        self.assertEqual(favorites["count"], 0)
        self.assertEqual(progress["count"], 0)
        self.assertNotIn(
            "待清理空目录",
            [row["category"] for row in await self.db.list_categories()],
        )

    async def test_folder_media_stats_include_document_backed_media(self):
        samples = [
            ("stat-photo", "photo.jpg", "document"),
            ("stat-video", "video.mp4", "document"),
            ("stat-animation", "loop.gif", "animation"),
            ("stat-audio", "track.mp3", "document"),
            ("stat-pdf", "guide.pdf", "document"),
            ("stat-native-video", "native-video", "video"),
            ("stat-native-photo", "native-photo", "photo"),
        ]
        for index, (key, name, media_type) in enumerate(samples, start=1):
            await self.db.add_file(
                key, 101, name, media_type, index, index + 100, "统计测试"
            )

        folder = next(
            row for row in await self.db.list_categories()
            if row["category"] == "统计测试"
        )
        self.assertEqual(folder["file_count"], 7)
        self.assertEqual(folder["image_count"], 2)
        self.assertEqual(folder["video_count"], 2)
        self.assertEqual(folder["animation_count"], 1)
        self.assertEqual(folder["audio_count"], 1)
        self.assertEqual(folder["document_count"], 1)

        stats = await self.db.catalog_stats()
        self.assertEqual(stats["images"], 2)
        self.assertEqual(stats["videos"], 2)
        self.assertEqual(stats["animations"], 1)
        self.assertEqual(stats["audios"], 1)

    async def test_bulk_folder_stats_refresh_after_import(self):
        await self.db.add_file(
            "batch-import-1", 101, "one.mp4", "video", 11, 22,
            "批量导入", source_fingerprint="batch-fingerprint-1",
            file_size=100, refresh_stats=False
        )
        await self.db.add_file(
            "batch-import-2", 101, "two.mp4", "video", 12, 23,
            "批量导入", source_fingerprint="batch-fingerprint-2",
            file_size=200, refresh_stats=False
        )
        before = next(
            row for row in await self.db.list_categories()
            if row["category"] == "批量导入"
        )
        self.assertEqual(before["file_count"], 0)

        await self.db.refresh_category_stats({"批量导入"})
        after = next(
            row for row in await self.db.list_categories()
            if row["category"] == "批量导入"
        )
        self.assertEqual(after["file_count"], 2)
        self.assertEqual(after["video_count"], 2)
        stats = await self.db.get_category_storage_stats("批量导入")
        self.assertEqual(stats["video_size"], 300)

    async def test_folder_size_stats_are_cached_and_refreshed_on_insert(self):
        await self.db.add_file(
            "cached-video", 101, "movie.mp4", "video", 11, 22,
            "缓存统计", file_size=1024
        )
        await self.db.add_file(
            "cached-photo", 101, "cover.jpg", "photo", 12, 23,
            "缓存统计", file_size=512
        )

        stats = await self.db.get_category_storage_stats("缓存统计")
        self.assertEqual(stats["video_size"], 1024)
        self.assertEqual(stats["image_size"], 512)
        self.assertEqual(stats["other_size"], 0)
        self.assertEqual(stats["sized_count"], 2)
        self.assertEqual(stats["file_count"], 2)

        folder = next(
            row for row in await self.db.list_categories()
            if row["category"] == "缓存统计"
        )
        shared = await self.db.get_category_by_share_key(folder["share_key"])
        self.assertEqual(shared["file_count"], 2)

        await self.db.add_file(
            "cached-document", 101, "guide.pdf", "document", 13, 24,
            "缓存统计", file_size=256
        )
        updated = await self.db.get_category_storage_stats("缓存统计")
        self.assertEqual(updated["other_size"], 256)
        self.assertEqual(updated["file_count"], 3)

    async def test_rename_category_keeps_files_and_empty_delete_is_safe(self):
        await self.db.save_category("待改名")
        await self.db.add_file(
            "rename-folder-file", 101, "sample.mp4", "video", 11, 22,
            "待改名"
        )
        self.assertTrue(await self.db.rename_category("待改名", "新目录"))
        record = await self.db.get_file("rename-folder-file")
        self.assertEqual(record["category"], "新目录")
        self.assertFalse(await self.db.delete_empty_category("新目录"))

        await self.db.save_category("待删除空目录")
        self.assertTrue(await self.db.delete_empty_category("待删除空目录"))
        self.assertFalse(await self.db.delete_empty_category("待删除空目录"))

    async def test_bulk_file_lookup_and_history(self):
        await self.db.add_file("bulk001", 101, "one.mp4", "video", 11, 22, "电影")
        await self.db.add_file("bulk002", 101, "two.mp4", "video", 12, 23, "电影")
        rows = await self.db.get_files_by_keys(["bulk001", "bulk002", "bulk001", "missing"])
        self.assertEqual({row["file_key"] for row in rows}, {"bulk001", "bulk002"})
        await self.db.record_history_batch(303, ["bulk001", "bulk002", "bulk001"])
        history = await self.db.list_user_history(303, 10)
        self.assertEqual({row["file_key"] for row in history}, {"bulk001", "bulk002"})

    async def test_catalog_stats_and_member_history(self):
        await self.db.add_file('history001', 101, 'movie.mp4', 'video', 11, 22, '电影')
        stats = await self.db.catalog_stats()
        self.assertEqual(stats['messages'], 1)
        self.assertEqual(stats['videos'], 1)
        await self.db.record_history(303, 'history001')
        rows = await self.db.list_user_history(303)
        self.assertEqual(rows[0]['file_key'], 'history001')
        self.assertTrue(await self.db.toggle_favorite(303, 'history001'))
        self.assertTrue(await self.db.is_favorite(303, 'history001'))

    async def test_public_catalog_search_and_category_filter(self):
        await self.db.add_file("key001", 101, "movie-one.mp4", "video", 11, 22, "电影")
        await self.db.add_file("key002", 101, "lesson.pdf", "document", 12, 23, "教程")

        rows, categories, total = await self.db.public_catalog(query="movie")
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["file_name"], "movie-one.mp4")
        self.assertEqual(set(rows[0].keys()), {
            "file_key", "file_name", "media_type", "category", "created_at",
            "album_group_id", "album_position", "album_size",
        })
        self.assertEqual(int(rows[0]["album_size"]), 1)
        self.assertEqual(set(categories), {"电影", "教程"})

        rows, _, total = await self.db.public_catalog(category="教程")
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["file_name"], "lesson.pdf")

    async def test_ban_and_user_tracking(self):
        await self.db.touch_user(404)
        self.assertFalse(await self.db.is_banned(404))
        await self.db.ban_user(404, "test", 101)
        self.assertTrue(await self.db.is_banned(404))
        await self.db.unban_user(404)
        self.assertFalse(await self.db.is_banned(404))
        self.assertEqual(await self.db.list_user_ids(), [404])

    async def test_album_aggregate_and_non_album_size(self):
        for pos in range(3):
            await self.db.add_file(
                f"alb{pos}", 101, f"p{pos}.jpg", "photo", 10 + pos, 20 + pos,
                "相册", album_group_id="group-1", album_position=pos,
            )
        await self.db.add_file("solo1", 101, "solo.jpg", "photo", 30, 31, "相册")

        rows, _, _ = await self.db.public_catalog(category="相册", limit=10)
        sizes = {row["file_key"]: int(row["album_size"]) for row in rows}
        self.assertEqual(sizes["alb0"], 3)
        self.assertEqual(sizes["alb2"], 3)
        # A file outside an album is its own unit, not the whole table's NULL count.
        self.assertEqual(sizes["solo1"], 1)

        files = await self.db.list_category_files("相册", limit=10)
        album_sizes = {int(row["album_size"]) for row in files if row["album_group_id"]}
        self.assertEqual(album_sizes, {3})

        expanded = await self.db.complete_album_groups(rows)
        self.assertEqual(
            {row["file_key"] for row in expanded}, {"alb0", "alb1", "alb2", "solo1"}
        )

    async def test_catalog_stats_cache_invalidated_on_write(self):
        await self.db.add_file("cache1", 101, "a.mp4", "video", 1, 2, "电影")
        self.assertEqual((await self.db.catalog_stats())["messages"], 1)
        self.assertEqual((await self.db.catalog_stats())["messages"], 1)
        await self.db.add_file("cache2", 101, "b.mp4", "video", 3, 4, "电影")
        self.assertEqual((await self.db.catalog_stats())["messages"], 2)


    async def _category_file_count(self, name="电影"):
        async with self.db.connect() as db:
            row = await (await db.execute(
                "SELECT file_count FROM categories WHERE name=?", (name,)
            )).fetchone()
            return int(row["file_count"] or 0) if row else 0

    async def test_folder_recount_is_throttled_then_catches_up(self):
        database.CATEGORY_STATS_MIN_INTERVAL = 0.4
        await self.db.add_file("t1", 101, "a.mp4", "video", 11, 22, "电影")
        self.assertEqual(await self._category_file_count(), 1)

        # Second write inside the window coalesces instead of rescanning the folder.
        await self.db.add_file("t2", 101, "b.mp4", "video", 12, 23, "电影")
        self.assertEqual(await self._category_file_count(), 1)

        # The deferred catch-up recount settles the count once the window closes.
        await asyncio.sleep(1.0)
        self.assertEqual(await self._category_file_count(), 2)

    async def test_forced_refresh_ignores_throttle_window(self):
        database.CATEGORY_STATS_MIN_INTERVAL = 60.0
        await self.db.add_file("t1", 101, "a.mp4", "video", 11, 22, "电影")
        await self.db.add_file("t2", 101, "b.mp4", "video", 12, 23, "电影")
        self.assertEqual(await self._category_file_count(), 1)

        # Recategorising a file is a structural edit: counts update immediately.
        await self.db.set_file_category("t2", "教程")
        self.assertEqual(await self._category_file_count(), 1)
        self.assertEqual(await self._category_file_count("教程"), 1)
        await self.db.refresh_category_stats(["电影"])
        self.assertEqual(await self._category_file_count(), 1)

    async def test_catalog_total_matches_filtered_pages(self):
        await self.db.add_file("t1", 101, "movie.mp4", "video", 11, 22, "电影")
        await self.db.add_file("t2", 101, "lesson.pdf", "document", 12, 23, "教程")
        self.assertEqual(await self.db.catalog_total(), 2)
        self.assertEqual(await self.db.catalog_total(category="教程"), 1)
        self.assertEqual(await self.db.catalog_total(query="movie"), 1)
        _, _, page_total = await self.db.public_catalog(category="教程")
        self.assertEqual(page_total, 1)

    async def test_scan_does_not_reindex_uploaded_copy(self):
        """A history scan must not add a second row for the bot's own upload copy."""
        now = "2026-11-11T00:00:00+00:00"
        primary_channel_id = 555
        for fingerprint in ("AQADfileunique", "upload:42:77"):
            self.db = Database(os.path.join(self.temp_dir.name, f"scan_{fingerprint[:4]}.db"))
            await self.db.initialize()
            await self.db.add_file(
                "uploadkey", 101, "a.mp4", "video", 1000, 2000, "电影",
                source_fingerprint=fingerprint, file_size=100, created_at=now,
            )
            await self.db.save_scanned_message(
                primary_channel_id, None, primary_channel_id, 1000,
                f"cangku:{primary_channel_id}:1000", "a.mp4", "video", "",
                now, 1, "scanrow",
                album_group_id=f"cangku-single:{primary_channel_id}:1000",
                album_position=0,
            )
            rows = await self.db.list_files(50)
            self.assertEqual([row["file_key"] for row in rows], ["uploadkey"])
            _, _, total = await self.db.public_catalog(limit=50)
            self.assertEqual(total, 1)

    async def test_scan_still_indexes_unseen_channel_message(self):
        """Guarding against re-indexing must not drop genuine history files."""
        now = "2026-11-11T00:00:00+00:00"
        await self.db.save_scanned_message(
            555, None, 555, 4321, "cangku:555:4321", "old.mp4", "video", "电影",
            now, 1, "legacyrow", album_group_id="cangku-single:555:4321",
            album_position=0,
        )
        rows = await self.db.list_files(50)
        self.assertEqual([row["file_key"] for row in rows], ["legacyrow"])

    async def test_delivery_failure_attempts_accumulate_per_user_and_unit(self):
        """A unit that keeps failing must be countable, and clearable on success."""
        self.assertEqual(
            await self.db.record_delivery_failure(7, ["broken"], "member:movies", "boom"),
            1,
        )
        self.assertEqual(
            await self.db.record_delivery_failure(7, ["broken"], "member:movies", "boom"),
            2,
        )
        self.assertEqual(
            await self.db.record_delivery_failure(7, ["broken"], "member:movies", "boom"),
            3,
        )
        # Counting is per user: another member still gets a fresh budget.
        self.assertEqual(
            await self.db.record_delivery_failure(8, ["broken"], "member:movies", "boom"),
            1,
        )
        await self.db.clear_delivery_failures(7, ["broken"], "member:movies")
        self.assertEqual(
            await self.db.record_delivery_failure(7, ["broken"], "member:movies", "boom"),
            1,
        )

    async def test_album_failure_tracks_peak_across_members(self):
        """An album is one unit: the member with the most failures drives the skip."""
        await self.db.record_delivery_failure(9, ["a1"], "member:photos", "x")
        await self.db.record_delivery_failure(9, ["a1"], "member:photos", "x")
        peak = await self.db.record_delivery_failure(9, ["a1", "a2"], "member:photos", "x")
        self.assertEqual(peak, 3)
        rows = await self.db.list_delivery_failures(10)
        self.assertEqual({row["file_key"] for row in rows}, {"a1", "a2"})

    async def test_pending_deletions_roundtrip(self):
        """Scheduled deletions survive a restart because they live in the database."""
        now = "2026-11-13T00:00:00+00:00"
        await self.db.add_pending_deletions(12345, [11, 12], now)
        await self.db.add_pending_deletions(12345, [12, 13], now)  # duplicate ids are ignored
        rows = await self.db.list_pending_deletions(10)
        self.assertEqual(len(rows), 3)
        self.assertEqual({int(row["message_id"]) for row in rows}, {11, 12, 13})
        await self.db.remove_pending_deletions(12345, [11, 12])
        rows = await self.db.list_pending_deletions(10)
        self.assertEqual([int(row["message_id"]) for row in rows], [13])

    async def test_startup_name_cleanup_pages_through_every_row(self):
        """The chunked migration must not skip rows when the library is large."""
        now = "2026-11-13T00:00:00+00:00"
        total = 12
        for index in range(total):
            await self.db.add_file(
                f"k{index}", 1, f"电影{index} @ad_channel 加群领取.mkv", "video",
                100 + index, 0, "电影", f"cangku:1:{index}", created_at=now
            )
        # Re-run the startup migration with the one-time marker removed.
        async with self.db.connect() as conn:
            await conn.execute("DELETE FROM settings WHERE key='filename_ad_cleanup_v1'")
            await conn.commit()
        await self.db.initialize()
        rows = await self.db.list_files(50)
        self.assertEqual(len(rows), total)
        for row in rows:
            self.assertNotIn("@", row["file_name"])
            self.assertNotIn("加群", row["file_name"])


    async def test_media_kind_matches_the_old_extension_rules(self):
        """The stored classifier must agree with the LIKE rules it replaced."""
        cases = [
            ("photo", "IMG_0001.jpg", "image"),
            ("document", "电影.mp4", "video"),
            ("video", "clip.mkv", "video"),
            ("document", "动图.gif", "animation"),
            ("animation", "anim", "animation"),
            ("document", "音频.mp3", "audio"),
            ("audio", "track.flac", "audio"),
            ("voice", "voice.ogg", "audio"),
            ("document", "报告.pdf", "document"),
            ("document", "包.zip", "document"),
            ("document", "无扩展名", "document"),
            ("video_note", "round", "video"),
        ]
        for media_type, file_name, expected in cases:
            self.assertEqual(
                database.media_kind_of(media_type, file_name), expected,
                f"{media_type}/{file_name}",
            )

    async def test_folder_stats_count_by_stored_kind(self):
        """Image/video/audio/document counts come from media_kind, not name scans."""
        now = "2026-11-13T00:00:00+00:00"
        for index, (media_type, name) in enumerate([
            ("photo", "a.jpg"),
            ("document", "b.mp4"),
            ("document", "c.gif"),
            ("document", "d.mp3"),
            ("document", "e.pdf"),
        ]):
            await self.db.add_file(
                f"k{index}", 1, name, media_type, 100 + index, 0, "统计",
                f"cangou:1:{index}", created_at=now,
            )
        async with self.db.connect() as db:
            row = await (await db.execute(
                "SELECT image_count, video_count, animation_count, audio_count, "
                "document_count, file_count FROM categories WHERE name='统计'"
            )).fetchone()
        self.assertEqual(
            (int(row["image_count"]), int(row["video_count"]),
             int(row["animation_count"]), int(row["audio_count"]),
             int(row["document_count"]), int(row["file_count"])),
            (1, 1, 1, 1, 1, 5),
        )

    async def test_backfill_classifies_legacy_rows_once(self):
        """Rows created before the column existed get classified at startup."""
        now = "2026-11-13T00:00:00+00:00"
        async with self.db.connect() as db:
            await db.execute("""
                INSERT INTO files(file_key, owner_id, file_name, media_type, category,
                    primary_message_id, backup_message_id, created_at, media_kind)
                VALUES('legacy1', 1, '旧视频.mp4', 'document', '旧目录', 7001, 0, ?, '')
            """, (now,))
            await db.execute("DELETE FROM settings WHERE key='media_kind_backfill_v1'")
            await db.commit()
        await self.db.initialize()
        async with self.db.connect() as db:
            row = await (await db.execute(
                "SELECT media_kind FROM files WHERE file_key='legacy1'"
            )).fetchone()
        self.assertEqual(row["media_kind"], "video")

    async def test_settings_cache_reflects_immediate_writes(self):
        """Cached settings must never hide a value the same process just saved."""
        await self.db.set_setting("member_auto_delete_seconds", "600")
        self.assertEqual(await self.db.get_setting("member_auto_delete_seconds"), "600")
        # Second read is served from cache and must return the same value.
        self.assertEqual(await self.db.get_setting("member_auto_delete_seconds"), "600")
        await self.db.set_setting("member_auto_delete_seconds", "120")
        self.assertEqual(await self.db.get_setting("member_auto_delete_seconds"), "120")
        self.assertIsNone(await self.db.get_setting("never_configured_key"))

if __name__ == "__main__":
    unittest.main()
