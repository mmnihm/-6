import os
import tempfile
import unittest
import faulthandler

faulthandler.dump_traceback_later(30, repeat=True)

from database import Database, PostgresCompat


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.temp_dir.name, "test.db"))
        await self.db.initialize()

    async def asyncTearDown(self):
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
            "file_key", "file_name", "media_type", "category", "created_at"
        })
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


if __name__ == "__main__":
    unittest.main()
