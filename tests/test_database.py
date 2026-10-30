import os
import tempfile
import unittest
import faulthandler

faulthandler.dump_traceback_later(30, repeat=True)

from database import Database


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.temp_dir.name, "test.db"))
        await self.db.initialize()

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

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
