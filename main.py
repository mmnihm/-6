import asyncio
import logging
import secrets
from pathlib import Path

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import (
    ChannelInvalid, ChatAdminRequired, PeerIdInvalid, UserNotParticipant
)

import config
from database import Database

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("member-cloud")

db = Database(config.DATABASE_PATH)
app = Client(
    "member_cloud_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

WELCOME = (
    "☁️ 会员文件网盘\n\n"
    "发送文件即可保存并生成分享链接。\n"
    "使用分享链接可取回文件。\n\n"
    "仅会员群成员和管理员可以使用。"
)

async def is_admin(user_id: int) -> bool:
    return await db.get_admin_id() == user_id

async def has_access(user_id: int) -> bool:
    if await is_admin(user_id):
        return True
    try:
        member = await app.get_chat_member(config.MEMBER_GROUP_ID, user_id)
        return member.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
    except (UserNotParticipant, PeerIdInvalid, ChannelInvalid):
        return False
    except Exception:
        log.exception("Membership check failed for user %s", user_id)
        return False

def media_details(message):
    candidates = (
        ("document", message.document),
        ("video", message.video),
        ("audio", message.audio),
        ("photo", message.photo),
        ("animation", message.animation),
        ("voice", message.voice),
    )
    for kind, media in candidates:
        if media:
            name = getattr(media, "file_name", None) or f"{kind}_{message.id}"
            return kind, name
    return None, None

async def send_share_link(message, key: str):
    link = f"https://t.me/{config.BOT_USERNAME}?start=file_{key}"
    await message.reply_text(
        "✅ 文件已保存到主存储和备份频道。\n"
        f"📄 文件：{Path(message.document.file_name).name if message.document and message.document.file_name else '已上传文件'}\n\n"
        f"🔗 分享链接：\n{link}",
        disable_web_page_preview=True,
    )

@app.on_message(filters.private & filters.command("start"))
async def start_handler(client, message):
    user = message.from_user
    if not user:
        return
    await db.touch_user(user.id)

    # First private /start atomically claims the one-time superadmin slot.
    admin_id = await db.get_admin_id()
    if admin_id is None:
        claimed = await db.claim_first_admin(user.id)
        if claimed:
            await message.reply_text(
                "👑 首位管理员认领成功！\n"
                "你现在是本机器人的超级管理员。\n\n" + WELCOME
            )
            return

    payload = message.command[1] if len(message.command) > 1 else ""
    if payload.startswith("file_"):
        if not await has_access(user.id):
            await message.reply_text("⛔ 仅限会员群成员使用。请先加入会员群后重试。")
            return
        key = payload[5:]
        record = await db.get_file(key)
        if not record:
            await message.reply_text("❌ 分享链接无效或文件不存在。")
            return
        try:
            await client.copy_message(
                chat_id=user.id,
                from_chat_id=config.STORAGE_CHANNEL_ID,
                message_id=record["primary_message_id"],
            )
        except Exception:
            log.warning("Primary copy failed for %s; trying backup", key, exc_info=True)
            try:
                await client.copy_message(
                    chat_id=user.id,
                    from_chat_id=config.BACKUP_CHANNEL_ID,
                    message_id=record["backup_message_id"],
                )
            except Exception:
                log.exception("Both storage copies failed for %s", key)
                await message.reply_text("❌ 文件暂时无法取回，请联系管理员检查备份频道。")
        return

    if not await has_access(user.id):
        await message.reply_text("⛔ 本机器人仅对会员开放。请先加入会员群。")
        return
    await message.reply_text(WELCOME)

@app.on_message(filters.private & (filters.document | filters.video | filters.audio |
                                   filters.photo | filters.animation | filters.voice))
async def upload_handler(client, message):
    user = message.from_user
    if not user:
        return
    await db.touch_user(user.id)
    if not await has_access(user.id):
        await message.reply_text("⛔ 仅限会员群成员上传文件。")
        return

    media_type, file_name = media_details(message)
    if not media_type:
        return

    status = await message.reply_text("⏳ 正在保存到主频道和备份频道…")
    primary_id = None
    try:
        primary = await client.copy_message(
            chat_id=config.STORAGE_CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
        primary_id = primary.id
        backup = await client.copy_message(
            chat_id=config.BACKUP_CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
        key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
        await db.add_file(key, user.id, file_name, media_type, primary.id, backup.id)
        link = f"https://t.me/{config.BOT_USERNAME}?start=file_{key}"
        await status.edit_text(
            "✅ 双频道保存成功\n"
            f"📄 {file_name}\n\n"
            f"🔗 分享链接：\n{link}",
            disable_web_page_preview=True,
        )
    except Exception:
        log.exception("Upload/store failed")
        if primary_id is not None:
            try:
                await client.delete_messages(config.STORAGE_CHANNEL_ID, primary_id)
            except Exception:
                log.warning("Could not roll back primary message %s", primary_id)
        await status.edit_text("❌ 保存失败，未生成分享链接。请检查两个私有频道的机器人管理员权限。")

@app.on_message(filters.private & filters.command("admin"))
async def admin_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await message.reply_text("⛔ 仅超级管理员可使用。")
        return
    users, files = await db.stats()
    await message.reply_text(
        "👑 管理员面板（基础版）\n\n"
        f"用户数：{users}\n"
        f"文件数：{files}\n\n"
        "管理员认领已锁定，机器人重启不会重置。"
    )

async def main():
    await db.initialize()
    await app.start()
    me = await app.get_me()
    log.info("Bot started: @%s", me.username)
    try:
        await asyncio.Event().wait()
    finally:
        await app.stop()

if __name__ == "__main__":
    app.run(main())
