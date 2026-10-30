import asyncio
import logging
import secrets
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import FloodWait, PeerIdInvalid, UserNotParticipant

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
    "• 发送文件：自动保存到主频道和备份频道\n"
    "• 分享文件：使用机器人生成的分享链接\n"
    "• 取回文件：打开分享链接即可\n\n"
    "仅会员群成员及管理员可以使用。"
)


async def is_admin(user_id: int) -> bool:
    return await db.get_admin_id() == user_id


async def has_access(user_id: int) -> bool:
    if await is_admin(user_id):
        return True
    if await db.is_banned(user_id):
        return False
    if await db.has_manual_membership(user_id):
        return True
    try:
        member = await app.get_chat_member(config.MEMBER_GROUP_ID, user_id)
        if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
            return False
        if member.status == ChatMemberStatus.RESTRICTED:
            return bool(getattr(member, "is_member", False))
        return True
    except (UserNotParticipant, PeerIdInvalid):
        return False
    except Exception:
        log.exception("Membership check failed for user %s", user_id)
        return False


def media_details(message):
    for kind in ("document", "video", "audio", "photo", "animation", "voice"):
        media = getattr(message, kind, None)
        if media:
            name = getattr(media, "file_name", None) or f"{kind}_{message.id}"
            return kind, name
    return None, None


def share_url(key: str) -> str:
    return f"https://t.me/{config.BOT_USERNAME}?start=file_{key}"


def parse_category(message) -> str:
    caption = (message.caption or "").strip()
    first = caption.splitlines()[0].strip() if caption else ""
    if first.startswith("分类:") or first.startswith("分类："):
        return first[3:].strip()[:40] or "未分类"
    return "未分类"


async def reply_admin_only(message):
    await message.reply_text("⛔ 仅超级管理员可使用此命令。")


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client, message):
    user = message.from_user
    if not user:
        return
    await db.touch_user(user.id)

    # The first private /start atomically claims the one-time superadmin slot.
    if await db.get_admin_id() is None:
        if await db.claim_first_admin(user.id):
            await message.reply_text(
                "👑 首位管理员认领成功！\n"
                "你现在是本机器人的超级管理员。\n\n"
                + WELCOME + "\n\n发送 /admin 查看管理命令。"
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


@app.on_message(
    filters.private
    & (filters.document | filters.video | filters.audio | filters.photo |
       filters.animation | filters.voice)
)
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

    category = parse_category(message)
    status = await message.reply_text("⏳ 正在保存到主频道和备份频道…")
    primary_id = None
    backup_id = None
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
        backup_id = backup.id

        key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
        await db.add_file(
            key, user.id, file_name, media_type,
            primary.id, backup.id, category
        )
        await status.edit_text(
            "✅ 双频道保存成功\n"
            f"📂 分类：{category}\n"
            f"📄 文件：{file_name}\n\n"
            f"🔗 分享链接：\n{share_url(key)}",
            disable_web_page_preview=True,
        )
    except Exception:
        log.exception("Upload/store failed")
        for channel_id, message_id in (
            (config.STORAGE_CHANNEL_ID, primary_id),
            (config.BACKUP_CHANNEL_ID, backup_id),
        ):
            if message_id is not None:
                try:
                    await client.delete_messages(channel_id, message_id)
                except Exception:
                    log.warning("Could not roll back storage message %s", message_id)
        await status.edit_text(
            "❌ 保存失败，未生成分享链接。请检查两个私有频道的机器人管理员权限。"
        )


@app.on_message(filters.private & filters.command("admin"))
async def admin_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    users, files, members, banned = await db.stats()
    await message.reply_text(
        "👑 超级管理员面板\n\n"
        f"用户数：{users}\n文件数：{files}\n"
        f"手动有效授权：{members}\n封禁用户：{banned}\n\n"
        "管理命令：\n"
        "/grant 用户ID 天数|永久 — 手动授权\n"
        "/revoke 用户ID — 取消手动授权\n"
        "/ban 用户ID 原因 — 封禁用户\n"
        "/unban 用户ID — 解除封禁\n"
        "/users — 最近用户 ID 列表\n"
        "/members — 手动授权列表\n"
        "/files — 最近文件列表\n"
        "/category 文件码 分类 — 修改分类\n"
        "/share 文件码1 文件码2 — 批量生成分享链接\n"
        "/broadcast 消息内容 — 广播全部已记录用户\n"
        "/admin — 刷新统计"
    )


@app.on_message(filters.private & filters.command("grant"))
async def grant_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    args = message.command[1:]
    if len(args) != 2:
        await message.reply_text("用法：/grant 用户ID 天数|永久")
        return
    try:
        target = int(args[0])
        days = None if args[1] in ("永久", "永久授权", "forever") else int(args[1])
        if days is not None and not 1 <= days <= 36500:
            raise ValueError()
        await db.grant_member(target, days, user.id)
        expiry = "永久" if days is None else f"{days} 天"
        await message.reply_text(f"✅ 已为用户 {target} 开通 {expiry} 手动会员权限。")
    except ValueError:
        await message.reply_text("❌ 参数错误。天数请填写 1–36500，或填写“永久”。")


@app.on_message(filters.private & filters.command("revoke"))
async def revoke_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    try:
        target = int(message.command[1])
        await db.revoke_member(target)
        await message.reply_text(f"✅ 已取消用户 {target} 的手动授权。")
    except (IndexError, ValueError):
        await message.reply_text("用法：/revoke 用户ID")


@app.on_message(filters.private & filters.command("ban"))
async def ban_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    args = message.command[1:]
    if not args:
        await message.reply_text("用法：/ban 用户ID 原因")
        return
    try:
        target = int(args[0])
        if target == user.id:
            await message.reply_text("不能封禁超级管理员本人。")
            return
        reason = " ".join(args[1:])[:200]
        await db.ban_user(target, reason, user.id)
        await message.reply_text(f"🚫 已封禁用户 {target}。")
    except ValueError:
        await message.reply_text("❌ 用户 ID 必须是数字。")


@app.on_message(filters.private & filters.command("unban"))
async def unban_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    try:
        target = int(message.command[1])
        await db.unban_user(target)
        await message.reply_text(f"✅ 已解除用户 {target} 的封禁。")
    except (IndexError, ValueError):
        await message.reply_text("用法：/unban 用户ID")


@app.on_message(filters.private & filters.command("users"))
async def users_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    ids = await db.list_user_ids()
    if not ids:
        await message.reply_text("暂时没有用户记录。")
        return
    lines = [str(uid) for uid in ids[-50:]]
    await message.reply_text("最近用户 ID（最多 50 个）：\n" + "\n".join(lines))


@app.on_message(filters.private & filters.command("members"))
async def members_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    rows = await db.list_manual_members()
    if not rows:
        await message.reply_text("暂无手动授权记录。")
        return
    now = datetime.now(timezone.utc)
    lines = []
    for row in rows:
        expiry = row["expires_at"]
        if expiry is None:
            label = "永久"
        else:
            try:
                dt = datetime.fromisoformat(expiry)
                label = "已过期" if dt <= now else dt.strftime("%Y-%m-%d %H:%M UTC")
            except ValueError:
                label = "日期异常"
        lines.append(f'{row["user_id"]} — {label}')
    await message.reply_text("手动授权列表：\n" + "\n".join(lines))


@app.on_message(filters.private & filters.command("files"))
async def files_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    rows = await db.list_files(20)
    if not rows:
        await message.reply_text("暂时没有文件记录。")
        return
    lines = []
    for row in rows:
        lines.append(
            f'{row["file_key"]} | {row["category"]} | {row["file_name"][:50]}'
        )
    await message.reply_text("最近 20 个文件：\n" + "\n".join(lines))


@app.on_message(filters.private & filters.command("category"))
async def category_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    args = message.command[1:]
    if len(args) < 2:
        await message.reply_text("用法：/category 文件码 分类名称")
        return
    key, category = args[0], " ".join(args[1:])[:40]
    if await db.set_file_category(key, category):
        await message.reply_text(f"✅ 文件 {key} 已归类为：{category}")
    else:
        await message.reply_text("❌ 没找到这个文件码。")


@app.on_message(filters.private & filters.command("share"))
async def share_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    keys = message.command[1:]
    if not keys or len(keys) > 20:
        await message.reply_text("用法：/share 文件码1 文件码2（最多 20 个）")
        return
    links = []
    missing = []
    for key in keys:
        if await db.get_file(key):
            links.append(f"{key}: {share_url(key)}")
        else:
            missing.append(key)
    response = "批量分享链接：\n" + ("\n".join(links) if links else "没有找到有效文件。")
    if missing:
        response += "\n\n未找到：" + ", ".join(missing)
    await message.reply_text(response, disable_web_page_preview=True)


@app.on_message(filters.private & filters.command("broadcast"))
async def broadcast_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    content = message.text.split(maxsplit=1)
    if len(content) < 2 or not content[1].strip():
        await message.reply_text("用法：/broadcast 要广播的文字内容")
        return
    target_ids = await db.list_user_ids()
    sent = 0
    failed = 0
    status = await message.reply_text(f"⏳ 开始广播，共 {len(target_ids)} 位用户…")
    for target_id in target_ids:
        try:
            await client.send_message(target_id, content[1])
            sent += 1
        except FloodWait as exc:
            await asyncio.sleep(exc.value)
            try:
                await client.send_message(target_id, content[1])
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await status.edit_text(
        f"✅ 广播完成\n成功：{sent}\n失败：{failed}\n总数：{len(target_ids)}"
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
