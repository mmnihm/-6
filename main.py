import asyncio
import logging
import os
import secrets
from datetime import datetime, timezone

from aiohttp import web

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.errors import (
    FloodWait, PeerIdInvalid, UserNotParticipant, SessionPasswordNeeded,
)

import config
from database import Database
from web_catalog import create_catalog_app

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("member-cloud")
db = Database(config.DATABASE_PATH)
BOT_USERNAME = None
pending_channel_bindings = {}
scanner_login_states = {}
scanner_login_clients = {}


def admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗄 双频道存储", callback_data="mc:storage")],
        [InlineKeyboardButton("👥 绑定会员群", callback_data="mc:bindgroup")],
        [InlineKeyboardButton("👤 会员管理", callback_data="mc:members"), InlineKeyboardButton("📂 文件管理", callback_data="mc:files")],
        [InlineKeyboardButton("📢 群发广播", callback_data="mc:broadcast"), InlineKeyboardButton("📊 数据统计", callback_data="mc:stats")],
        [InlineKeyboardButton("🔐 扫描账号登录", callback_data="mc:scannerlogin")],
        [InlineKeyboardButton("📖 命令帮助", callback_data="mc:commands")],
    ])


def back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")]])
pending_binding_filter = filters.create(
    lambda _, __, message: bool(
        message.from_user and message.from_user.id in pending_channel_bindings
    )
)


async def get_storage_channels():
    primary = await db.get_setting("storage_primary_channel_id")
    backup = await db.get_setting("storage_backup_channel_id")
    return (int(primary) if primary else None, int(backup) if backup else None)


async def get_member_group_id():
    value = await db.get_setting("member_group_id")
    return int(value) if value else None

app = Client(
    "member_cloud_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

SCANNER_WORKDIR = os.path.dirname(config.DATABASE_PATH) or "."
SCANNER_SESSION_PATH = os.path.join(SCANNER_WORKDIR, "member_cloud_scanner.session")
scanner = (
    Client("member_cloud_scanner", api_id=config.API_ID,
           api_hash=config.API_HASH, workdir=SCANNER_WORKDIR)
    if os.path.exists(SCANNER_SESSION_PATH) else None
)
scan_task = None

WELCOME = (
    "☁️ 会员文件网盘\n\n"
    "• 发送文件：自动保存到主频道和备份频道\n"
    "• 分享文件：使用机器人生成的分享链接\n"
    "• 取回文件：打开分享链接即可\n\n"
    "仅会员群成员及管理员可以使用。"
)


async def is_admin(user_id: int) -> bool:
    return config.ADMIN_ID == user_id


async def has_access(user_id: int) -> bool:
    if await is_admin(user_id):
        return True
    if await db.is_banned(user_id):
        return False
    if await db.has_manual_membership(user_id):
        return True
    group_id = await get_member_group_id()
    if not group_id:
        return False
    try:
        member = await app.get_chat_member(group_id, user_id)
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
            unique_id = getattr(media, "file_unique_id", None)
            return kind, name, unique_id
    return None, None, None


def share_url(key: str) -> str:
    if not BOT_USERNAME:
        raise RuntimeError("Bot username is not initialized")
    return f"https://t.me/{BOT_USERNAME}?start=file_{key}"


def parse_category(message) -> str:
    caption = (message.caption or "").strip()
    first = caption.splitlines()[0].strip() if caption else ""
    if first.startswith("分类:") or first.startswith("分类："):
        return first[3:].strip()[:40] or "未分类"
    return "未分类"


def guess_category(message, media_type, file_name):
    explicit = parse_category(message)
    if explicit != "未分类":
        return explicit
    name = (file_name or "").lower()
    rules = (
        ("电视剧", ("电视剧", "连续剧", "season", "episode", "第", "集")),
        ("动漫", ("动漫", "动画", "番剧", "anime")),
        ("电影", ("电影", "movie", "bluray", "web-dl")),
        ("综艺", ("综艺", "真人秀", "variety")),
        ("短视频", ("短视频", "短片", "reels", "tiktok")),
        ("音乐", ("音乐", "歌曲", "专辑", "music", "album")),
        ("教程", ("教程", "课程", "教学", "course", "lesson")),
    )
    for category, keywords in rules:
        if any(word in name for word in keywords):
            return category
    return {
        "video": "视频",
        "photo": "图片",
        "animation": "动图",
        "audio": "音频",
        "voice": "语音",
        "document": "文档",
    }.get(media_type, "其他")


async def run_history_scan(primary_channel_id, backup_channel_id):
    global scan_task
    try:
        for channel_id, other_channel_id in (
            (primary_channel_id, backup_channel_id),
            (backup_channel_id, primary_channel_id),
        ):
            progress = await db.get_scan_progress(channel_id)
            if progress["completed"]:
                continue
            offset_id = progress["last_message_id"]
            log.info("History scan started for channel %s from offset %s", channel_id, offset_id)
            async for item in scanner.get_chat_history(channel_id, offset_id=offset_id):
                media_type, file_name, fingerprint = media_details(item)
                created_at = (
                    item.date.isoformat() if getattr(item, "date", None) else
                    datetime.now(timezone.utc).isoformat()
                )
                if media_type and fingerprint:
                    category = guess_category(item, media_type, file_name)
                    await db.save_scanned_message(
                        channel_id, other_channel_id, primary_channel_id,
                        item.id, fingerprint, file_name, media_type, category,
                        created_at, config.ADMIN_ID,
                        secrets.token_urlsafe(9).replace("-", "").replace("_", ""),
                    )
                else:
                    await db.advance_scan_cursor(channel_id, item.id)
            await db.mark_scan_completed(channel_id)

        primary_progress = await db.get_scan_progress(primary_channel_id)
        backup_progress = await db.get_scan_progress(backup_channel_id)
        _, total_files, _, _ = await db.stats()
        await app.send_message(
            config.ADMIN_ID,
            "✅ 主备份频道历史扫描完成\n"
            f"主频道已检查媒体：{primary_progress['scanned_count']}\n"
            f"备份频道已检查媒体：{backup_progress['scanned_count']}\n"
            f"当前目录文件总数：{total_files}\n\n"
            "只有主、备份频道都能匹配到的资源才会加入可取回目录。",
        )
    except asyncio.CancelledError:
        await app.send_message(config.ADMIN_ID, "⏸️ 历史扫描已暂停。再次发送 /scan 可从上次进度继续。")
        raise
    except Exception:
        log.exception("History scan failed")
        try:
            await app.send_message(
                config.ADMIN_ID,
                "⚠️ 历史扫描遇到错误，已保留进度。修复频道访问或稍后发送 /scan 继续。",
            )
        except Exception:
            log.exception("Could not notify admin about scan failure")
    finally:
        scan_task = None


async def reply_admin_only(message):
    await message.reply_text("⛔ 仅超级管理员可使用此命令。")


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client, message):
    user = message.from_user
    if not user:
        return
    await db.touch_user(user.id)

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
        primary_channel_id, backup_channel_id = await get_storage_channels()
        if not primary_channel_id or not backup_channel_id:
            await message.reply_text("⚠️ 管理员尚未绑定主存储和备份频道，请稍后再试。")
            return
        try:
            await client.copy_message(
                chat_id=user.id,
                from_chat_id=primary_channel_id,
                message_id=record["primary_message_id"],
            )
        except Exception:
            log.warning("Primary copy failed for %s; trying backup", key, exc_info=True)
            try:
                await client.copy_message(
                    chat_id=user.id,
                    from_chat_id=backup_channel_id,
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

    media_type, file_name, source_fingerprint = media_details(message)
    if not media_type:
        return

    if source_fingerprint and await db.get_file_by_fingerprint(source_fingerprint):
        await message.reply_text(
            f"⏭️ 已跳过重复文件：{file_name}\n这个文件此前已经导入网盘。"
        )
        return

    primary_channel_id, backup_channel_id = await get_storage_channels()
    if not primary_channel_id or not backup_channel_id:
        await message.reply_text("⚠️ 网盘尚未完成双频道绑定，请管理员先使用 /bindmain 和 /bindbackup。")
        return

    category = parse_category(message)
    status = await message.reply_text("⏳ 正在保存到主频道和备份频道…")
    primary_id = None
    backup_id = None
    try:
        primary = await client.copy_message(
            chat_id=primary_channel_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
        primary_id = primary.id
        backup = await client.copy_message(
            chat_id=backup_channel_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
        backup_id = backup.id

        key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
        inserted = await db.add_file(
            key, user.id, file_name, media_type,
            primary.id, backup.id, category, source_fingerprint
        )
        if not inserted:
            for channel_id, message_id in (
                (primary_channel_id, primary.id),
                (backup_channel_id, backup.id),
            ):
                try:
                    await client.delete_messages(channel_id, message_id)
                except Exception:
                    log.warning("Could not remove duplicate import copy %s", message_id)
            await status.edit_text(f"⏭️ 已跳过重复文件：{file_name}")
            return
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
            (primary_channel_id, primary_id),
            (backup_channel_id, backup_id),
        ):
            if message_id is not None:
                try:
                    await client.delete_messages(channel_id, message_id)
                except Exception:
                    log.warning("Could not roll back storage message %s", message_id)
        await status.edit_text(
            "❌ 保存失败，未生成分享链接。请检查两个私有频道的机器人管理员权限。"
        )


@app.on_message(filters.private & filters.forwarded & pending_binding_filter)
async def bind_channel_forward_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        return

    setting_key = pending_channel_bindings.get(user.id)
    if not setting_key:
        return
    source = message.forward_from_chat
    if not source:
        await message.reply_text("❌ 没识别到转发来源。请从对应的频道或会员群直接转发一条普通消息，不要复制粘贴。")
        return
    is_group_binding = setting_key == "member_group_id"
    allowed_types = (ChatType.GROUP, ChatType.SUPERGROUP) if is_group_binding else (ChatType.CHANNEL,)
    if source.type not in allowed_types:
        expected = "会员群" if is_group_binding else "私有频道"
        await message.reply_text(f"❌ 来源类型不正确，请直接转发一条{expected}消息。")
        return

    me = await client.get_me()
    try:
        member = await client.get_chat_member(source.id, me.id)
    except Exception:
        await message.reply_text("❌ 机器人无法访问这个频道。请先把机器人添加为该频道管理员，再重新转发消息。")
        return
    valid_statuses = (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER) if not is_group_binding else (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER, ChatMemberStatus.RESTRICTED)
    if member.status not in valid_statuses:
        await message.reply_text("❌ 机器人尚未加入这个群/频道，请先添加机器人后重新绑定。")
        return

    if not is_group_binding:
        other_key = "storage_backup_channel_id" if setting_key == "storage_primary_channel_id" else "storage_primary_channel_id"
        other_id = await db.get_setting(other_key)
        if other_id and int(other_id) == source.id:
            await message.reply_text("❌ 主存储频道和备份频道不能绑定为同一个频道。")
            return

    await db.set_setting(setting_key, str(source.id))
    pending_channel_bindings.pop(user.id, None)
    labels = {
        "storage_primary_channel_id": "主存储频道",
        "storage_backup_channel_id": "备份存储频道",
        "member_group_id": "会员群",
    }
    label = labels.get(setting_key, "目标群/频道")
    await message.reply_text(
        f"✅ {label}绑定成功！\n"
        f"名称：{source.title or '未命名'}\n"
        f"ID：{source.id}\n\n"
        "可发送 /storage 查看绑定状态，或 /admin 打开管理面板。"
    )


@app.on_message(filters.private & filters.command("bindmain"))
async def bind_main_channel_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    pending_channel_bindings[user.id] = "storage_primary_channel_id"
    await message.reply_text(
        "📥 请把主存储私有频道中的任意一条消息，直接转发到这里。\n"
        "要求：机器人必须已经是该频道管理员。"
    )


@app.on_message(filters.private & filters.command("bindbackup"))
async def bind_backup_channel_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    pending_channel_bindings[user.id] = "storage_backup_channel_id"
    await message.reply_text(
        "📥 请把备份存储私有频道中的任意一条消息，直接转发到这里。\n"
        "要求：机器人必须已经是该频道管理员。"
    )


@app.on_message(filters.private & filters.command("bindgroup"))
async def bind_member_group_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    await message.reply_text(
        "👥 私密会员群请使用群内绑定方式：\n"
        "1. 先把机器人添加到会员群并设为管理员。\n"
        "2. 在会员群内发送 /bindgroup。\n"
        "3. 机器人会自动读取当前群 ID 并完成绑定。\n\n"
        "不需要公开群组，也不需要转发消息。"
    )


@app.on_message(filters.group & filters.command("bindgroup"))
async def bind_member_group_in_chat_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await message.reply_text("⛔ 仅超级管理员可绑定会员群。")
        return

    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply_text("❌ 请在需要绑定的会员群内发送 /bindgroup。")
        return

    me = await client.get_me()
    try:
        bot_member = await client.get_chat_member(message.chat.id, me.id)
    except Exception:
        log.exception("Could not verify bot membership in group %s", message.chat.id)
        await message.reply_text("❌ 机器人无法读取本群成员状态。请确认机器人已加入群组，并将机器人设为管理员后重试。")
        return

    valid_statuses = (
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
        ChatMemberStatus.RESTRICTED,
    )
    if bot_member.status not in valid_statuses:
        await message.reply_text("❌ 机器人尚未加入本群，请先添加机器人后重试。")
        return

    await db.set_setting("member_group_id", str(message.chat.id))
    pending_channel_bindings.pop(user.id, None)
    await message.reply_text(
        "✅ 会员群绑定成功！\n"
        f"群组：{message.chat.title or '未命名'}\n"
        f"群 ID：{message.chat.id}\n\n"
        "机器人已记录这个私密群组。可回到私聊发送 /storage 查看绑定状态。"
    )


@app.on_message(filters.private & filters.command("scanlogin"))
async def scanner_login_start_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scanner is not None and scanner.is_connected:
        await message.reply_text("✅ 扫描账号已经登录。可以直接发送 /scan 开始扫描。")
        return
    old_client = scanner_login_clients.pop(user.id, None)
    if old_client:
        try:
            await old_client.disconnect()
        except Exception:
            pass
    login_client = Client(
        "member_cloud_scanner",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        workdir=SCANNER_WORKDIR,
    )
    try:
        await login_client.connect()
    except Exception:
        log.exception("Could not connect scanner login client")
        await message.reply_text("❌ 无法连接 Telegram 登录服务，请稍后重试。")
        return
    scanner_login_clients[user.id] = login_client
    scanner_login_states[user.id] = {"step": "phone"}
    await message.reply_text(
        "🔐 扫描账号登录\n\n"
        "请发送专门用于扫描的普通 Telegram 账号手机号（含国家区号，例如 +1）。\n"
        "接下来 Telegram 会发送登录验证码，你需要在这里输入验证码；如开启两步验证，还需要输入密码。\n\n"
        "⚠️ 建议使用专用扫描账号，不要使用存有重要私聊的主账号。验证码/两步验证密码会短暂经过本机器人私聊，处理后会尝试立即删除输入消息。\n"
        "发送 /scancancel 可取消。"
    )


@app.on_message(filters.private & filters.command("scancancel"))
async def scanner_login_cancel_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    login_client = scanner_login_clients.pop(user.id, None)
    scanner_login_states.pop(user.id, None)
    if login_client:
        try:
            await login_client.disconnect()
        except Exception:
            log.exception("Could not disconnect cancelled scanner login")
    await message.reply_text("已取消扫描账号登录流程。")


@app.on_message(
    filters.private & filters.text & ~filters.regex(r"^/")
)
async def scanner_login_input_handler(client, message):
    global scanner
    user = message.from_user
    if not user or not await is_admin(user.id):
        return
    state = scanner_login_states.get(user.id)
    login_client = scanner_login_clients.get(user.id)
    if not state or not login_client:
        return
    value = (message.text or "").strip()
    if not value:
        return

    if state["step"] == "phone":
        phone = value.replace(" ", "").replace("-", "")
        try:
            sent = await login_client.send_code(phone)
            state.update({
                "step": "code",
                "phone": phone,
                "phone_code_hash": sent.phone_code_hash,
            })
            await message.reply_text(
                "📩 已请求 Telegram 登录验证码。\n"
                "请查看 Telegram 官方服务消息或短信，把收到的验证码发送到这里。\n"
                "不要发送其他账号的验证码。"
            )
        except Exception:
            log.exception("Could not request scanner login code")
            await message.reply_text("❌ 验证码发送失败，请检查手机号和网络后发送 /scancancel，再重新 /scanlogin。")
        return

    if state["step"] == "code":
        try:
            signed_in = await login_client.sign_in(
                state["phone"], state["phone_code_hash"], value
            )
        except SessionPasswordNeeded:
            state["step"] = "password"
            try:
                await message.delete()
            except Exception:
                pass
            await message.reply_text(
                "🔒 这个账号开启了两步验证。\n"
                "请发送该扫描账号的两步验证密码。处理后会尝试立即删除密码消息。"
            )
            return
        except Exception:
            log.exception("Scanner account verification code failed")
            try:
                await message.delete()
            except Exception:
                pass
            await message.reply_text("❌ 验证码无效或已过期。请发送 /scancancel 后重新 /scanlogin。")
            return
        try:
            await message.delete()
        except Exception:
            pass
        if getattr(signed_in, "is_bot", False):
            await login_client.log_out()
            scanner_login_clients.pop(user.id, None)
            scanner_login_states.pop(user.id, None)
            await message.reply_text("❌ 不能使用机器人账号，请使用普通 Telegram 用户账号。")
            return
    elif state["step"] == "password":
        try:
            signed_in = await login_client.check_password(value)
        except Exception:
            log.exception("Scanner account two-step password failed")
            try:
                await message.delete()
            except Exception:
                pass
            await message.reply_text("❌ 两步验证密码不正确。请重新输入，或发送 /scancancel 取消。")
            return
        try:
            await message.delete()
        except Exception:
            pass
        if getattr(signed_in, "is_bot", False):
            await login_client.log_out()
            scanner_login_clients.pop(user.id, None)
            scanner_login_states.pop(user.id, None)
            await message.reply_text("❌ 不能使用机器人账号，请使用普通 Telegram 用户账号。")
            return
    else:
        return

    try:
        await login_client.disconnect()
        scanner = Client(
            "member_cloud_scanner",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            workdir=SCANNER_WORKDIR,
        )
        await scanner.start()
        scanner_me = await scanner.get_me()
        if scanner_me.is_bot:
            raise RuntimeError("Scanner login must be a normal user account")
        scanner_login_clients.pop(user.id, None)
        scanner_login_states.pop(user.id, None)
        await message.reply_text(
            "✅ 扫描账号登录成功！\n"
            f"账号 ID：{scanner_me.id}\n"
            "登录会话已保存在部署平台的数据目录中。\n\n"
            "请确保平台的数据目录启用了持久化存储。现在可以发送 /scan 开始扫描主、备份频道。"
        )
    except Exception:
        log.exception("Could not activate scanner after login")
        try:
            await login_client.disconnect()
        except Exception:
            pass
        scanner = None
        scanner_login_clients.pop(user.id, None)
        scanner_login_states.pop(user.id, None)
        await message.reply_text(
            "⚠️ 验证通过，但扫描账号启动失败。请确认部署平台的数据目录可写并且已持久化，然后重新 /scanlogin。"
        )


@app.on_message(filters.private & filters.command("scan"))
async def scan_history_handler(client, message):
    global scan_task
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scanner is None:
        await message.reply_text(
            "⚠️ 尚未登录历史扫描账号。\n"
            "请先发送 /scanlogin，在机器人私聊中完成登录。"
        )
        return
    if scan_task and not scan_task.done():
        await message.reply_text("⏳ 历史扫描正在运行。发送 /scanstatus 查看进度。")
        return
    primary, backup = await get_storage_channels()
    if not primary or not backup:
        await message.reply_text("❌ 请先绑定主存储频道和备份频道。")
        return
    try:
        await scanner.get_chat(primary)
        await scanner.get_chat(backup)
    except Exception:
        log.exception("Scanner account cannot access both storage channels")
        await message.reply_text(
            "❌ 扫描账号无法访问主频道或备份频道。\n"
            "请先用普通 Telegram 账号加入这两个私有频道，再重新运行 /scan。"
        )
        return
    p = await db.get_scan_progress(primary)
    b = await db.get_scan_progress(backup)
    if p["completed"] and b["completed"]:
        await message.reply_text("✅ 两个频道的历史扫描已完成。已有目录会保留，新增资源由机器人正常登记。")
        return
    scan_task = asyncio.create_task(run_history_scan(primary, backup))
    await message.reply_text(
        "🚀 已启动主、备份频道历史扫描。\n"
        "只读取文件信息并建立目录，不下载、不转发、不删除原消息。\n\n"
        "发送 /scanstatus 查看进度；发送 /scanstop 可暂停，之后可继续。"
    )


@app.on_message(filters.private & filters.command("scanstatus"))
async def scan_status_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    primary, backup = await get_storage_channels()
    if not primary or not backup:
        await message.reply_text("主频道或备份频道尚未绑定。")
        return
    p = await db.get_scan_progress(primary)
    b = await db.get_scan_progress(backup)
    running = bool(scan_task and not scan_task.done())
    def label(progress):
        if progress["completed"]:
            return "已完成"
        if not progress["last_message_id"] and not progress["scanned_count"]:
            return "未开始"
        return f"待继续（最近消息 ID：{progress['last_message_id']}）"
    await message.reply_text(
        f"📊 历史扫描状态：{'运行中' if running else '未运行'}\n\n"
        f"主频道：{label(p)}\n已检查媒体：{p['scanned_count']}\n\n"
        f"备份频道：{label(b)}\n已检查媒体：{b['scanned_count']}"
    )


@app.on_message(filters.private & filters.command("scanlogout"))
async def scanner_logout_handler(client, message):
    global scanner
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scan_task and not scan_task.done():
        await message.reply_text("⚠️ 扫描正在运行，请先发送 /scanstop，暂停后再退出账号。")
        return
    pending = scanner_login_clients.pop(user.id, None)
    scanner_login_states.pop(user.id, None)
    if pending:
        try:
            await pending.disconnect()
        except Exception:
            log.exception("Could not disconnect pending scanner login")
    try:
        if scanner is not None and scanner.is_connected:
            await scanner.log_out()
        elif os.path.exists(SCANNER_SESSION_PATH):
            os.remove(SCANNER_SESSION_PATH)
            journal = SCANNER_SESSION_PATH + "-journal"
            if os.path.exists(journal):
                os.remove(journal)
        scanner = None
        await message.reply_text("✅ 扫描账号已退出并清除本地登录会话。需要更换账号时，再打开 /admin → 扫描账号登录。")
    except Exception:
        log.exception("Could not log out scanner account")
        await message.reply_text("❌ 退出扫描账号失败，请稍后重试。")


@app.on_message(filters.private & filters.command("scanstop"))
async def scan_stop_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scan_task and not scan_task.done():
        scan_task.cancel()
        await message.reply_text("⏸️ 已请求暂停扫描，进度会保留。")
    else:
        await message.reply_text("当前没有正在运行的扫描任务。")


@app.on_message(filters.private & filters.command("import"))
async def import_help_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    await message.reply_text(
        "📥 历史文件批量导入\n\n"
        "1. 打开原来的仓库群。\n"
        "2. 选择需要导入的旧文件，批量转发给本机器人。\n"
        "3. 机器人会逐个保存到主频道和备份频道，并加入网页目录。\n"
        "4. 已导入过的相同文件会自动跳过。\n\n"
        "如果这些旧资源已经在本机器人的主、备份存储频道中，无需转发，可先登录普通扫描账号，再发送 /scan 自动建立目录。"
        "本命令仍适用于从其他仓库群导入资源；如果原群禁止转发，需由有权限的管理员发送文件副本。"
    )


@app.on_message(filters.private & filters.command("storage"))
async def storage_status_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    primary, backup = await get_storage_channels()
    group_id = await get_member_group_id()
    await message.reply_text(
        "🗄 机器人绑定状态\n\n"
        f"会员群：{group_id or '未绑定'}\n"
        f"主存储频道：{primary or '未绑定'}\n"
        f"备份存储频道：{backup or '未绑定'}\n\n"
        "绑定命令：/bindgroup、/bindmain、/bindbackup"
    )


@app.on_message(filters.private & filters.command("admin"))
async def admin_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    users, files, members, banned = await db.stats()
    group_id = await get_member_group_id()
    primary, backup = await get_storage_channels()
    await message.reply_text(
        "👑 超级管理员控制面板\n\n"
        f"会员群：{'已绑定' if group_id else '未绑定'}\n"
        f"主存储：{'已绑定' if primary else '未绑定'}  |  备份：{'已绑定' if backup else '未绑定'}\n\n"
        f"用户：{users}  |  文件：{files}\n"
        f"手动会员：{members}  |  封禁：{banned}\n\n"
        "请选择下方按钮管理机器人。",
        reply_markup=admin_keyboard(),
    )


@app.on_callback_query(filters.regex(r"^mc:"))
async def admin_callback_handler(client, query):
    user = query.from_user
    if not user or not await is_admin(user.id):
        await query.answer("无管理员权限", show_alert=True)
        return
    await query.answer()
    action = query.data.split(":", 1)[1]
    message = query.message
    if action == "home":
        users, files, members, banned = await db.stats()
        group_id = await get_member_group_id()
        primary, backup = await get_storage_channels()
        text = (
            "👑 超级管理员控制面板\n\n"
            f"会员群：{'已绑定' if group_id else '未绑定'}\n"
            f"主存储：{'已绑定' if primary else '未绑定'}  |  备份：{'已绑定' if backup else '未绑定'}\n\n"
            f"用户：{users}  |  文件：{files}\n手动会员：{members}  |  封禁：{banned}\n\n请选择下方按钮管理机器人。"
        )
        await message.edit_text(text, reply_markup=admin_keyboard())
    elif action == "storage":
        primary, backup = await get_storage_channels()
        group_id = await get_member_group_id()
        await message.edit_text(
            "🗄 绑定设置\n\n"
            f"会员群：{group_id or '未绑定'}\n主存储频道：{primary or '未绑定'}\n备份频道：{backup or '未绑定'}\n\n选择需要绑定或刷新状态：",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👥 绑定会员群", callback_data="mc:bindgroup")],
                [InlineKeyboardButton("📥 绑定主频道", callback_data="mc:bindmain"), InlineKeyboardButton("📥 绑定备份频道", callback_data="mc:bindbackup")],
                [InlineKeyboardButton("⬅️ 返回", callback_data="mc:home")],
            ]),
        )
    elif action in ("bindgroup", "bindmain", "bindbackup"):
        setting = {"bindgroup": "member_group_id", "bindmain": "storage_primary_channel_id", "bindbackup": "storage_backup_channel_id"}[action]
        pending_channel_bindings[user.id] = setting
        label = {"bindgroup": "会员群", "bindmain": "主存储私有频道", "bindbackup": "备份私有频道"}[action]
        await message.edit_text(
            f"📨 请把{label}中的任意一条普通消息直接转发给机器人。\n\n"
            "会员群：推荐直接在会员群内发送 /bindgroup，无需转发。\n"
            "存储频道：仍可直接转发一条频道消息；机器人需要是频道管理员。",
            reply_markup=back_keyboard(),
        )
    elif action == "members":
        rows = await db.list_manual_members()
        if not rows:
            body = "暂无手动授权记录。\n\n授权：/grant 用户ID 天数|永久\n取消：/revoke 用户ID\n封禁：/ban 用户ID 原因\n解封：/unban 用户ID"
        else:
            now = datetime.now(timezone.utc)
            lines = []
            for row in rows:
                expiry = row["expires_at"]
                if expiry is None:
                    label = "永久"
                else:
                    try:
                        expiry_dt = datetime.fromisoformat(expiry)
                        label = "已过期" if expiry_dt <= now else expiry_dt.strftime("%Y-%m-%d %H:%M UTC")
                    except ValueError:
                        label = "日期异常"
                lines.append(f"{row['user_id']} — {label}")
            body = "👥 手动会员列表\n" + "\n".join(lines) + "\n\n授权：/grant 用户ID 天数|永久\n取消：/revoke 用户ID"
        await message.edit_text(body, reply_markup=back_keyboard())
    elif action == "files":
        rows = await db.list_files(15)
        lines = [f"{row['file_key']} | {row['category']} | {row['file_name'][:40]}" for row in rows]
        body = "📂 最近文件（最多 15 个）\n" + ("\n".join(lines) if lines else "暂无文件记录") + "\n\n修改分类：/category 文件码 分类\n批量分享：/share 文件码1 文件码2"
        await message.edit_text(body, reply_markup=back_keyboard())
    elif action == "broadcast":
        await message.edit_text("📢 广播操作\n\n发送：/broadcast 广播内容\n机器人会向已记录用户逐一发送，并返回成功/失败数量。", reply_markup=back_keyboard())
    elif action == "stats":
        users, files, members, banned = await db.stats()
        await message.edit_text(f"📊 数据统计\n\n用户总数：{users}\n文件总数：{files}\n有效手动会员：{members}\n封禁用户：{banned}", reply_markup=back_keyboard())
    elif action == "scannerlogin":
        await message.edit_text(
            "🔐 扫描账号设置\n\n"
            "使用普通 Telegram 用户账号读取主、备份频道历史。建议使用专用账号，并先将它加入两个私有频道。\n\n"
            "点击下方按钮后，机器人会引导你输入手机号、登录验证码及（如有）两步验证密码。敏感输入会尝试立即删除。\n"
            "登录成功后会保存在部署平台的数据目录中；请确保该目录已启用持久化。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔑 开始登录", callback_data="mc:scannerloginstart")],
                [InlineKeyboardButton("⬅️ 返回", callback_data="mc:home")],
            ]),
        )
    elif action == "scannerloginstart":
        if scanner is not None and scanner.is_connected:
            await message.edit_text("✅ 扫描账号已经登录，可以直接发送 /scan 开始扫描。", reply_markup=back_keyboard())
        else:
            old_client = scanner_login_clients.pop(user.id, None)
            if old_client:
                try:
                    await old_client.disconnect()
                except Exception:
                    pass
            login_client = Client(
                "member_cloud_scanner",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                workdir=SCANNER_WORKDIR,
            )
            try:
                await login_client.connect()
                scanner_login_clients[user.id] = login_client
                scanner_login_states[user.id] = {"step": "phone"}
                await message.edit_text(
                    "🔐 请发送扫描账号的手机号（含国家区号，例如 +1）。\n\n"
                    "随后按提示输入 Telegram 登录验证码；如开启两步验证，还需要输入密码。\n"
                    "建议使用专用扫描账号。验证码/密码会短暂经过本机器人私聊，处理后会尝试立即删除。\n\n"
                    "发送 /scancancel 可取消。"
                )
            except Exception:
                log.exception("Could not start scanner login from admin panel")
                await message.edit_text("❌ 无法启动登录流程，请稍后重试。", reply_markup=back_keyboard())
    elif action == "commands":
        await message.edit_text(
            "📖 管理命令速查\n\n"
            "/bindgroup — 绑定会员群\n/bindmain — 绑定主存储频道\n/bindbackup — 绑定备份频道\n/storage — 查看绑定状态\n"
            "/grant 用户ID 天数|永久 — 授权会员\n/revoke 用户ID — 取消授权\n/ban 用户ID 原因 — 封禁\n/unban 用户ID — 解封\n"
            "/members — 会员列表\n/users — 用户列表\n/files — 文件列表\n/import — 批量转发导入旧文件\n/scanlogin — 在机器人内登录扫描账号\n/scancancel — 取消扫描账号登录\n/scanlogout — 退出/更换扫描账号\n/scan — 自动扫描主备份频道历史\n/scanstatus — 查看扫描进度\n/scanstop — 暂停扫描\n/category 文件码 分类 — 修改分类\n/share 文件码1 文件码2 — 批量分享\n/broadcast 内容 — 广播",
            reply_markup=back_keyboard(),
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
    global BOT_USERNAME
    await db.initialize()
    await app.start()
    scanner_started = False
    runner = None
    try:
        global scanner
        if scanner is not None:
            try:
                await scanner.start()
                scanner_started = True
                scanner_me = await scanner.get_me()
                if scanner_me.is_bot:
                    raise RuntimeError("History scanner must be a normal Telegram user account")
                log.info("History scanner ready: user %s", scanner_me.id)
            except Exception:
                log.exception("History scanner is unavailable; bot will continue without history scanning")
                if scanner_started:
                    try:
                        await scanner.stop()
                    except Exception:
                        log.exception("Could not stop failed history scanner session")
                    scanner_started = False
                scanner = None

        me = await app.get_me()
        BOT_USERNAME = me.username
        log.info("Bot started: @%s", me.username)

        runner = web.AppRunner(create_catalog_app(db, lambda: BOT_USERNAME))
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", config.WEB_PORT)
        await site.start()
        log.info("Public catalog listening on port %s", config.WEB_PORT)

        await asyncio.Event().wait()
    finally:
        if runner is not None:
            await runner.cleanup()
        if scanner is not None and scanner.is_connected:
            await scanner.stop()
        await app.stop()


if __name__ == "__main__":
    app.run(main())
