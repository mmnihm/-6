import asyncio
import json
import aiohttp
import logging
import os
import secrets
from datetime import datetime, timezone, timedelta

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
db = Database(config.DATABASE_PATH, database_url=config.DATABASE_URL)
BOT_USERNAME = None
BOT_ID = None
pending_channel_bindings = {}
scanner_login_states = {}
scanner_login_clients = {}
pending_directory_imports = {}
pending_category_actions = {}
pending_home_button_actions = {}
directory_import_running = False
cangku_backup_in_progress = set()
category_delivery_in_progress = set()


def admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗄 双频道存储", callback_data="mc:storage")],
        [InlineKeyboardButton("👥 绑定会员群", callback_data="mc:bindgroup")],
        [InlineKeyboardButton("👤 会员管理", callback_data="mc:members"), InlineKeyboardButton("📂 文件管理", callback_data="mc:files")],
        [InlineKeyboardButton("📁 目录管理", callback_data="mc:directories")],
        [InlineKeyboardButton("🔘 首页按钮设置", callback_data="mc:homebuttons")],
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

async def telegram_bot_api(method: str, payload: dict):
    """Call Telegram Bot API directly, avoiding Pyrogram user-peer cache lookups."""
    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/{method}"
    timeout = aiohttp.ClientTimeout(total=12)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                data = await response.json(content_type=None)
    except Exception as exc:
        # Do not include the request URL: it contains the bot token.
        raise RuntimeError(
            f"Telegram {method} network error: {type(exc).__name__}"
        ) from None
    if not isinstance(data, dict) or not data.get("ok"):
        error_code = data.get("error_code", "unknown") if isinstance(data, dict) else "unknown"
        description = data.get("description", "invalid API response") if isinstance(data, dict) else "invalid API response"
        raise RuntimeError(f"Telegram {method} failed ({error_code}): {description}")
    return data.get("result")

app = Client(
    "member_cloud_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

SCANNER_WORKDIR = os.path.dirname(config.DATABASE_PATH) or "."
os.makedirs(SCANNER_WORKDIR, exist_ok=True)
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
    if config.ADMIN_ID == user_id:
        return True
    try:
        stored_admin_id = await db.get_admin_id()
        return stored_admin_id is not None and int(stored_admin_id) == int(user_id)
    except Exception:
        log.exception("Could not read stored superadmin ID")
        return False


async def get_membership_tier(user_id: int):
    if await is_admin(user_id):
        return "超级管理员"
    if await db.is_banned(user_id):
        log.info("Membership denied: user %s is in the local ban list", user_id)
        return None
    if await db.has_manual_membership(user_id):
        return "手动会员"

    group_id = await get_member_group_id()
    if not group_id:
        log.error("Membership check cannot run: member group is not bound")
        return None

    try:
        # Match Cangku's proven approach: Bot API accepts numeric user IDs
        # directly and does not depend on Pyrogram's local Peer/access_hash cache.
        bot_id = BOT_ID
        if not bot_id:
            bot_id = (await app.get_me()).id
        bot_member = await telegram_bot_api(
            "getChatMember",
            {"chat_id": group_id, "user_id": bot_id},
        )
        bot_status = str(bot_member.get("status", ""))
        if bot_status not in ("administrator", "creator"):
            log.error(
                "Membership verification unavailable: bot is not admin "
                "in bound group %s (bot status=%s)",
                group_id, bot_status,
            )
            return None

        member = await telegram_bot_api(
            "getChatMember",
            {"chat_id": group_id, "user_id": int(user_id)},
        )
        status = str(member.get("status", ""))
        log.info(
            "Membership check via Bot API: user=%s group=%s status=%s",
            user_id, group_id, status,
        )
        if status in ("member", "administrator", "creator"):
            return "黄金会员"
        if status == "restricted" and member.get("is_member") is True:
            return "黄金会员"
        return None
    except Exception as exc:
        log.error(
            "Bot API membership check failed: user=%s group=%s error=%s",
            user_id, group_id, str(exc)[:400],
        )
        return None


async def has_access(user_id: int) -> bool:
    return await get_membership_tier(user_id) is not None


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


def folder_share_url(share_key: str) -> str:
    if not BOT_USERNAME:
        raise RuntimeError("Bot username is not initialized")
    return f"https://t.me/{BOT_USERNAME}?start=folder_{share_key}"


def format_folder_size(value):
    size = max(0, int(value or 0))
    if size <= 0:
        return "领取资源后逐步统计"
    units = ("B", "KB", "MB", "GB", "TB")
    amount = float(size)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.2f} {unit}"
        amount /= 1024
    return f"{amount:.2f} TB"


def format_folder_datetime(value):
    if not value:
        return "暂无记录"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError, OverflowError):
        return str(value)[:32]


def folder_detail_text(category_row, files, page, member_group_id):
    name = str(category_row["category"])
    body = [
        f"📁 #{name}",
        "",
        f"🏷️ #{name}",
    ]
    description = str(category_row["description"] or "").strip()
    if description:
        body.extend(["", f"📝 {description}"])
    body.extend([
        "",
        "📖 内容统计：",
        f"　• 消息数：{int(category_row['file_count'] or 0)} 条",
        f"　• 图片数：{int(category_row['image_count'] or 0)} 张",
        f"　• 视频数：{int(category_row['video_count'] or 0)} 个",
        f"　• 总大小：{format_folder_size(category_row['total_size'])}（已统计 {int(category_row['sized_count'] or 0)}/{int(category_row['file_count'] or 0)} 个资源）",
        "",
        f"📅 创建时间：{format_folder_datetime(category_row['created_at'])}",
        f"✏️ 最后更新：{format_folder_datetime(category_row['updated_at'])}",
        "",
        "🔗 分享链接：",
        folder_share_url(str(category_row["share_key"])) if category_row["share_key"] else "暂不可用",
        "",
        "🔒 需要满足以下条件之一才能访问：",
    ])
    if member_group_id:
        body.append("👥 加入已绑定会员群/频道：1 个（系统自动验证）")
    body.append("或由管理员授予有效会员权限")
    body.extend(["", f"📄 资源列表（第 {page + 1} 页）："])
    body.append("\n".join(f"• {row['file_name'][:48]}" for row in files) if files else "这个目录暂时没有资源。")
    return "\n".join(body)


def folder_detail_keyboard(category_row, page, has_next, is_favorite):
    share_key = str(category_row["share_key"] or "")
    rows = []
    if int(category_row["file_count"] or 0) > 0:
        rows.append([InlineKeyboardButton(
            "📥 获取本批资源（最多20个）",
            callback_data=f"uc:getfolder:{share_key}:{page * 20}",
        )])
    rows.append([InlineKeyboardButton(
        "⭐ 取消收藏目录" if is_favorite else "⭐ 收藏目录",
        callback_data=f"uc:folderstar:{share_key}",
    )])
    nav = []
    category_index = int(category_row["category_index"])
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"uc:folder:{category_index}:{max(0, page * 20 - 20)}"))
    if has_next:
        nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"uc:folder:{category_index}:{(page + 1) * 20}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ 文件夹列表", callback_data="uc:folders")])
    return InlineKeyboardMarkup(rows)


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
        # Backfill files already scanned by the older dual-match-only version.
        backfilled = await db.publish_primary_catalog(primary_channel_id, config.ADMIN_ID)
        if backfilled:
            log.info("Published %s existing primary-channel files to catalog", backfilled)

        for channel_id, other_channel_id in (
            (primary_channel_id, backup_channel_id),
            (backup_channel_id, primary_channel_id),
        ):
            progress = await db.get_scan_progress(channel_id)
            if progress["completed"]:
                continue
            offset_id = progress["last_message_id"]
            chat = await resolve_scanner_chat(channel_id)
            resolved_channel_id = int(chat.id)
            log.info(
                "History scan started for channel %s (%s) from offset %s",
                resolved_channel_id, getattr(chat, "title", ""), offset_id
            )
            async for item in scanner.get_chat_history(resolved_channel_id, offset_id=offset_id):
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
            "主频道资源可直接进入目录；备份频道中匹配到的副本会自动补全，取回时优先使用主频道。",
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


DEFAULT_HOME_BUTTONS = [
    {"key": "history", "text": "📜 历史访问", "type": "action", "enabled": True},
    {"key": "folders", "text": "🗂️ 文件夹列表", "type": "action", "enabled": True},
    {"key": "favorites", "text": "⭐ 收藏列表", "type": "action", "enabled": True},
    {"key": "random", "text": "🎲 随机探索", "type": "action", "enabled": True},
]


async def get_home_buttons():
    raw = await db.get_setting("home_buttons")
    buttons = None
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                buttons = [
                    item for item in parsed
                    if isinstance(item, dict) and item.get("key") and item.get("text")
                ]
        except (TypeError, ValueError):
            log.warning("Invalid home_buttons setting; restoring default buttons")
    if buttons is None:
        buttons = [dict(item) for item in DEFAULT_HOME_BUTTONS]
    # Keep every built-in action available after upgrades.
    by_key = {str(item.get("key")): item for item in buttons}
    for default in DEFAULT_HOME_BUTTONS:
        current = by_key.get(default["key"])
        if current is None:
            buttons.append(dict(default))
            continue
        # Random exploration must remain visible and usable; old saved settings
        # may contain a stale disabled entry from an earlier broken menu.
        if default["key"] == "random":
            current["text"] = default["text"]
            current["type"] = "action"
            current["enabled"] = True
    return buttons


async def save_home_buttons(buttons):
    await db.set_setting("home_buttons", json.dumps(buttons, ensure_ascii=False))


async def user_home_keyboard():
    buttons = await get_home_buttons()
    rows, current = [], []
    for item in buttons:
        if not item.get("enabled", True):
            continue
        if item.get("type") == "url":
            button = InlineKeyboardButton(str(item["text"])[:60], url=str(item["url"]))
        else:
            button = InlineKeyboardButton(
                str(item["text"])[:60], callback_data=f"uc:{item['key']}"
            )
        current.append(button)
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    return InlineKeyboardMarkup(rows) if rows else InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 刷新首页", callback_data="uc:home")]
    ])


async def user_home_text(user_id):
    tier = await get_membership_tier(user_id)
    stats = await db.catalog_stats()
    return (
        "🤖 欢迎使用网盘机器人\n\n"
        f"🏅 会员级别：{tier or '未开通'}\n\n"
        "📊 资源统计\n"
        f"• 文件夹：{stats['folders']} 个\n"
        f"• 消息：{stats['messages']} 条\n"
        f"• 图片：{stats['images']} 张\n"
        f"• 视频：{stats['videos']} 个\n\n"
        "请选择下方功能："
    )


async def cache_cangku_backup(client, file_key, source_chat_id, source_message_id):
    try:
        _, backup_channel_id = await get_storage_channels()
        if not backup_channel_id:
            return
        current = await db.get_file(file_key)
        if not current or int(current["backup_message_id"] or 0) > 0:
            return
        copied = await telegram_bot_api(
            "copyMessage",
            {
                "chat_id": backup_channel_id,
                "from_chat_id": source_chat_id,
                "message_id": source_message_id,
            },
        )
        media_size = 0
        for media_name in ("document", "video", "audio", "animation", "voice", "video_note"):
            media = copied.get(media_name)
            if isinstance(media, dict):
                media_size = int(media.get("file_size") or 0)
                break
        if not media_size and isinstance(copied.get("photo"), list) and copied["photo"]:
            media_size = int(copied["photo"][-1].get("file_size") or 0)
        updated = await db.update_backup_message_id(
            file_key, int(copied["message_id"]), media_size
        )
        if updated:
            log.info("Cached Cangku resource %s into backup channel", file_key)
    except Exception:
        log.exception("Could not cache Cangku resource %s into backup channel", file_key)
    finally:
        cangku_backup_in_progress.discard(file_key)


async def send_catalog_file(client, user_id, file_key):
    record = await db.get_file(file_key)
    if not record:
        return False, "文件不存在或已被移除。"

    fingerprint = str(record["source_fingerprint"] or "")
    copied_message_id = None
    if fingerprint.startswith("cangku:"):
        try:
            parts = fingerprint.split(":", 2)
            if len(parts) != 3:
                raise ValueError("原始资源索引格式无效")
            source_chat_id = int(parts[1])
            source_message_id = int(parts[2])
            _, backup_channel_id = await get_storage_channels()
            backup_message_id = int(record["backup_message_id"] or 0)
            if backup_channel_id and backup_message_id > 0:
                try:
                    copied = await telegram_bot_api(
                        "copyMessage",
                        {
                            "chat_id": user_id,
                            "from_chat_id": backup_channel_id,
                            "message_id": backup_message_id,
                        },
                    )
                    copied_message_id = int(copied["message_id"])
                except Exception:
                    log.warning("Cached backup failed for %s; falling back to Cangku", file_key)
            if copied_message_id is None:
                copied = await telegram_bot_api(
                    "copyMessage",
                    {
                        "chat_id": user_id,
                        "from_chat_id": source_chat_id,
                        "message_id": source_message_id,
                    },
                )
                copied_message_id = int(copied["message_id"])
                if file_key not in cangku_backup_in_progress:
                    cangku_backup_in_progress.add(file_key)
                    asyncio.create_task(cache_cangku_backup(
                        client, file_key, source_chat_id, source_message_id
                    ))
        except Exception as exc:
            log.warning("Cangku source delivery failed for %s: %s", file_key, str(exc)[:300])
            return False, "原频道资源暂时无法取回，请联系管理员检查机器人是否仍有原频道访问权限。"
    else:
        primary_channel_id, backup_channel_id = await get_storage_channels()
        if not primary_channel_id:
            return False, "管理员尚未绑定主存储频道。"
        try:
            copied = await client.copy_message(
                chat_id=user_id,
                from_chat_id=primary_channel_id,
                message_id=record["primary_message_id"],
            )
            copied_message_id = copied.id
        except Exception:
            log.warning("Primary copy failed for %s; trying backup", file_key, exc_info=True)
            backup_message_id = int(record["backup_message_id"] or 0)
            if not backup_channel_id or backup_message_id <= 0:
                return False, "主存储文件暂时无法取回，而且尚未建立备份副本。"
            try:
                copied = await client.copy_message(
                    chat_id=user_id,
                    from_chat_id=backup_channel_id,
                    message_id=backup_message_id,
                )
                copied_message_id = copied.id
            except Exception:
                log.exception("Both storage copies failed for %s", file_key)
                return False, "文件暂时无法取回，请联系管理员检查存储频道。"

    await db.record_history(user_id, file_key)
    return True, "文件已发送。"


async def send_catalog_batch(client, user_id, share_key, offset, batch):
    """Copy each contiguous run of files from one source with Telegram copyMessages."""
    sent = failed = 0
    next_offset = offset
    primary_channel_id, backup_channel_id = await get_storage_channels()
    prepared = []

    for item in batch:
        record = await db.get_file(item["file_key"])
        if not record:
            prepared.append((item, None, None, None))
            continue
        fingerprint = str(record["source_fingerprint"] or "")
        if fingerprint.startswith("cangku:"):
            parts = fingerprint.split(":", 2)
            if len(parts) != 3:
                prepared.append((item, record, None, None))
                continue
            source_chat_id = int(parts[1])
            source_message_id = int(parts[2])
            backup_message_id = int(record["backup_message_id"] or 0)
            if backup_channel_id and backup_message_id > 0:
                source = (int(backup_channel_id), backup_message_id)
            else:
                source = (source_chat_id, source_message_id)
        else:
            source = (
                (int(primary_channel_id), int(record["primary_message_id"]))
                if primary_channel_id and record["primary_message_id"] else None
            )
        prepared.append((item, record, source[0] if source else None, source[1] if source else None))

    index = 0
    while index < len(prepared):
        item, record, source_chat_id, source_message_id = prepared[index]
        if not record or source_chat_id is None or source_message_id is None:
            ok, _ = await send_catalog_file(client, user_id, item["file_key"])
            if not ok:
                failed += 1
                break
            sent += 1
            next_offset = offset + index + 1
            await db.set_category_delivery_progress(user_id, share_key, next_offset)
            index += 1
            continue

        end = index + 1
        run_ids = [source_message_id]
        while end < len(prepared):
            next_item, next_record, next_chat_id, next_message_id = prepared[end]
            if not next_record or next_chat_id != source_chat_id or next_message_id is None:
                break
            run_ids.append(next_message_id)
            end += 1

        try:
            if len(run_ids) == 1:
                copied = await telegram_bot_api(
                    "copyMessage",
                    {"chat_id": user_id, "from_chat_id": source_chat_id, "message_id": run_ids[0]},
                )
                copied_messages = [copied]
            else:
                copied_messages = await telegram_bot_api(
                    "copyMessages",
                    {"chat_id": user_id, "from_chat_id": source_chat_id, "message_ids": run_ids},
                )
            if not isinstance(copied_messages, list) or len(copied_messages) != len(run_ids):
                raise RuntimeError("Telegram batch copy returned an unexpected message count")
            for position in range(index, end):
                current_item, current_record, _, _ = prepared[position]
                await db.record_history(user_id, current_item["file_key"])
                if str(current_record["source_fingerprint"] or "").startswith("cangku:"):
                    backup_id = int(current_record["backup_message_id"] or 0)
                    if backup_channel_id and backup_id <= 0:
                        parts = str(current_record["source_fingerprint"]).split(":", 2)
                        if len(parts) == 3 and current_item["file_key"] not in cangku_backup_in_progress:
                            cangku_backup_in_progress.add(current_item["file_key"])
                            asyncio.create_task(cache_cangku_backup(
                                client, current_item["file_key"], int(parts[1]), int(parts[2])
                            ))
                sent += 1
                next_offset = offset + position + 1
                await db.set_category_delivery_progress(user_id, share_key, next_offset)
            index = end
        except Exception as exc:
            log.warning(
                "Batch copy failed; falling back to individual delivery: user=%s source=%s count=%s error=%s",
                user_id, source_chat_id, len(run_ids), str(exc)[:300],
            )
            # Keep the reliable single-file fallback for unavailable source copies.
            for position in range(index, end):
                current_item = prepared[position][0]
                ok, error_text = await send_catalog_file(client, user_id, current_item["file_key"])
                if not ok:
                    failed += 1
                    log.warning(
                        "Folder delivery paused: user=%s share_key=%s offset=%s file=%s reason=%s",
                        user_id, share_key, offset + position, current_item["file_key"], error_text,
                    )
                    break
                sent += 1
                next_offset = offset + position + 1
                await db.set_category_delivery_progress(user_id, share_key, next_offset)
            if failed:
                break
            index = end

    return sent, failed, next_offset


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client, message):
    user = message.from_user
    if not user:
        return
    await db.touch_user(user.id)

    payload = message.command[1] if len(message.command) > 1 else ""
    if payload.startswith("folder_"):
        if not await has_access(user.id):
            await message.reply_text("⛔ 仅限会员群成员使用。请先加入会员群后重试。")
            return
        category_row = await db.get_category_by_share_key(payload[7:])
        if not category_row:
            await message.reply_text("⚠️ 这个文件夹分享链接已失效或不存在。")
            return
        categories = await db.list_categories()
        category_index = next(
            (i for i, row in enumerate(categories) if row["category"] == category_row["name"]),
            None,
        )
        if category_index is None:
            await message.reply_text("⚠️ 文件夹不存在或已被移除。")
            return
        category_row = categories[category_index]
        files = await db.list_category_files(category_row["category"], limit=20, offset=0)
        category_row = dict(category_row)
        category_row["category_index"] = category_index
        await message.reply_text(
            folder_detail_text(category_row, files, 0, await get_member_group_id()),
            reply_markup=folder_detail_keyboard(
                category_row, 0, len(files) == 20,
                await db.is_category_favorite(user.id, category_row["share_key"])
            ),
            disable_web_page_preview=True,
        )
        return
    if payload.startswith("file_"):
        if not await has_access(user.id):
            await message.reply_text("⛔ 仅限会员群成员使用。请先加入会员群后重试。")
            return
        ok, result = await send_catalog_file(client, user.id, payload[5:])
        if not ok:
            await message.reply_text(f"❌ {result}")
        return

    if not await has_access(user.id):
        await message.reply_text("⛔ 本机器人仅对会员开放。请先加入会员群。")
        return
    await message.reply_text(
        await user_home_text(user.id),
        reply_markup=await user_home_keyboard(),
    )


@app.on_message(
    filters.private
    & (filters.document | filters.video | filters.audio | filters.photo |
       filters.animation | filters.voice)
)
async def directory_export_upload_handler(client, message):
    """Receive an admin's Cangku JSON export and prepare a safe preview."""
    user = message.from_user
    if not user:
        return
    if not await is_admin(user.id):
        # Preserve the existing normal-upload flow for non-admin users.
        await upload_handler(client, message)
        return

    # Acknowledge immediately so the admin can see that the document was received.
    status = await message.reply_text("⏳ 已收到文件，正在检查是否为 Cangku 目录迁移 JSON…")
    document = message.document
    if not document or not (document.file_name or "").lower().endswith(".json"):
        try:
            await status.delete()
        except Exception:
            pass
        await upload_handler(client, message)
        return
    local_path = None
    try:
        await status.edit_text("📥 正在从 Telegram 下载 JSON 文件…")
        local_path = await asyncio.wait_for(message.download(), timeout=180)
        await status.edit_text("📄 文件已下载，正在解析目录和资源清单…")
        with open(local_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict) or payload.get("format") != "cangku-directory-export" or payload.get("version") != 1:
            raise ValueError("文件格式或版本不正确，请从 Cangku 使用 /exportdirectories 重新导出。")
        directories = payload.get("directories")
        resources = payload.get("resources")
        if not isinstance(directories, list) or not isinstance(resources, list):
            raise ValueError("目录或资源清单缺失。")
        clean_directories = []
        seen_directories = set()
        for item in directories:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()[:120]
            if not name or name in seen_directories:
                continue
            seen_directories.add(name)
            created_at = None
            raw_created_at = item.get("createdAt")
            try:
                if raw_created_at:
                    timestamp = float(raw_created_at)
                    if timestamp > 100000000000:
                        timestamp /= 1000
                    created_at = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
            except (TypeError, ValueError, OverflowError, OSError):
                created_at = str(raw_created_at)[:64] if raw_created_at else None
            clean_directories.append({
                "name": name,
                "description": str(item.get("description") or "")[:1000],
                "created_at": created_at,
            })
        clean_resources = []
        seen = set()
        invalid = 0
        for item in resources:
            if not isinstance(item, dict):
                invalid += 1
                continue
            chat_id = str(item.get("chatId") or "").strip()
            try:
                message_id = int(item.get("messageId") or 0)
            except (TypeError, ValueError):
                message_id = 0
            if not chat_id or message_id <= 0:
                invalid += 1
                continue
            source_key = f"cangku:{chat_id}:{message_id}"
            if source_key in seen:
                continue
            seen.add(source_key)
            resource_created_at = None
            try:
                resource_date = float(item.get("date") or 0)
                if resource_date > 0:
                    resource_created_at = datetime.fromtimestamp(
                        resource_date, timezone.utc
                    ).isoformat()
            except (TypeError, ValueError, OverflowError, OSError):
                pass
            try:
                file_size = max(0, int(item.get("size") or item.get("fileSize") or item.get("bytes") or 0))
            except (TypeError, ValueError):
                file_size = 0
            clean_resources.append({
                "chat_id": chat_id,
                "message_id": message_id,
                "file_name": str(item.get("title") or "未命名资源")[:200],
                "media_type": str(item.get("fileType") or "document")[:32].lower(),
                "category": str(item.get("category") or "未分类").strip()[:120] or "未分类",
                "source_fingerprint": source_key,
                "file_size": file_size,
                "created_at": resource_created_at,
            })
        if not clean_resources:
            raise ValueError("文件中没有可迁移的媒体资源。")
        # Source-linked directory import does not require destination storage channels.
        await status.edit_text(
            f"🔎 正在检查重复资源…共 {len(clean_resources)} 条，请稍候。"
        )
        fingerprints = [item["source_fingerprint"] for item in clean_resources]
        existing_fingerprints = await db.get_existing_fingerprints(fingerprints)
        existing = len(existing_fingerprints)
        pending_directory_imports[user.id] = {
            "directories": clean_directories,
            "resources": clean_resources,
            "source_exported_at": str(payload.get("exportedAt") or ""),
            "invalid": invalid,
        }
        await status.edit_text(
            "📦 Cangku 目录迁移预览\n\n"
            f"📁 文件夹：{len(clean_directories)} 个\n"
            f"📄 资源清单：{len(clean_resources)} 个\n"
            f"⏭️ 已导入/重复：{existing} 个\n"
            f"⚠️ 无效记录：{invalid} 个\n\n"
            "本次只导入目录和资源索引，不复制文件；原文件继续保留在 Cangku 频道。\n"
            "确认开始请发送：/importconfirm\n"
            "取消本次预览请发送：/importcancel"
        )
    except asyncio.TimeoutError:
        log.error("Cangku directory export download timed out for admin %s", user.id)
        await status.edit_text("❌ 下载 JSON 文件超过 180 秒，已超时。请重新发送文件；如果文件很大，请先压缩或分卷后再导入。")
    except Exception as exc:
        log.exception("Could not prepare Cangku directory import")
        await status.edit_text(f"❌ 无法读取迁移文件：{str(exc)[:500]}")
    finally:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError:
                pass


@app.on_message(filters.private & filters.command("importcancel"))
async def directory_import_cancel_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    pending_directory_imports.pop(user.id, None)
    await message.reply_text("↩️ 已取消本次迁移预览，没有复制或删除任何文件。")


@app.on_message(filters.private & filters.command("importconfirm"))
async def directory_import_confirm_handler(client, message):
    global directory_import_running
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if directory_import_running:
        await message.reply_text("⏳ 当前已有迁移任务运行，请勿重复启动。")
        return
    pending = pending_directory_imports.get(user.id)
    if not pending:
        await message.reply_text("⚠️ 没有待确认的迁移清单。请先把 Cangku 导出的 JSON 文件发送给我。")
        return
    # Directory-only import stores source references and needs no destination channels.
    directory_import_running = True
    status = await message.reply_text("⏳ 正在导入目录和资源索引，不会复制或移动原文件…")
    success = skipped = failed = 0
    try:
        for folder in pending["directories"]:
            await db.save_category(
                folder["name"], folder.get("description", ""), folder.get("created_at")
            )

        total = len(pending["resources"])
        for index, item in enumerate(pending["resources"], 1):
            fingerprint = item["source_fingerprint"]
            try:
                if await db.get_file_by_fingerprint(fingerprint):
                    skipped += 1
                else:
                    file_key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
                    inserted = await db.add_file(
                        file_key, user.id, item["file_name"], item["media_type"],
                        item["message_id"], 0, item["category"], fingerprint,
                        item.get("file_size", 0), item.get("created_at")
                    )
                    if inserted:
                        await db.save_category(item["category"])
                        success += 1
                    else:
                        skipped += 1
            except Exception:
                failed += 1
                log.exception("Directory index import failed: %s", fingerprint)

            if index % 100 == 0 or index == total:
                try:
                    await status.edit_text(
                        f"🔄 索引导入进度：{index}/{total}\n"
                        f"📁 目录：{len(pending['directories'])}\n"
                        f"✅ 已建立索引：{success}  ⏭️ 重复跳过：{skipped}  ❌ 失败：{failed}\n"
                        "原频道文件没有复制、移动或删除。"
                    )
                except Exception:
                    pass
            if index % 100 == 0:
                await asyncio.sleep(0.05)

        if failed == 0:
            pending_directory_imports.pop(user.id, None)
        await status.edit_text(
            "✅ Cangku 目录和资源索引导入完成\n\n"
            f"📁 文件夹：{len(pending['directories'])} 个\n"
            f"✅ 已建立索引：{success} 个\n"
            f"⏭️ 重复跳过：{skipped} 个\n"
            f"❌ 失败：{failed} 个\n\n"
            "会员点击目录即可查看资源，点击资源后从 Cangku 原频道直接取回。\n"
            "原文件未复制、移动或删除。"
            + ("\n失败记录已保留待处理，请重新发送 JSON 后续跑。" if failed else "")
        )
    except Exception:
        log.exception("Directory index import interrupted")
        await status.edit_text(
            f"⚠️ 索引导入中断。已建立：{success}，跳过：{skipped}，失败：{failed}。\n"
            "重新发送同一份 JSON 文件并确认导入即可续跑，已建立的索引会自动跳过。"
        )
    finally:
        directory_import_running = False


async def backup_retry_worker(client):
    """Resume durable backup jobs after restarts, with bounded exponential backoff."""
    while True:
        try:
            jobs = await db.list_due_backup_retries(limit=10)
            for job in jobs:
                file_key = str(job["file_key"])
                try:
                    record = await db.get_file(file_key)
                    if not record or int(record["backup_message_id"] or 0) > 0:
                        await db.complete_backup_retry(file_key)
                        continue
                    copied = await client.copy_message(
                        chat_id=int(job["backup_channel_id"]),
                        from_chat_id=int(job["primary_channel_id"]),
                        message_id=int(job["primary_message_id"]),
                    )
                    updated = await db.update_backup_message_id(file_key, copied.id)
                    if not updated:
                        latest = await db.get_file(file_key)
                        if not latest or int(latest["backup_message_id"] or 0) <= 0:
                            raise RuntimeError("Backup copy succeeded but database update was not confirmed")
                    await db.complete_backup_retry(file_key)
                    log.info(
                        "Durable backup retry completed for file %s (message %s)",
                        file_key, copied.id,
                    )
                except Exception as exc:
                    await db.fail_backup_retry(file_key, f"{type(exc).__name__}: {str(exc)[:400]}")
                    log.exception("Durable backup retry failed for file %s", file_key)
                await asyncio.sleep(0.3)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Backup retry worker cycle failed")
        await asyncio.sleep(15)


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
    if primary_channel_id == backup_channel_id:
        await message.reply_text("❌ 主存储频道和备份频道不能是同一个频道，请管理员检查 /storage。")
        return

    category = parse_category(message)
    status = await message.reply_text("⏳ 正在检查频道权限并保存文件…")
    primary_id = None
    backup_id = 0
    step = "检查主频道权限"
    try:
        me = await client.get_me()
        for label, channel_id in (("主频道", primary_channel_id), ("备份频道", backup_channel_id)):
            member = await client.get_chat_member(channel_id, me.id)
            if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                raise RuntimeError(f"{label}中机器人不是管理员（当前状态：{member.status}）")
            if member.status == ChatMemberStatus.ADMINISTRATOR:
                privileges = getattr(member, "privileges", None)
                if privileges is not None and not getattr(privileges, "can_post_messages", False):
                    raise RuntimeError(f"{label}中机器人没有“发布消息”权限")

        step = "保存主频道"
        primary = await client.copy_message(
            chat_id=primary_channel_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
        primary_id = primary.id

        step = "保存备份频道"
        backup_error = None
        try:
            backup = await client.copy_message(
                chat_id=backup_channel_id,
                from_chat_id=message.chat.id,
                message_id=message.id,
            )
            backup_id = backup.id
        except Exception as exc:
            backup_error = exc
            log.exception("Backup-channel copy failed; keeping primary copy")

        step = "写入文件数据库"
        key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
        media_object = getattr(message, media_type, None)
        file_size = int(getattr(media_object, "file_size", 0) or 0)
        inserted = await db.add_file(
            key, user.id, file_name, media_type,
            primary_id, backup_id, category, source_fingerprint,
            file_size=file_size,
        )
        if not inserted:
            await status.edit_text(f"⏭️ 已跳过重复文件：{file_name}\n数据库中已存在相同文件记录。")
            return

        step = "生成分享链接"
        global BOT_USERNAME
        if not BOT_USERNAME:
            BOT_USERNAME = (await client.get_me()).username
        link = share_url(key)
        if backup_error:
            await status.edit_text(
                "⚠️ 文件已保存到主频道，分享链接已生成；备份频道暂未成功，系统会保留主副本。\n"
                f"📂 分类：{category}\n📄 文件：{file_name}\n\n"
                f"🔗 分享链接：\n{link}\n\n"
                f"备份原因：{type(backup_error).__name__}: {str(backup_error)[:250]}",
                disable_web_page_preview=True,
            )
            await db.enqueue_backup_retry(
                key, primary_channel_id, primary_id, backup_channel_id,
                f"{type(backup_error).__name__}: {str(backup_error)[:400]}",
            )
        else:
            await status.edit_text(
                "✅ 双频道保存成功\n"
                f"📂 分类：{category}\n📄 文件：{file_name}\n\n"
                f"🔗 分享链接：\n{link}",
                disable_web_page_preview=True,
            )
    except Exception as exc:
        log.exception("Upload/store failed at step: %s", step)
        # Never delete a successfully stored channel copy on a later-stage error.
        detail = f"{type(exc).__name__}: {str(exc)[:350]}"
        await status.edit_text(
            f"❌ 文件保存失败（步骤：{step}）\n{detail}\n\n"
            + (f"主频道消息已保存，消息 ID：{primary_id}。请管理员检查数据库/分享链接步骤；未删除该副本。"
               if primary_id is not None else
               "主频道尚未确认保存成功。请管理员检查频道管理员身份、发布消息权限及 Telegram 网络。")
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
        if member.status == ChatMemberStatus.ADMINISTRATOR:
            privileges = getattr(member, "privileges", None)
            if privileges is not None and not getattr(privileges, "can_post_messages", False):
                await message.reply_text(
                    "❌ 机器人虽然是频道管理员，但没有“发布消息”权限。\n"
                    "请在频道管理权限中允许机器人发布消息后，再重新绑定。"
                )
                return
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

    try:
        me = await client.get_me()
        bot_member = await telegram_bot_api(
            "getChatMember",
            {"chat_id": message.chat.id, "user_id": me.id},
        )
    except Exception as exc:
        log.error(
            "Could not verify bot membership in group %s: %s",
            message.chat.id, str(exc)[:400],
        )
        await message.reply_text(
            "❌ 无法通过 Telegram Bot API 读取机器人在本群的状态："
            f"{str(exc)[:250]}"
        )
        return

    if bot_member.get("status") not in ("administrator", "creator"):
        await message.reply_text(
            "❌ 机器人当前不是本群管理员，不能可靠验证其他群成员。\n\n"
            "请先把机器人设为管理员（不需要授予额外管理权限），再重新发送 /bindgroup。"
        )
        return

    await db.set_setting("member_group_id", str(message.chat.id))
    pending_channel_bindings.pop(user.id, None)
    await message.reply_text(
        "✅ 会员群绑定成功！\n"
        f"群组：{message.chat.title or '未命名'}\n"
        f"群 ID：{message.chat.id}\n\n"
        "已通过 Telegram Bot API 确认机器人管理员身份。\n"
        "会员资格将直接按用户数字 ID 实时验证。"
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
    value = (message.text or "").strip()
    if not value:
        return

    pending_home_action = pending_home_button_actions.get(user.id)
    if pending_home_action:
        if pending_home_action["action"] == "add":
            parts = value.split("|", 1)
            if len(parts) != 2:
                await message.reply_text("格式不正确，请按：按钮名称|https://网址 发送。")
                return
            label, url = parts[0].strip()[:60], parts[1].strip()
            if not label or not (url.startswith("https://") or url.startswith("http://")):
                await message.reply_text("按钮名称不能为空，链接必须以 http:// 或 https:// 开头，请重新发送。")
                return
            buttons = await get_home_buttons()
            buttons.append({
                "key": "link_" + secrets.token_hex(4),
                "text": label,
                "type": "url",
                "url": url,
                "enabled": True,
            })
            await save_home_buttons(buttons)
            pending_home_button_actions.pop(user.id, None)
            await message.reply_text("✅ 首页链接按钮已添加。发送 /admin 可继续管理。")
        elif pending_home_action["action"] == "rename":
            buttons = await get_home_buttons()
            target = next((item for item in buttons if item.get("key") == pending_home_action["key"]), None)
            if not target:
                pending_home_button_actions.pop(user.id, None)
                await message.reply_text("⚠️ 按钮不存在，请重新打开首页按钮设置。")
                return
            target["text"] = value[:60]
            await save_home_buttons(buttons)
            pending_home_button_actions.pop(user.id, None)
            await message.reply_text("✅ 首页按钮名称已修改。发送 /admin 可继续管理。")
        return

    pending_category = pending_category_actions.get(user.id)
    if pending_category:
        if pending_category["action"] == "create":
            await db.save_category(value[:120])
            pending_category_actions.pop(user.id, None)
            await message.reply_text(
                f"✅ 已创建目录：{value[:120]}\n空目录会立即显示在会员文件夹列表中。"
            )
        elif pending_category["action"] == "rename":
            old_name = pending_category["name"]
            if await db.rename_category(old_name, value[:120]):
                pending_category_actions.pop(user.id, None)
                await message.reply_text(f"✅ 目录已重命名：{old_name} → {value[:120]}")
            else:
                await message.reply_text("❌ 重命名失败：新名称可能已存在，或名称无效。请重新发送新的名称。")
        return

    if not state or not login_client:
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


async def resolve_scanner_chat(channel_id):
    """Resolve a channel like Cangku's scanner: direct lookup first, then scan all dialogs."""
    if scanner is None or not scanner.is_connected:
        raise RuntimeError("扫描账号尚未连接")

    wanted = int(channel_id)
    direct_error = None

    # First try the normal peer lookup, matching Cangku's getInputEntity-first flow.
    try:
        chat = await scanner.get_chat(wanted)
        if int(getattr(chat, "id", 0)) == wanted:
            log.info("Scanner channel resolved directly: id=%s title=%s", wanted, getattr(chat, "title", ""))
            return chat
    except Exception as exc:
        direct_error = f"{type(exc).__name__}: {exc}"
        log.info("Direct scanner channel lookup failed for %s; searching dialogs: %s", wanted, direct_error)

    # A private channel's numeric ID is not sufficient without its access_hash.
    # get_dialogs() hydrates Pyrogram's peer cache; retain the resolved Chat object.
    dialog_count = 0
    async for dialog in scanner.get_dialogs(limit=0):
        dialog_count += 1
        chat = getattr(dialog, "chat", None) or dialog
        candidate_id = getattr(chat, "id", None)
        if candidate_id is None:
            candidate_id = getattr(dialog, "id", None)
        try:
            matched = int(candidate_id) == wanted
        except (TypeError, ValueError):
            matched = False
        if matched:
            log.info(
                "Scanner channel resolved from dialogs: id=%s title=%s dialogs=%s",
                wanted, getattr(chat, "title", ""), dialog_count
            )
            # A second lookup now uses the peer information loaded from the dialog.
            try:
                return await scanner.get_chat(chat.id)
            except Exception:
                return chat

    raise LookupError(
        f"扫描账号的对话列表中没有频道 {wanted}（已检查 {dialog_count} 个对话）。"
        "请确认当前登录的普通账号确实加入了该频道，并且没有被移出或封禁。"
        + (f" 直接查询错误：{direct_error}" if direct_error else "")
    )


async def inspect_scan_channel(channel_id):
    """Resolve the channel entity first, then verify membership and history access."""
    result = {
        "channel_id": channel_id,
        "title": str(channel_id),
        "chat_type": "未知",
        "member_status": "无法确认",
        "history_ok": False,
        "ok": False,
        "error": None,
        "member_warning": None,
    }
    try:
        chat = await resolve_scanner_chat(channel_id)
        result["title"] = getattr(chat, "title", None) or getattr(chat, "first_name", None) or str(channel_id)
        result["chat_type"] = str(getattr(chat, "type", "未知"))
    except Exception as exc:
        result["error"] = f"解析频道失败：{type(exc).__name__}: {exc}"
        return result

    try:
        me = await scanner.get_me()
        member = await scanner.get_chat_member(chat.id, me.id)
        status = getattr(member, "status", None)
        result["member_status"] = getattr(status, "value", None) or str(status or "未知")
        if result["member_status"].lower() in ("left", "kicked", "banned"):
            result["error"] = f"扫描账号当前状态为 {result['member_status']}，不在频道内"
            return result
    except Exception as exc:
        result["member_warning"] = f"{type(exc).__name__}: {exc}"

    try:
        async for _ in scanner.get_chat_history(chat.id, limit=1):
            break
        result["history_ok"] = True
        result["ok"] = True
    except Exception as exc:
        result["error"] = f"读取历史消息失败：{type(exc).__name__}: {exc}"
    return result


def format_scan_check(result, label):
    status = "✅ 可扫描" if result["ok"] else "❌ 不可扫描"
    lines = [
        f"{label}：{status}",
        f"频道：{result['title']}",
        f"ID：{result['channel_id']}",
        f"类型：{result['chat_type']}",
        f"扫描账号状态：{result['member_status']}",
    ]
    if result["error"]:
        lines.append(f"原因：{result['error']}")
    if result["member_warning"]:
        lines.append(f"成员状态查询提示：{result['member_warning']}")
    return "\n".join(lines)


@app.on_message(filters.private & filters.command("build"))
async def build_info_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    scanner_id = None
    if scanner is not None and scanner.is_connected:
        try:
            scanner_me = await scanner.get_me()
            scanner_id = scanner_me.id
        except Exception as exc:
            scanner_id = f"读取失败：{type(exc).__name__}: {exc}"
    await message.reply_text(
        "🧩 机器人运行版本\n"
        "会员验证版本：peer-warmup-44e31a3\n"
        f"扫描账号：{scanner_id if scanner_id is not None else '未连接'}\n"
        f"运行文件：{os.path.abspath(__file__)}"
    )


@app.on_message(filters.private & filters.command("scancheck"))
async def scan_check_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scanner is None or not scanner.is_connected:
        await message.reply_text("⚠️ 扫描账号尚未登录或连接已断开。请先发送 /scanlogin 登录。")
        return

    primary, backup = await get_storage_channels()
    if not primary or not backup:
        await message.reply_text("❌ 主频道或备份频道尚未绑定，请先检查 /storage。")
        return

    await message.reply_text("🔎 正在分别检查主频道、备份频道及历史读取权限……")
    primary_result = await inspect_scan_channel(primary)
    backup_result = await inspect_scan_channel(backup)
    body = (
        "🔎 扫描账号频道检查结果\n\n"
        + format_scan_check(primary_result, "主存储频道")
        + "\n\n"
        + format_scan_check(backup_result, "备份存储频道")
    )
    if primary_result["ok"] and backup_result["ok"]:
        body += "\n\n✅ 两个频道均可读取历史消息，可以发送 /scan 开始扫描。"
    else:
        body += "\n\n⚠️ 请根据上面的具体错误修复频道权限后，再运行 /scan。"
    await message.reply_text(body)


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
    if not scanner.is_connected:
        await message.reply_text("⚠️ 扫描账号连接已断开。请发送 /scanlogin 重新登录。")
        return
    checks = [
        ("主存储频道", await inspect_scan_channel(primary)),
        ("备份存储频道", await inspect_scan_channel(backup)),
    ]
    failed = [format_scan_check(result, label) for label, result in checks if not result["ok"]]
    if failed:
        log.error("Scanner channel check failed: %s", " | ".join(failed))
        await message.reply_text(
            "❌ 扫描未启动，以下频道检查未通过：\n\n"
            + "\n\n".join(failed)
            + "\n\n修复对应频道权限后，先运行 /scancheck 确认，再发送 /scan。"
        )
        return
    p = await db.get_scan_progress(primary)
    b = await db.get_scan_progress(backup)
    backfilled = await db.publish_primary_catalog(primary, config.ADMIN_ID)
    if p["completed"] and b["completed"]:
        await message.reply_text(
            "✅ 两个频道的历史扫描已完成。\n"
            f"本次从已保存的主频道扫描记录补入目录：{backfilled} 个文件。\n"
            "已有文件和扫描进度均已保留。"
        )
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
        "Cangku 整体目录迁移：先在 Cangku 管理员私聊发送 /exportdirectories，收到 JSON 后把文件发送给本机器人，预览后发送 /importconfirm 开始；发送 /importcancel 可取消。\n\n"
        "也可以继续使用原有方式，从其他仓库群批量转发文件。\n\n"
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


@app.on_message(filters.private & filters.command("directories"))
async def admin_directories_command(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    categories = await db.list_categories()
    rows = [[InlineKeyboardButton("➕ 新建目录", callback_data="mc:dirnew")]]
    for index, row in enumerate(categories[:20]):
        rows.append([InlineKeyboardButton(
            f"📁 {row['category'][:36]}（{int(row['file_count'] or 0)}）",
            callback_data=f"mc:directory:{index}",
        )])
    if len(categories) > 20:
        rows.append([InlineKeyboardButton("下一页 ➡️", callback_data="mc:directories:1")])
    rows.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
    await message.reply_text(
        f"📁 目录管理（共 {len(categories)} 个）\\n\\n"
        "可以新建空目录、修改目录名称；只有空目录可以删除。",
        reply_markup=InlineKeyboardMarkup(rows),
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


@app.on_callback_query(filters.regex(r"^uc:"))
async def user_catalog_callback_handler(client, query):
    user = query.from_user
    if not user or not await has_access(user.id):
        await query.answer("仅限会员群成员使用", show_alert=True)
        return
    action = query.data.split(":")
    message = query.message
    if not message:
        return

    if action[1] == "home":
        await query.answer()
        await message.edit_text(
            await user_home_text(user.id),
            reply_markup=await user_home_keyboard(),
        )
        return

    if action[1] == "random":
        # Acknowledge the button immediately; database and Telegram delivery
        # must not leave the user's callback spinner hanging.
        await query.answer("🎲 正在为你随机发送最多 20 个文件")
        try:
            if not await has_access(user.id):
                await message.reply_text("⛔ 仅限会员群成员使用。")
                return
            _, _, total = await db.public_catalog(limit=1)
            if not total:
                await message.reply_text("📭 资源库暂时没有可随机发送的文件。")
                return

            batch_size = min(20, int(total))
            start = secrets.randbelow(int(total))
            rows, _, _ = await db.public_catalog(
                limit=min(batch_size, int(total) - start), offset=start
            )
            if len(rows) < batch_size:
                extra, _, _ = await db.public_catalog(
                    limit=batch_size - len(rows), offset=0
                )
                rows.extend(extra)

            if not rows:
                await message.reply_text("📭 暂时没有找到可发送的资源，请稍后重试。")
                return

            sent = 0
            failed = []
            for item in rows:
                try:
                    ok, result = await send_catalog_file(
                        client, user.id, item["file_key"]
                    )
                    if ok:
                        sent += 1
                    else:
                        failed.append(f"{item['file_name'][:40]}：{result}")
                except Exception as exc:
                    log.exception(
                        "Random delivery failed: user=%s file=%s",
                        user.id, item["file_key"],
                    )
                    failed.append(
                        f"{item['file_name'][:40]}：{type(exc).__name__}"
                    )

            summary = f"🎲 随机发送完成：成功 {sent}/{len(rows)} 个文件。"
            if failed:
                summary += "\n\n失败 " + str(len(failed)) + " 个：\n"
                summary += "\n".join(failed[:5])
            await message.reply_text(summary)
        except Exception:
            log.exception("Random exploration failed for user %s", user.id)
            await message.reply_text(
                "❌ 随机发送失败，请稍后重试；管理员可检查机器人运行日志。"
            )
        return

    if action[1] == "folders":
        await query.answer()
        page = int(action[2]) if len(action) > 2 else 0
        categories = await db.list_categories()
        start_at = max(0, page) * 20
        selected = categories[start_at:start_at + 20]
        rows = [
            [InlineKeyboardButton(
                f"🗂 {row['category']}（{row['file_count']}）",
                callback_data=f"uc:folder:{start_at + index}:0",
            )]
            for index, row in enumerate(selected)
        ]
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"uc:folders:{page - 1}"))
        if start_at + 20 < len(categories):
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"uc:folders:{page + 1}"))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton("⬅️ 返回首页", callback_data="uc:home")])
        await message.edit_text(
            f"🗂️ 文件夹列表（{len(categories)} 个， 第 {page + 1} 页）\n\n选择分类查看文件："
            if categories else "🗂️ 暂无文件夹分类。",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if action[1] == "folder" and len(action) >= 3:
        try:
            index = int(action[2])
            offset = max(0, int(action[3])) if len(action) > 3 else 0
            categories = await db.list_categories()
            category_row = categories[index]
            category = category_row["category"]
        except (ValueError, IndexError):
            await query.answer("分类已更新，请重新打开列表", show_alert=True)
            return
        await query.answer()
        files = await db.list_category_files(category, limit=20, offset=offset)
        page = offset // 20
        group_id = await get_member_group_id()
        category_row = dict(category_row)
        category_row["category_index"] = index
        await message.edit_text(
            folder_detail_text(category_row, files, page, group_id),
            reply_markup=folder_detail_keyboard(
                category_row, page, len(files) == 20,
                await db.is_category_favorite(user.id, category_row["share_key"])
            ),
            disable_web_page_preview=True,
        )
        return

    if action[1] in ("history", "favorites"):
        await query.answer()
        if action[1] == "history":
            records = await db.list_user_history(user.id, 12)
            title = "📜 历史访问"
            rows = [[InlineKeyboardButton(
                f"📄 {row['file_name'][:32]}",
                callback_data=f"uc:open:{row['file_key']}",
            )] for row in records]
            body = title + "\n\n" + (
                "\n".join(f"• {row['file_name'][:48]}（{row['category']}）" for row in records)
                if records else "这里暂时没有记录。"
            )
        else:
            records = await db.list_favorite_categories(user.id, 30)
            title = "⭐ 已收藏的文件夹"
            rows = [[InlineKeyboardButton(
                f"📁 {row['name'][:32]}",
                callback_data=f"uc:folderlink:{row['share_key']}",
            )] for row in records]
            body = title + "\n\n" + (
                "\n".join(f"• {row['name']}" for row in records)
                if records else "你还没有收藏文件夹。"
            )
        rows.append([InlineKeyboardButton("⬅️ 返回首页", callback_data="uc:home")])
        await message.edit_text(body, reply_markup=InlineKeyboardMarkup(rows))
        return

    if action[1] == "folderstar" and len(action) == 3:
        share_key = action[2]
        category = await db.get_category_by_share_key(share_key)
        if not category:
            await query.answer("文件夹不存在", show_alert=True)
            return
        added = await db.toggle_category_favorite(user.id, share_key)
        await query.answer("⭐ 已收藏文件夹" if added else "已取消收藏")
        categories = await db.list_categories()
        index = next((i for i, row in enumerate(categories) if row["share_key"] == share_key), None)
        if index is not None:
            row = dict(categories[index])
            row["category_index"] = index
            files = await db.list_category_files(row["category"], limit=20, offset=0)
            await message.edit_text(
                folder_detail_text(row, files, 0, await get_member_group_id()),
                reply_markup=folder_detail_keyboard(row, 0, len(files) == 20, added),
                disable_web_page_preview=True,
            )
        return

    if action[1] == "folderlink" and len(action) == 3:
        category = await db.get_category_by_share_key(action[2])
        if not category:
            await query.answer("文件夹不存在", show_alert=True)
            return
        categories = await db.list_categories()
        index = next((i for i, row in enumerate(categories) if row["share_key"] == action[2]), None)
        if index is None:
            await query.answer("文件夹不存在", show_alert=True)
            return
        row = dict(categories[index])
        row["category_index"] = index
        files = await db.list_category_files(row["category"], limit=20, offset=0)
        await query.answer()
        await message.edit_text(
            folder_detail_text(row, files, 0, await get_member_group_id()),
            reply_markup=folder_detail_keyboard(
                row, 0, len(files) == 20,
                await db.is_category_favorite(user.id, action[2])
            ),
            disable_web_page_preview=True,
        )
        return

    if action[1] == "getfolder" and len(action) == 4:
        share_key = action[2]
        try:
            offset = max(0, int(action[3]))
        except ValueError:
            await query.answer("批次参数无效", show_alert=True)
            return
        category = await db.get_category_by_share_key(share_key)
        if not category:
            await query.answer("文件夹不存在", show_alert=True)
            return
        delivery_key = (int(user.id), str(share_key))
        if delivery_key in category_delivery_in_progress:
            await query.answer("这一批资源正在发送，请稍候，不要重复点击。", show_alert=True)
            return
        category_delivery_in_progress.add(delivery_key)
        try:
            total = next((int(row["file_count"] or 0) for row in await db.list_categories()
                          if row["share_key"] == share_key), 0)
            saved_offset = await db.get_category_delivery_progress(user.id, share_key)
            # A stale button must never make a user receive already-delivered batches again.
            offset = max(offset, saved_offset)
            batch = await db.list_category_files(category["name"], limit=20, offset=offset)
            if not batch:
                await query.answer("这个目录的资源已经领取完毕", show_alert=True)
                return
            await query.answer("开始批量发送，最多20个文件")
            progress = await message.reply_text(
                f"📥 正在批量获取「{category['name']}」第 {offset // 20 + 1} 组资源（最多20个）…"
            )
            sent, failed, next_offset = await send_catalog_batch(
                client, user.id, share_key, offset, batch
            )
            text_result = (
                f"📦 本组发送完成\n\n成功：{sent} 个\n失败：{failed} 个\n"
                f"已记录领取进度：{next_offset}/{total}"
            )
            buttons = []
            if next_offset < total:
                buttons.append([InlineKeyboardButton(
                    "📥 获取下一组（最多20个）" if failed == 0 else "🔄 重试未发送文件",
                    callback_data=f"uc:getfolder:{share_key}:{next_offset}",
                )])
            buttons.append([InlineKeyboardButton("📁 返回文件夹", callback_data=f"uc:folderlink:{share_key}")])
            await progress.edit_text(text_result, reply_markup=InlineKeyboardMarkup(buttons))
            return
        finally:
            category_delivery_in_progress.discard(delivery_key)

    if action[1] == "open" and len(action) == 3:
        ok, result = await send_catalog_file(client, user.id, action[2])
        await query.answer(result if ok else result, show_alert=not ok)
        return

    if action[1] == "star" and len(action) >= 3:
        record = await db.get_file(action[2])
        if not record:
            await query.answer("文件不存在或已被移除", show_alert=True)
            return
        added = await db.toggle_favorite(user.id, action[2])
        await query.answer("⭐ 已加入收藏" if added else "已取消收藏")
        if len(action) > 3 and action[3] == "favorites":
            records = await db.list_favorites(user.id, 12)
            rows = []
            for row in records:
                rows.append([InlineKeyboardButton(
                    f"📄 {row['file_name'][:32]}",
                    callback_data=f"uc:open:{row['file_key']}",
                )])
                rows.append([InlineKeyboardButton(
                    "⭐ 取消收藏",
                    callback_data=f"uc:star:{row['file_key']}:favorites",
                )])
            rows.append([InlineKeyboardButton("⬅️ 返回首页", callback_data="uc:home")])
            await message.edit_text(
                "⭐ 收藏列表\n\n" + (
                    "\n".join(f"• {row['file_name'][:48]}（{row['category']}）" for row in records)
                    if records else "这里暂时没有收藏。"
                ),
                reply_markup=InlineKeyboardMarkup(rows),
            )
        return


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
    elif action == "directories" or action.startswith("directories:"):
        categories = await db.list_categories()
        page = int(action.split(":", 1)[1]) if ":" in action else 0
        page = max(0, page)
        start_at = page * 20
        selected = categories[start_at:start_at + 20]
        rows = [[InlineKeyboardButton("➕ 新建目录", callback_data="mc:dirnew")]]
        for index, row in enumerate(selected, start_at):
            rows.append([InlineKeyboardButton(
                f"📁 {row['category'][:36]}（{int(row['file_count'] or 0)}）",
                callback_data=f"mc:directory:{index}",
            )])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"mc:directories:{page - 1}"))
        if start_at + 20 < len(categories):
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"mc:directories:{page + 1}"))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
        await message.edit_text(
            f"📁 目录管理（共 {len(categories)} 个，第 {page + 1} 页）\n\n"
            "空目录也会立即显示给会员。选择目录可修改名称；只有空目录可以删除。",
            reply_markup=InlineKeyboardMarkup(rows),
        )
    elif action == "dirnew":
        pending_category_actions[user.id] = {"action": "create"}
        await message.edit_text(
            "➕ 新建目录\n\n请直接发送目录名称（最多 120 个字符）。\n"
            "创建后空目录会立即显示给会员。",
            reply_markup=back_keyboard(),
        )
    elif action.startswith("directory:"):
        try:
            index = int(action.split(":", 1)[1])
            categories = await db.list_categories()
            row = categories[index]
        except (ValueError, IndexError):
            await message.edit_text("⚠️ 目录列表已更新，请重新打开目录管理。", reply_markup=back_keyboard())
            return
        count = int(row["file_count"] or 0)
        buttons = [
            [InlineKeyboardButton("✏️ 修改目录名称", callback_data=f"mc:dirrename:{index}")],
        ]
        if count == 0:
            buttons.append([InlineKeyboardButton("🗑 删除空目录", callback_data=f"mc:dirdelete:{index}")])
        buttons.append([InlineKeyboardButton("⬅️ 返回目录管理", callback_data="mc:directories")])
        await message.edit_text(
            f"📁 目录：{row['category']}\n📦 文件数：{count}\n"
            f"🔗 分享链接：{folder_share_url(str(row['share_key'])) if row['share_key'] and BOT_USERNAME else '机器人启动后可用'}\n\n"
            + ("这是空目录，可以删除。" if count == 0 else "目录含有资源，只能修改名称，不能删除资源。"),
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True,
        )
    elif action.startswith("dirrename:"):
        try:
            index = int(action.split(":", 1)[1])
            row = (await db.list_categories())[index]
        except (ValueError, IndexError):
            await message.edit_text("⚠️ 目录列表已更新，请重新打开目录管理。", reply_markup=back_keyboard())
            return
        pending_category_actions[user.id] = {"action": "rename", "name": row["category"]}
        await message.edit_text(
            f"✏️ 修改目录名称\n\n当前名称：{row['category']}\n请发送新的目录名称。\n"
            "目录内的资源会同步更名，不会删除或移动文件。",
            reply_markup=back_keyboard(),
        )
    elif action.startswith("dirdelete:"):
        try:
            index = int(action.split(":", 1)[1])
            row = (await db.list_categories())[index]
        except (ValueError, IndexError):
            await message.edit_text("⚠️ 目录列表已更新，请重新打开目录管理。", reply_markup=back_keyboard())
            return
        if await db.delete_empty_category(row["category"]):
            await message.edit_text(f"✅ 已删除空目录：{row['category']}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ 返回目录管理", callback_data="mc:directories")]
            ]))
        else:
            await message.edit_text("❌ 删除失败：目录可能已有文件，系统不会删除或影响其中资源。", reply_markup=back_keyboard())
    elif action == "homebuttons":
        buttons = await get_home_buttons()
        rows = []
        for item in buttons:
            enabled = item.get("enabled", True)
            status = "✅" if enabled else "⏸"
            rows.append([
                InlineKeyboardButton(
                    f"{status} {str(item['text'])[:32]}",
                    callback_data=f"mc:homebutton:toggle:{item['key']}",
                ),
                InlineKeyboardButton(
                    "✏️ 改名" if item.get("type") == "action" else "🗑 删除",
                    callback_data=(
                        f"mc:homebutton:rename:{item['key']}"
                        if item.get("type") == "action"
                        else f"mc:homebutton:delete:{item['key']}"
                    ),
                ),
            ])
        rows.append([InlineKeyboardButton("➕ 添加网址按钮", callback_data="mc:homebutton:add")])
        rows.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
        await message.edit_text(
            "🔘 用户首页按钮设置\n\n"
            "这里控制用户发送 /start 后看到的内联按钮。\n"
            "点击左侧可启用/停用；右侧可修改内置按钮名称或删除自定义网址按钮。\n"
            "随机探索等原有功能保留，停用只会隐藏按钮，不会删除功能。",
            reply_markup=InlineKeyboardMarkup(rows),
        )
    elif action.startswith("homebutton:"):
        parts = action.split(":")
        if len(parts) < 3:
            return
        operation, key = parts[1], parts[2]
        buttons = await get_home_buttons()
        target = next((item for item in buttons if str(item.get("key")) == key), None)
        if operation == "toggle" and target:
            target["enabled"] = not target.get("enabled", True)
            await save_home_buttons(buttons)
            rows = []
            for item in buttons:
                status = "✅" if item.get("enabled", True) else "⏸"
                rows.append([
                    InlineKeyboardButton(f"{status} {str(item['text'])[:32]}", callback_data=f"mc:homebutton:toggle:{item['key']}"),
                    InlineKeyboardButton(
                        "✏️ 改名" if item.get("type") == "action" else "🗑 删除",
                        callback_data=f"mc:homebutton:{'rename' if item.get('type') == 'action' else 'delete'}:{item['key']}",
                    ),
                ])
            rows.append([InlineKeyboardButton("➕ 添加网址按钮", callback_data="mc:homebutton:add")])
            rows.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
            await message.edit_text("🔘 用户首页按钮设置\n\n点击左侧切换显示状态，右侧修改名称或删除网址按钮。", reply_markup=InlineKeyboardMarkup(rows))
        elif operation == "rename" and target:
            pending_home_button_actions[user.id] = {"action": "rename", "key": key}
            await message.edit_text(f"✏️ 修改按钮名称\n\n当前名称：{target['text']}\n请直接发送新的按钮名称。", reply_markup=back_keyboard())
        elif operation == "add":
            pending_home_button_actions[user.id] = {"action": "add"}
            await message.edit_text("➕ 添加首页网址按钮\n\n请按以下格式发送：\n按钮名称|https://网址\n\n例如：联系客服|https://t.me/example", reply_markup=back_keyboard())
        elif operation == "delete" and target and target.get("type") == "url":
            buttons = [item for item in buttons if item.get("key") != key]
            await save_home_buttons(buttons)
            rows = []
            for item in buttons:
                status = "✅" if item.get("enabled", True) else "⏸"
                rows.append([
                    InlineKeyboardButton(f"{status} {str(item['text'])[:32]}", callback_data=f"mc:homebutton:toggle:{item['key']}"),
                    InlineKeyboardButton(
                        "✏️ 改名" if item.get("type") == "action" else "🗑 删除",
                        callback_data=f"mc:homebutton:{'rename' if item.get('type') == 'action' else 'delete'}:{item['key']}",
                    ),
                ])
            rows.append([InlineKeyboardButton("➕ 添加网址按钮", callback_data="mc:homebutton:add")])
            rows.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
            await message.edit_text("✅ 已删除网址按钮。\n\n🔘 用户首页按钮设置", reply_markup=InlineKeyboardMarkup(rows))
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
            "扫描进度保存在数据库中；登录会话保存在本地文件。若部署平台没有持久化文件存储，重新部署后可能需要重新登录扫描账号。",
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
            "/bindgroup — 绑定会员群\n/membercheck [用户ID] — 检查会员群和成员权限\n/bindmain — 绑定主存储频道\n/bindbackup — 绑定备份频道\n/storage — 查看绑定状态\n"
            "/grant 用户ID 天数|永久 — 授权会员\n/revoke 用户ID — 取消授权\n/ban 用户ID 原因 — 封禁\n/unban 用户ID — 解封\n"
            "/members — 会员列表\n/users — 用户列表\n/files — 文件列表\n/import — 批量转发导入旧文件\n/scanlogin — 在机器人内登录扫描账号\n/scancancel — 取消扫描账号登录\n/scanlogout — 退出/更换扫描账号\n/scancheck — 分别检查主备份频道访问和历史读取权限\n/scan — 自动扫描主备份频道历史\n/scanstatus — 查看扫描进度\n/scanstop — 暂停扫描\n/category 文件码 分类 — 修改分类\n/share 文件码1 文件码2 — 批量分享\n/broadcast 内容 — 广播",
            reply_markup=back_keyboard(),
        )


@app.on_message(filters.private & filters.command("membercheck"))
async def membercheck_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return

    group_id = await get_member_group_id()
    if not group_id:
        await message.reply_text(
            "❌ 尚未绑定会员群。\n"
            "请把机器人添加到会员群并设为管理员，然后在群内发送 /bindgroup。"
        )
        return

    try:
        group = await telegram_bot_api("getChat", {"chat_id": group_id})
        me = await client.get_me()
        bot_member = await telegram_bot_api(
            "getChatMember",
            {"chat_id": group_id, "user_id": me.id},
        )
        bot_status = str(bot_member.get("status", "unknown"))
        lines = [
            "🔎 会员群权限诊断（Telegram Bot API）",
            f"群名称：{group.get('title') or '未命名'}",
            f"群 ID：{group_id}",
            f"机器人状态：{bot_status}",
        ]
        if bot_status not in ("administrator", "creator"):
            lines.append("❌ 机器人不是群管理员，无法可靠查询普通成员。")
            await message.reply_text("\n".join(lines))
            return

        if len(message.command) > 1:
            target_id = int(message.command[1])
            target = await telegram_bot_api(
                "getChatMember",
                {"chat_id": group_id, "user_id": target_id},
            )
            target_status = str(target.get("status", "unknown"))
            lines.append(f"用户 {target_id} 状态：{target_status}")
            if target_status in ("member", "administrator", "creator") or (
                target_status == "restricted" and target.get("is_member") is True
            ):
                lines.append("✅ Bot API 确认该用户是有效群成员，应获得黄金会员权限。")
            else:
                lines.append("❌ Bot API 返回该用户不是有效群成员。")
        else:
            lines.append("✅ 机器人管理员权限正常。")
            lines.append("可追加用户数字 ID 检查：/membercheck 用户ID")
        await message.reply_text("\n".join(lines))
    except ValueError:
        await message.reply_text("用法：/membercheck 用户数字ID")
    except Exception as exc:
        log.error(
            "Member group diagnostic failed for group %s: %s",
            group_id, str(exc)[:400],
        )
        await message.reply_text(
            "❌ 会员群诊断失败："
            f"{type(exc).__name__}: {str(exc)[:300]}"
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
    global BOT_USERNAME, BOT_ID
    await db.initialize()
    log.info("Database backend: %s", "PostgreSQL" if db.uses_postgres else "SQLite (local fallback)")
    await app.start()
    scanner_started = False
    runner = None
    backup_retry_task = None
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
        BOT_ID = me.id
        log.info("Bot started: @%s", me.username)

        backup_retry_task = asyncio.create_task(backup_retry_worker(app))
        log.info("Durable backup retry worker started")

        runner = web.AppRunner(create_catalog_app(db, lambda: BOT_USERNAME))
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", config.WEB_PORT)
        await site.start()
        log.info("Public catalog listening on port %s", config.WEB_PORT)

        await asyncio.Event().wait()
    finally:
        if backup_retry_task is not None:
            backup_retry_task.cancel()
            try:
                await backup_retry_task
            except asyncio.CancelledError:
                pass
        if runner is not None:
            await runner.cleanup()
        if scanner is not None and scanner.is_connected:
            await scanner.stop()
        await app.stop()


if __name__ == "__main__":
    app.run(main())
