import asyncio
import base64
import io
import json
import aiohttp
import logging
import os
import secrets
import sqlite3
import time
from datetime import datetime, timezone, timedelta

from aiohttp import web

from pyrogram import Client, filters, StopPropagation, raw
from pyrogram.session import Auth, Session
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    KeyboardButton, ReplyKeyboardMarkup, BotCommand,
)
from pyrogram.enums import ChatAction, ChatMemberStatus, ChatType
from pyrogram.errors import (
    FloodWait,
)

import qrcode

import config
from alerts import AlertThrottle
from captcha_image import stored_answer_index
from newcomer import DEFAULT_HOURS, clamp_hours, window_active
from upload_report import add_failure, render_failures
from database import Database, clean_file_name
from web_catalog import create_catalog_app

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("member-cloud")
db = Database(config.DATABASE_PATH, database_url=config.DATABASE_URL)
BOT_USERNAME = None
BOT_ID = None
telegram_http_session = None
pending_channel_bindings = {}
membership_cache = {}
# Browsing re-verifies membership at most once per hour; every claim path still
# asks Telegram directly. A denial expires fast so someone who just joined is
# not locked out for the rest of the hour.
MEMBERSHIP_TTL_SECONDS = 3600
MEMBERSHIP_DENIED_TTL_SECONDS = 60
# Throwaway per-user settings (page cursors, CAPTCHA state) are purged after
# three days; audit and delivery history are never touched by this sweep.
SETTING_RETENTION_SECONDS = 3 * 24 * 3600
SETTING_CLEANUP_INTERVAL = 6 * 3600
TEMP_SETTING_PREFIXES = (
    "captcha_session:", "captcha_challenge:", "captcha_block:", "captcha_notice:",
    "folder_view:", "folder_list_page:", "folder_list_sort:",
)
category_catalog_cache = {"expires_at": 0.0, "rows": []}
scanner_login_states = {}
scanner_login_clients = {}
pending_directory_imports = {}
pending_category_actions = {}
pending_upload_categories = {}
upload_status_messages = {}
pending_album_uploads = {}
pending_upload_tasks = {}
upload_finish_in_progress = set()
upload_copy_semaphore = asyncio.Semaphore(2)
upload_received_counts = {}
# Per-admin upload failures collected during the current batch; surfaced in the
# finish summary and cleared once the batch is published or canceled.
upload_failures = {}
pending_upload_confirmation_tasks = {}
upload_confirmation_messages = {}
upload_latest_messages = {}
upload_progress_state = {}
pending_home_button_actions = {}
pending_admin_actions = {}
pending_welcome_edit = set()
pending_announce_edit = set()
pending_member_group_url = set()
broadcast_input_mode = set()
pending_broadcasts = {}
broadcast_album_buffers = {}
broadcast_album_tasks = {}
broadcast_running = set()
directory_import_running = False

# A light global pacing guard for outgoing Bot API sends. Telegram still owns the
# authoritative limits; the existing 429/retry_after handling below is untouched.
TELEGRAM_SEND_MIN_INTERVAL = 0.15
TELEGRAM_SEND_METHODS = frozenset({
    "copyMessage", "copyMessages", "forwardMessage", "forwardMessages",
    "sendMessage", "sendPhoto", "sendVideo", "sendDocument", "sendAudio",
    "sendAnimation", "sendVoice", "sendMediaGroup",
})
telegram_send_lock = asyncio.Lock()
last_telegram_send_at = 0.0
cangku_backup_in_progress = set()
backup_retry_manual_requested = False
backup_worker_running = False
last_user_activity = 0.0
category_delivery_in_progress = set()
last_getfolder_click_at = {}  # Per-user 5-second resource button cooldown
file_open_in_progress = set()
# Per-member delivery pacing. A member requesting resources in a tight loop looks
# exactly like scraping, and it also floods Telegram with repeated sends; a short
# per-action interval plus an hourly ceiling keeps normal browsing untouched
# (one batch click still delivers its whole batch) while stopping click loops.
DELIVERY_MIN_INTERVAL = 1.5
DELIVERY_HOURLY_LIMIT = 120
_delivery_last_at = {}
_delivery_window = {}


def delivery_pace_hint(user_id):
    """Return a short hint when the member is clicking too fast, else None."""
    now = time.monotonic()
    window = _delivery_window.get(int(user_id))
    if window is None:
        window = []
        _delivery_window[int(user_id)] = window
    while window and now - window[0] > 3600:
        window.pop(0)
    last = _delivery_last_at.get(int(user_id))
    if last is not None and now - last < DELIVERY_MIN_INTERVAL:
        return "⏳ 点得有点快，稍等一下再取。"
    if len(window) >= DELIVERY_HOURLY_LIMIT:
        return "⏳ 这一小时取得太多了，稍后再来。"
    _delivery_last_at[int(user_id)] = now
    window.append(now)
    if len(_delivery_window) > 5000:
        for key in list(_delivery_window)[:2500]:
            _delivery_window.pop(key, None)
            _delivery_last_at.pop(key, None)
    return None

# Delivery watermark: one short code per delivered unit, written into the audit
# row and shown in the caption the member receives, so a leaked or resold file
# can be traced back to the account that received it.
WATERMARK_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
WATERMARK_LENGTH = 6


def make_watermark():
    """Six unambiguous characters; ~1e9 combinations, no O/0/I/1 confusion."""
    return "".join(secrets.choice(WATERMARK_ALPHABET) for _ in range(WATERMARK_LENGTH))


def delivery_caption(category, file_name, watermark, user_id):
    """Caption shown with every delivered file or album."""
    lines = []
    if category:
        lines.append(f"📁 {category}")
    if file_name:
        lines.append(f"📄 {file_name}")
    lines.append(f"🆔 领取编号：{watermark}")
    lines.append(f"👤 用户 #{int(user_id)} · 仅限本人使用，转卖可追溯")
    return "\n".join(lines)[:1000]


async def apply_delivery_watermark(client, chat_id, message_ids, category, file_name,
                                   watermark, user_id):
    """Stamp the caption after delivery so the send path itself stays fast.

    Albums share one caption, so the first message of the unit is enough.
    """
    caption = delivery_caption(category, file_name, watermark, user_id)
    for message_id in message_ids:
        if not message_id:
            continue
        try:
            await telegram_bot_api("editMessageCaption", {
                "chat_id": int(chat_id),
                "message_id": int(message_id),
                "caption": caption,
            })
        except Exception as exc:
            log.warning(
                "Watermark caption edit failed: user=%s message=%s code=%s error=%s",
                user_id, message_id, watermark, str(exc)[:180],
            )
        return
bot_admin_status_cache = {}
# Ordinary members may browse and retrieve; their uploads are ignored silently.
search_sessions = {}
class PendingSearchUsers(set):
    """Set-backed search state with compatibility for legacy dict-style writes."""

    def __setitem__(self, user_id, enabled):
        if enabled:
            self.add(int(user_id))
        else:
            self.discard(int(user_id))


pending_search_users = PendingSearchUsers()

# One shared gate for all metadata reads made by the user-account scanner.
# Telegram's channels.getMessages is especially sensitive to bursts of album lookups.
scanner_api_lock = asyncio.Lock()
scanner_api_next_at = 0.0
scanner_metadata_cooldowns = {}


async def scanner_api_call(method, *args, min_interval=1.5, **kwargs):
    """Serialize scanner metadata requests and keep a small gap between calls."""
    global scanner_api_next_at
    async with scanner_api_lock:
        loop = asyncio.get_running_loop()
        wait_for = scanner_api_next_at - loop.time()
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        try:
            return await method(*args, **kwargs)
        except FloodWait as exc:
            # Pyrogram may already auto-wait depending on sleep_threshold. Keep
            # subsequent scanner calls cooled down as well, rather than letting
            # other tasks immediately hit the same Telegram limit.
            seconds = max(30, int(getattr(exc, "value", 0) or 0) + 3)
            scanner_api_next_at = loop.time() + seconds
            log.warning("Scanner shared cooldown activated for %s seconds", seconds)
            raise
        finally:
            scanner_api_next_at = max(scanner_api_next_at, loop.time() + min_interval)

async def iter_chat_history_newest_first(chat_id, before_message_id=0, page_size=500):
    """Yield Telegram history newest-to-oldest in bounded, resumable pages.

    Pyrogram's get_chat_history uses Telegram's normal exclusive offset_id
    pagination. The previous custom GetHistory/add_offset window could return
    an empty page prematurely on large channels, making a partial scan appear
    complete. Keep each request bounded and resume strictly below the last
    yielded message ID.
    """
    cursor = max(0, int(before_message_id or 0))
    page_size = max(1, min(500, int(page_size or 500)))
    while True:
        page_count = 0
        async for message in scanner.get_chat_history(
            chat_id, limit=page_size, offset_id=cursor
        ):
            message_id = int(getattr(message, "id", 0) or 0)
            if not message_id or (cursor and message_id >= cursor):
                continue
            cursor = message_id
            page_count += 1
            yield message
        if page_count == 0:
            return


def admin_home_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📁 创建文件夹"), KeyboardButton("📋 文件夹列表")],
            [KeyboardButton("📤 上传文件"), KeyboardButton("🛠 管理设置")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def upload_active_keyboard():
    # Keep the original two persistent admin shortcuts. Upload completion
    # controls are shown inline beneath the received-file count instead.
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📋 文件夹列表"), KeyboardButton("🛠 管理设置")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )

async def get_upload_status_message(message, user_id, text):
    """Reuse one editable progress message for an admin's active upload batch."""
    status = upload_status_messages.get(int(user_id))
    if status is None:
        status = await message.reply_text(text, reply_markup=upload_active_keyboard())
        upload_status_messages[int(user_id)] = status
    else:
        try:
            await status.edit_text(text, reply_markup=upload_active_keyboard())
        except Exception as exc:
            log.debug("Could not update upload progress message: %s", exc)
    return status


background_tasks = set()

# Runtime failures only reach the log today, which nobody watches. These alerts
# are deliberately throttled: a crash loop must not turn into a DM storm.
ALERT_COOLDOWN_SECONDS = 900
ALERT_HOURLY_LIMIT = 6
alert_throttle = AlertThrottle(ALERT_COOLDOWN_SECONDS, ALERT_HOURLY_LIMIT)


async def alert_admins(title, detail="", key="general"):
    """DM the operators about a runtime failure. Never raises."""
    try:
        if not alert_throttle.allow(key, time.monotonic()):
            log.warning("Alert suppressed by throttle: key=%s title=%s", key, title)
            return False

        admin_ids = []
        if config.ADMIN_ID:
            admin_ids.append(int(config.ADMIN_ID))
        try:
            stored = await db.get_admin_id()
        except Exception:
            stored = None
        if stored and int(stored) not in admin_ids:
            admin_ids.append(int(stored))
        if not admin_ids:
            return False

        body = f"{title}"
        if detail:
            body += f"\n\n{detail}"
        body += "\n\n（同类告警 15 分钟内只发一次，每小时最多 6 条）"
        body = body[:3900]
        for admin_id in admin_ids:
            try:
                await app.send_message(admin_id, body, disable_web_page_preview=True)
            except Exception as exc:
                log.warning("Alert could not reach admin %s: %s", admin_id, exc)
        return True
    except Exception:
        log.exception("Alert dispatch failed")
        return False


def schedule_alert(title, detail="", key="general"):
    """Fire an alert from sync context (done-callbacks, workers). Never raises."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    try:
        task = loop.create_task(alert_admins(title, detail=detail, key=key))
    except Exception:
        log.exception("Could not schedule alert: key=%s", key)
        return
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


def spawn_background(name, coro):
    """Run a fire-and-forget task while keeping a reference and logging crashes."""
    task = asyncio.create_task(coro, name=name)
    background_tasks.add(task)

    def _finished(completed):
        background_tasks.discard(completed)
        if completed.cancelled():
            return
        error = completed.exception()
        if error:
            log.error(
                "Background task crashed: name=%s error=%r",
                name, error, exc_info=(type(error), error, error.__traceback__),
            )
            schedule_alert(
                f"🚨 后台任务崩溃：{name}",
                f"错误：{type(error).__name__}: {error}",
                key=f"task:{name}",
            )

    task.add_done_callback(_finished)
    return task


def track_upload_task(user_id, task):
    uid = int(user_id)
    tasks = pending_upload_tasks.setdefault(uid, set())
    tasks.add(task)
    def _finished(completed):
        tasks.discard(completed)
        try:
            error = completed.exception()
        except asyncio.CancelledError:
            return
        if error:
            log.error("Background upload task failed for user %s: %s", uid, error, exc_info=(type(error), error, error.__traceback__))
    task.add_done_callback(_finished)
    return task


def record_upload_failure(uid, name, step, error):
    """Keep a bounded record of one file/album that failed to land in this batch."""
    if uid is None:
        return
    add_failure(upload_failures.setdefault(int(uid), []), name, step, error)

async def _show_upload_confirmation_after_idle(client, user_id):
    uid = int(user_id)
    current = asyncio.current_task()
    try:
        # Only debounce incoming Telegram updates. Do not wait for channel copies:
        # the admin should be able to choose finish/continue while saving runs.
        await asyncio.sleep(1.0)
        if pending_upload_confirmation_tasks.get(uid) is not current:
            return
        last_message = upload_latest_messages.get(uid)
        batch = await db.get_upload_batch(uid)
        if not batch or not last_message:
            return
        received = int(upload_received_counts.get(uid, 0))
        prompt_text = (
            f"📥 已接收文件：{received} 个\n"
            "⏳ 文件正在后台保存，不会因为显示此提示而中断。\n\n"
            "请选择下一步："
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ 结束上传", callback_data="mc:uploadfinish"),
             InlineKeyboardButton("➕ 继续上传", callback_data="mc:uploadcontinue")]
        ])
        old = upload_confirmation_messages.get(uid)
        if old:
            # Editing an existing Telegram message does not change its position.
            # Remove it first, then reply to the newest uploaded file so the
            # single confirmation card always stays at the bottom of the chat.
            try:
                await old.delete()
            except Exception as exc:
                log.debug("Could not delete previous upload confirmation for user %s: %s", uid, exc)
        prompt = await last_message.reply_text(prompt_text, reply_markup=keyboard)
        upload_confirmation_messages[uid] = prompt
    except asyncio.CancelledError:
        return
    except Exception:
        log.exception("Could not show upload confirmation for user %s", uid)
    finally:
        if pending_upload_confirmation_tasks.get(uid) is current:
            pending_upload_confirmation_tasks.pop(uid, None)


def schedule_upload_confirmation(client, user_id, message):
    uid = int(user_id)
    previous = pending_upload_confirmation_tasks.get(uid)
    if previous and not previous.done():
        previous.cancel()
    # Keep the same confirmation message; after the next pause it will be edited
    # with the cumulative count instead of deleting/reposting another message.
    upload_latest_messages[uid] = message
    pending_upload_confirmation_tasks[uid] = asyncio.create_task(
        _show_upload_confirmation_after_idle(client, uid)
    )


async def _run_single_upload_task(client, message):
    async with upload_copy_semaphore:
        await _upload_single_handler(client, message)


async def _run_album_upload_task(client, key):
    async with upload_copy_semaphore:
        await _flush_album_upload(client, key)


async def update_admin_upload_progress(message, user_id, category, increment=1, force=False):
    """Maintain one compact live counter instead of sending per-file/per-album notices."""
    uid = int(user_id)
    state = upload_progress_state.setdefault(uid, {"received": 0, "last_shown": 0})
    state["received"] += int(increment)
    received = state["received"]
    # Avoid hammering Telegram with edits during a fast multi-album forward.
    if force or state["last_shown"] == 0 or received - state["last_shown"] >= 5:
        await get_upload_status_message(
            message, uid,
            f"📂 上传目录：{category}\n📥 已接收 {received} 个文件，后台保存中…\n"
            "发送暂停后会自动显示数量和确认按钮。",
        )
        state["last_shown"] = received
    return received



async def admin_home_text():
    stats_row, activity, categories, catalog = await asyncio.gather(
        db.stats(), db.activity_stats(), db.list_categories(), db.catalog_stats()
    )
    users, files, _, _ = stats_row
    album_groups = sum(int(row["album_count"] or 0) for row in categories)
    return (
        "⚙️ 资源管理中心\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "管理上传、目录、存储、扫描与备份\n\n"
        "📊 数据概览\n"
        f"🟢 今日活跃：{activity['day']} 人（近 7 天 {activity['week']} 人）\n"
        f"👥 注册总数：{users}\n"
        f"📁 文件夹数量：{len(categories)}\n"
        f"📦 已发布资源：{files}\n"
        f"🖼 图片：{catalog['images']} 张  ·  🎬 视频：{catalog['videos']} 个\n"
        f"🗂 相册分组：{album_groups} 组\n\n"
        "请选择下方两列功能按钮。"
    )


def admin_keyboard():
    """Compact two-column dashboard; seldom-used modules live in mc:more."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 上传资源", callback_data="mc:upload"),
         InlineKeyboardButton("📁 文件夹管理", callback_data="mc:directories")],
        [InlineKeyboardButton("📚 资源管理", callback_data="mc:files"),
         InlineKeyboardButton("🧾 领取记录与审计", callback_data="mc:audit")],
        [InlineKeyboardButton("🗄 储存管理", callback_data="mc:storage"),
         InlineKeyboardButton("📦 备份管理", callback_data="mc:backup")],
        [InlineKeyboardButton("👥 会员群绑定", callback_data="mc:bindgroup"),
         InlineKeyboardButton("📊 数据统计", callback_data="mc:stats")],
        [InlineKeyboardButton("🧩 验证码设置", callback_data="mc:captcha"),
         InlineKeyboardButton("⚙️ 更多设置", callback_data="mc:more")],
    ])


def admin_more_keyboard():
    """Secondary dashboard for modules that are used rarely."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 扫描账号", callback_data="mc:scannerlogin"),
         InlineKeyboardButton("🛡 内容保护", callback_data="mc:contentguard")],
        [InlineKeyboardButton("🆔 领取编号查询", callback_data="mc:wm"),
         InlineKeyboardButton("🛡 管理员管理", callback_data="mc:admins")],
        [InlineKeyboardButton("👋 进群欢迎", callback_data="mc:welcome"),
         InlineKeyboardButton("🔘 首页按钮", callback_data="mc:homebuttons")],
        [InlineKeyboardButton("📢 首页公告", callback_data="mc:announce"),
         InlineKeyboardButton("📣 群发广播", callback_data="mc:broadcast")],
        [InlineKeyboardButton("📖 命令帮助", callback_data="mc:commands"),
         InlineKeyboardButton("🏠 返回管理首页", callback_data="mc:home")],
    ])


def back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 返回管理首页", callback_data="mc:home")]])
pending_binding_filter = filters.create(
    lambda _, __, message: bool(
        message.from_user and message.from_user.id in pending_channel_bindings
    )
)


async def get_storage_channels():
    primary = await db.get_setting("storage_primary_channel_id")
    backup = await db.get_setting("storage_backup_channel_id")
    return (int(primary) if primary else None, int(backup) if backup else None)

async def validate_storage_chat(chat_id, role):
    """Validate a configured Telegram storage destination without changing stored data."""
    chat = await telegram_bot_api("getChat", {"chat_id": int(chat_id)})
    chat_type = chat.get("type")
    if role == "primary" and chat_type != "channel":
        raise ValueError("主存储必须使用频道，不能使用群组。")
    if role == "backup" and chat_type not in ("channel", "group", "supergroup"):
        raise ValueError("备用存储只支持频道或群组。")

    bot_id = BOT_ID
    if not bot_id:
        me = await telegram_bot_api("getMe", {})
        bot_id = int(me["id"])
    member = await telegram_bot_api("getChatMember", {
        "chat_id": int(chat_id), "user_id": int(bot_id),
    })
    status = member.get("status")
    if status not in ("administrator", "creator"):
        raise ValueError("机器人必须先加入目标并设为管理员。")
    if chat_type == "channel" and status == "administrator" and not member.get("can_post_messages", False):
        raise ValueError("机器人在频道中没有“发布消息”权限。")
    return chat


async def get_member_group_id():
    value = await db.get_setting("member_group_id")
    return int(value) if value else None


MEMBER_GROUP_URL_KEY = "member_group_url"


def normalize_member_group_url(raw):
    """Accept only http/https/t.me links; anything else counts as "not set"."""
    value = str(raw or "").strip()[:200]
    if not value:
        return ""
    if not value.lower().startswith(("http://", "https://", "t.me/")):
        return ""
    return value


async def get_member_group_url():
    return normalize_member_group_url(await db.get_setting(MEMBER_GROUP_URL_KEY))


async def membership_gate_markup():
    """Join button for the membership gate replies; None keeps them plain text."""
    url = await get_member_group_url()
    if not url:
        return None
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 加入会员群", url=url)]
    ])


MEMBERSHIP_GATE_TEXT = "⛔ 获取资源失败：请先加入管理员指定的会员群，再重新点击获取资源。"


async def reply_membership_gate(message):
    """Membership gate reply: plain text unless an invitation link is configured."""
    await message.reply_text(
        MEMBERSHIP_GATE_TEXT,
        reply_markup=await membership_gate_markup(),
    )


WELCOME_ENABLED_KEY = "welcome_enabled"
WELCOME_MESSAGE_KEY = "welcome_message"

DEFAULT_WELCOME_MESSAGE = (
    "👋 欢迎 {name} 加入会员资源群！\n\n"
    "在这里可以按文件夹挑选资源，领取后会发到你的私聊。\n"
    "点下面的机器人入口开始使用：\n"
    "{bot}"
)


WELCOME_AUTO_DELETE_KEY = "welcome_auto_delete_seconds"
WELCOME_AUTO_DELETE_ALLOWED = (0, 300, 1800, 3600, 86400)


async def get_welcome_delete_seconds():
    """Auto-delete delay for the group welcome message; 0 keeps it forever."""
    raw = await db.get_setting(WELCOME_AUTO_DELETE_KEY)
    try:
        seconds = int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        return 0
    return seconds if seconds in WELCOME_AUTO_DELETE_ALLOWED else 0


async def get_welcome_settings():
    """Return (enabled, template) for the group welcome message.

    The welcome is on by default; only an explicit "0" turns it off.
    A blank custom template falls back to the built-in default.
    """
    enabled_value = await db.get_setting(WELCOME_ENABLED_KEY)
    enabled = enabled_value != "0"
    custom = await db.get_setting(WELCOME_MESSAGE_KEY)
    template = (custom or "").strip() or DEFAULT_WELCOME_MESSAGE
    return enabled, template


def render_welcome_message(template, names, bot_url):
    name_text = "、".join(names)
    return template.replace("{name}", name_text).replace("{bot}", bot_url)


async def render_captcha_admin(message, notice=""):
    enabled = await captcha_is_enabled()
    body = (
        "🧩 图片验证码设置\n"
        "━━━━━━━━━━━━━━\n"
        f"当前状态：{'🟢 已开启' if enabled else '⏸ 已关闭'}\n\n"
        "开启后，普通会员进入机器人前需要完成图片验证码。\n"
        "• 图片显示 4 位数字，下方 6 个按钮选一组\n"
        "• 验证码本身有效 3 分钟\n"
        "• 验证成功后 1 小时内有效\n"
        "• 连续错误 5 次，临时限制 15 分钟\n"
        "• 管理员不受验证码拦截\n"
        "• 设置和验证状态保存在 PostgreSQL，重启后保留"
    )
    if notice:
        body = notice + "\n\n" + body
    toggle_label = "⏸ 关闭验证码" if enabled else "✅ 开启验证码"
    await message.edit_text(
        body,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(toggle_label, callback_data="mc:captcha:toggle")],
            [InlineKeyboardButton("🏠 返回管理首页", callback_data="mc:home")],
        ]),
    )


async def render_welcome_admin(message, notice=""):
    enabled, template = await get_welcome_settings()
    delete_after = await get_welcome_delete_seconds()
    delete_labels = {0: "不删除", 300: "5 分钟", 1800: "30 分钟", 3600: "1 小时", 86400: "24 小时"}
    body = (
        "👋 进群欢迎设置\n\n"
        f"当前状态：{'开启' if enabled else '关闭'}\n"
        f"欢迎语自动删除：{delete_labels.get(delete_after, '不删除')}\n\n"
        f"当前文案：\n{template}\n"
    )
    if notice:
        body += f"\n{notice}\n"
    await message.edit_text(
        body,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{'✅' if enabled else '⏸'} 自动欢迎开关", callback_data="mc:welcome:toggle"),
             InlineKeyboardButton("✏️ 修改文案", callback_data="mc:welcome:edit")],
            [InlineKeyboardButton("不删除", callback_data="mc:welcome:del:0"),
             InlineKeyboardButton("5 分钟后删", callback_data="mc:welcome:del:300")],
            [InlineKeyboardButton("30 分钟", callback_data="mc:welcome:del:1800"),
             InlineKeyboardButton("1 小时", callback_data="mc:welcome:del:3600")],
            [InlineKeyboardButton("24 小时", callback_data="mc:welcome:del:86400"),
             InlineKeyboardButton("♻️ 恢复默认文案", callback_data="mc:welcome:reset")],
            [InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")],
        ]),
    )


ANNOUNCEMENT_KEY = "home_announcement"
ANNOUNCEMENT_MAX_LEN = 300


async def get_home_announcement():
    value = await db.get_setting(ANNOUNCEMENT_KEY)
    return str(value or "").strip()[:ANNOUNCEMENT_MAX_LEN]


async def render_announce_admin(message, notice=""):
    announcement = await get_home_announcement()
    body = "📢 首页公告设置\n\n"
    body += "当前公告：\n" + (announcement if announcement else "（未设置）") + "\n"
    if notice:
        body += f"\n{notice}\n"
    await message.edit_text(
        body,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ 编辑公告", callback_data="mc:announce:edit"),
             InlineKeyboardButton("🗑 清空公告", callback_data="mc:announce:clear")],
            [InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")],
        ]),
    )


async def wait_telegram_send_slot():
    """Space out outgoing sends so bursts never reach Telegram back to back."""
    global last_telegram_send_at
    async with telegram_send_lock:
        now = time.monotonic()
        delay = TELEGRAM_SEND_MIN_INTERVAL - (now - last_telegram_send_at)
        if delay > 0:
            await asyncio.sleep(delay)
        last_telegram_send_at = time.monotonic()


async def telegram_bot_api(method: str, payload: dict, timeout=None):
    """Call Telegram Bot API directly, retrying rate-limited media transfers safely."""
    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/{method}"
    session = telegram_http_session
    if session is None or session.closed:
        raise RuntimeError("Telegram HTTP session is not initialized")

    # Only active sending/copying methods are paced; callback answers and other
    # read-only calls must stay immediate.
    if method in TELEGRAM_SEND_METHODS:
        await wait_telegram_send_slot()

    # Telegram explicitly tells us when a 429 may be retried. Retry both copy
    # and forward operations so backup jobs survive rate limits.
    retry_copy = method in ("copyMessage", "copyMessages", "forwardMessage", "forwardMessages")
    max_attempts = 6 if retry_copy else 1
    for attempt in range(max_attempts):
        try:
            async with session.post(url, json=payload, timeout=timeout) as response:
                data = await response.json(content_type=None)
        except Exception as exc:
            # Do not include the request URL: it contains the bot token.
            raise RuntimeError(
                f"Telegram {method} network error: {type(exc).__name__}"
            ) from None

        if isinstance(data, dict) and data.get("ok"):
            return data.get("result")

        error_code = data.get("error_code", "unknown") if isinstance(data, dict) else "unknown"
        description = data.get("description", "invalid API response") if isinstance(data, dict) else "invalid API response"
        if retry_copy and str(error_code) == "429" and attempt < max_attempts - 1:
            parameters = data.get("parameters") if isinstance(data, dict) else None
            retry_after = parameters.get("retry_after", 1) if isinstance(parameters, dict) else 1
            try:
                delay = max(1, int(retry_after))
            except (TypeError, ValueError):
                delay = 1
            log.warning(
                "Telegram %s rate limited; retrying in %ss (attempt %s/%s)",
                method, delay, attempt + 1, max_attempts - 1,
            )
            await asyncio.sleep(delay)
            continue

        raise RuntimeError(f"Telegram {method} failed ({error_code}): {description}")


def folder_storage_caption(category: str) -> str:
    name = " ".join(str(category or "").strip().lstrip("#").split())
    return "#" + name.replace(" ", "_") if name else "#未分类"


async def queue_storage_message_caption(chat_id: int, message_id: int, caption: str):
    # Persist caption work; the scanner user account edits channel posts asynchronously.
    await db.enqueue_scanner_caption_job(int(chat_id), int(message_id), str(caption))
    log.info("Scanner caption job queued: chat=%s message=%s", chat_id, message_id)


async def scanner_caption_worker():
    while True:
        try:
            if scanner is None or not scanner.is_connected:
                await asyncio.sleep(5)
                continue
            jobs = await db.list_scanner_caption_jobs(20)
            if not jobs:
                await asyncio.sleep(2)
                continue
            for job in jobs:
                if scanner is None or not scanner.is_connected:
                    break
                channel_id, message_id = int(job["channel_id"]), int(job["message_id"])
                try:
                    await scanner.edit_message_caption(
                        chat_id=channel_id, message_id=message_id,
                        caption=str(job["caption"] or ""), parse_mode=None,
                        caption_entities=[],
                    )
                    await db.complete_scanner_caption_job(channel_id, message_id)
                    log.info("Scanner caption confirmed: chat=%s message=%s", channel_id, message_id)
                except Exception as exc:
                    wait_seconds = max(2, min(60, int(getattr(exc, "value", 5) or 5)))
                    await db.retry_scanner_caption_job(
                        channel_id, message_id,
                        f"{type(exc).__name__}: {str(exc)[:400]}",
                    )
                    log.warning(
                        "Scanner caption edit failed; retained for retry: chat=%s message=%s error=%s",
                        channel_id, message_id, exc,
                    )
                    await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Scanner caption worker iteration failed")
            await asyncio.sleep(5)


app = Client(
    "member_cloud_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

# "Active users" has to come from real traffic, not from the upload path only.
# Every update refreshes users.last_seen, but at most once per window per user
# so a chatty session cannot turn into a write every message.
ACTIVITY_TOUCH_INTERVAL = 300
ACTIVITY_TOUCH_CACHE_MAX = 20000
activity_touch_cache = {}


async def remember_activity(user_id):
    uid = int(user_id)
    now = time.monotonic()
    last = activity_touch_cache.get(uid)
    if last is not None and now - last < ACTIVITY_TOUCH_INTERVAL:
        return
    if len(activity_touch_cache) >= ACTIVITY_TOUCH_CACHE_MAX:
        activity_touch_cache.clear()
    activity_touch_cache[uid] = now
    # Fire and forget: this runs before every other handler, so a slow or
    # locked database here would delay every reply in the bot.
    try:
        spawn_background("activity-touch", db.touch_user(uid))
    except Exception as exc:
        log.debug("Could not queue activity touch for %s: %s", uid, exc)


@app.on_message(filters.private, group=-1)
async def track_private_activity(client, message):
    if message.from_user:
        await remember_activity(message.from_user.id)


@app.on_callback_query(group=-1)
async def track_callback_activity(client, query):
    if query.from_user:
        await remember_activity(query.from_user.id)


# Telegram has no "is this user still reachable" endpoint: the send attempt is
# the check. A typing action fails exactly when a message would (403 blocked /
# deactivated) without delivering anything the user can see or keep.
REACH_CHECK_BATCH = 200
REACH_CHECK_PAUSE = 0.05
reach_check_state = {"task": None}


def classify_reach_error(exc):
    # Telegram mixes styles: "bot was blocked by the user" but "PEER_ID_INVALID".
    text = str(exc).lower().replace("_", " ")
    if "blocked" in text:
        return "blocked"
    if "deactivated" in text:
        return "deactivated"
    if "can't initiate conversation" in text or "cant initiate conversation" in text:
        # The user never opened this bot, so no message can ever be delivered.
        return "gone"
    if "chat not found" in text or "user not found" in text or "peer id invalid" in text:
        return "gone"
    return "unknown"


async def probe_user_reach(user_id):
    """Return ok/blocked/deactivated/gone/unknown for one user."""
    for attempt in range(2):
        try:
            # pyrogram expects a ChatAction enum here; a plain string raises
            # AttributeError before any request is made.
            await app.send_chat_action(user_id, ChatAction.TYPING)
            return "ok"
        except FloodWait as exc:
            await asyncio.sleep(int(getattr(exc, "value", 5) or 5) + 1)
            continue
        except Exception as exc:
            status = classify_reach_error(exc)
            if status != "unknown":
                return status
            # Keep the real reason visible: without it an unknown result is
            # indistinguishable from a code bug (which is how the first run
            # classified every user as unknown).
            log.warning(
                "Reach probe undetermined: user=%s attempt=%s error=%r",
                user_id, attempt + 1, exc,
            )
            await asyncio.sleep(1)
    return "unknown"


def reach_summary_text(counts, note=""):
    checked = counts["total"] - counts["unchecked"]
    return (
        "📡 用户可达性统计\n"
        "━━━━━━━━━━━━━━\n"
        "（可达 = 现在还能收到机器人消息）\n\n"
        f"✅ 可达：{counts['ok']} 人\n"
        f"🚫 已屏蔽机器人：{counts['blocked']} 人\n"
        f"💀 账号已注销：{counts['deactivated']} 人\n"
        f"❓ 无法触达：{counts['gone']} 人\n"
        f"⚠️ 未确定：{counts['unknown']} 人\n"
        f"⏳ 尚未检测：{counts['unchecked']} 人\n\n"
        f"👥 注册总数：{counts['total']} 人｜已检测 {checked} 人"
        + (f"\n\n{note}" if note else "")
    )


async def run_reach_check(status_message):
    """Walk the whole user table once and record who can still receive messages."""
    try:
        counts = await db.reach_stats()
        total = counts["total"]
        if not total:
            await status_message.edit_text(
                "📡 用户可达性统计\n\n还没有任何注册用户，无需检测。",
                reply_markup=back_keyboard(),
            )
            return

        done = 0
        offset = 0
        unchecked = counts["unchecked"]
        while True:
            user_ids = await db.list_users_for_reach_check(REACH_CHECK_BATCH, offset)
            if not user_ids:
                break
            results = []
            for user_id in user_ids:
                results.append((user_id, await probe_user_reach(user_id)))
                await asyncio.sleep(REACH_CHECK_PAUSE)
            await db.set_user_reach_bulk(results)
            done += len(user_ids)
            offset += len(user_ids)
            try:
                await status_message.edit_text(
                    "📡 正在检测（不会给对方发任何消息）\n"
                    "━━━━━━━━━━━━━━\n"
                    f"进度：{done} / {total} 人\n"
                    f"预计剩余约 {max(0, int((total - done) * REACH_CHECK_PAUSE))} 秒\n\n"
                    "期间机器人可以正常领取资源，检测完成后会自动出结果。"
                )
            except Exception:
                log.debug("Reach check progress edit failed", exc_info=True)

        final = await db.reach_stats()
        await status_message.edit_text(
            reach_summary_text(
                final,
                f"本轮检测了 {done} 人（其中此前未检测 {unchecked} 人）。",
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 重新检测", callback_data="mc:reachcheck")],
                [InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")],
            ]),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("User reachability check failed")
        try:
            await status_message.edit_text(
                "⚠️ 检测中断，已保存的进度不会丢失，可以稍后点「重新检测」继续。",
                reply_markup=back_keyboard(),
            )
        except Exception:
            log.debug("Could not report reach check failure", exc_info=True)
    finally:
        reach_check_state["task"] = None


# Newcomer guard: a member who just joined sees protected content for a while.
# Telegram exposes no join timestamp, so the only trustworthy source is the
# chat_member update the bot receives while it is an admin of the member group.
NEWCOMER_GUARD_KEY = "newcomer_guard_enabled"
NEWCOMER_HOURS_KEY = "newcomer_guard_hours"
newcomer_guard_cache = {}


async def newcomer_guard_hours():
    return clamp_hours(await db.get_setting(NEWCOMER_HOURS_KEY) or DEFAULT_HOURS)


async def newcomer_guard_active(user_id):
    """True while this user is still inside the newcomer window."""
    if await db.get_setting(NEWCOMER_GUARD_KEY) != "1":
        return False
    uid = int(user_id)
    now = time.monotonic()
    cached = newcomer_guard_cache.get(uid)
    if cached and cached[0] > now:
        return cached[1]
    try:
        joined_at = await db.get_group_join(uid)
        hours = await newcomer_guard_hours()
        active = window_active(joined_at, datetime.now(timezone.utc), hours)
    except Exception:
        log.exception("Newcomer guard check failed for user %s", uid)
        active = False
    if len(newcomer_guard_cache) >= 5000:
        newcomer_guard_cache.clear()
    newcomer_guard_cache[uid] = (now + (60 if active else 300), active)
    return active


@app.on_chat_member_updated()
async def track_group_joins(client, update):
    """Record joins/leaves in the bound member group."""
    try:
        group_id = await get_member_group_id()
    except Exception:
        log.exception("Could not read member group binding for join tracking")
        return
    if not group_id:
        return
    try:
        if update.chat is None or int(update.chat.id) != int(group_id):
            return
        new = update.new_chat_member
        if new is None or new.user is None:
            return
        user_id = int(new.user.id)
        old_status = getattr(update.old_chat_member, "status", None)
        if new.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
            await db.clear_group_join(user_id)
            newcomer_guard_cache.pop(user_id, None)
        elif new.status == ChatMemberStatus.MEMBER and old_status in (
            None, ChatMemberStatus.LEFT, ChatMemberStatus.BANNED,
        ):
            await db.record_group_join(user_id)
            newcomer_guard_cache.pop(user_id, None)
        elif new.status == ChatMemberStatus.RESTRICTED and getattr(new, "is_member", False) and old_status in (
            None, ChatMemberStatus.LEFT, ChatMemberStatus.BANNED,
        ):
            await db.record_group_join(user_id)
            newcomer_guard_cache.pop(user_id, None)
    except Exception:
        log.exception("Could not record member group join change")


SCANNER_WORKDIR = os.path.dirname(config.DATABASE_PATH) or "."
os.makedirs(SCANNER_WORKDIR, exist_ok=True)
SCANNER_SESSION_PATH = os.path.join(SCANNER_WORKDIR, "member_cloud_scanner.session")
SCANNER_SESSION_SETTING_KEY = "scanner_session_string_v1"

def legacy_scanner_session_is_authorized(path):
    """Return True only for an existing, already-authorized Pyrogram session."""
    if not os.path.isfile(path):
        return False
    try:
        uri = "file:" + os.path.abspath(path) + "?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=2) as connection:
            row = connection.execute(
                "SELECT user_id, auth_key FROM sessions LIMIT 1"
            ).fetchone()
        return bool(row and row[0] and row[1] and len(row[1]) >= 256)
    except (sqlite3.Error, OSError, TypeError):
        return False

# The authoritative scanner login is stored in the database, not the
# deployment filesystem, which may be replaced during every redeploy.
scanner = None
scan_task = None


@app.on_message(filters.private, group=-100)
async def mark_private_activity(client, message):
    global last_user_activity
    last_user_activity = asyncio.get_running_loop().time()


@app.on_callback_query(group=-100)
async def mark_callback_activity(client, query):
    global last_user_activity
    last_user_activity = asyncio.get_running_loop().time()

WELCOME = (
    "☁️ 会员文件网盘\n\n"
    "• 浏览目录：按文件夹挑选需要的资源\n"
    "• 一键领取：整批发送到你的私聊\n"
    "• 收藏资源：把常用的文件加入收藏\n\n"
    "仅会员群成员及管理员可以领取。"
)


async def is_superadmin(user_id: int) -> bool:
    if int(config.ADMIN_ID) == int(user_id):
        return True
    try:
        stored_admin_id = await db.get_admin_id()
        return stored_admin_id is not None and int(stored_admin_id) == int(user_id)
    except Exception:
        log.exception("Could not read stored superadmin ID")
        return False


async def is_admin(user_id: int) -> bool:
    if await is_superadmin(user_id):
        return True
    try:
        return await db.is_admin(user_id)
    except Exception:
        log.exception("Could not read delegated admin list")
        return False


async def get_membership_tier(user_id: int, force_refresh: bool = False):
    if await is_superadmin(user_id):
        return "超级管理员"
    if await is_admin(user_id):
        return "管理员"
    # These independent local checks used to run as two serial database
    # round trips. Run them concurrently without weakening either permission.
    is_banned, has_manual_membership = await asyncio.gather(
        db.is_banned(user_id),
        db.has_manual_membership(user_id),
    )
    if is_banned:
        log.info("Membership denied: user %s is in the local ban list", user_id)
        return None
    if has_manual_membership:
        return "手动会员"

    now = asyncio.get_running_loop().time()
    # Claim paths pass force_refresh=True so each delivery re-validates with
    # Telegram even inside the hourly window; browsing reuses the cache below
    # so a member who was removed is re-checked once an hour at most. A fresh
    # denial is reused regardless, otherwise blocked users would retry
    # getChatMember on every click.
    cached = membership_cache.get(int(user_id))
    if cached and cached[0] > now and (cached[1] is None or not force_refresh):
        return cached[1]

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
        # The bot's own admin status barely ever changes, so it is cached far
        # longer than a member check and removed from every click's latency.
        cached_admin = bot_admin_status_cache.get((int(group_id), int(bot_id)))
        if cached_admin and cached_admin[0] > now:
            bot_is_admin = cached_admin[1]
        else:
            bot_member = await telegram_bot_api(
                "getChatMember",
                {"chat_id": group_id, "user_id": bot_id},
            )
            bot_is_admin = str(bot_member.get("status", "")) in ("administrator", "creator")
            bot_admin_status_cache[(int(group_id), int(bot_id))] = (now + 600, bot_is_admin)
        if not bot_is_admin:
            log.error(
                "Membership verification unavailable: bot is not admin in bound group %s",
                group_id,
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
        if status in ("member", "administrator", "creator") or (
            status == "restricted" and member.get("is_member") is True
        ):
            tier = "黄金会员"
        else:
            tier = None
        # Cache group membership: an hour once verified, a minute when denied,
        # so a fresh joiner can retry immediately while members cost at most
        # one getChatMember call per hour on browsing paths.
        ttl = MEMBERSHIP_TTL_SECONDS if tier else MEMBERSHIP_DENIED_TTL_SECONDS
        if len(membership_cache) >= 2000:
            membership_cache.pop(next(iter(membership_cache)))
        membership_cache[int(user_id)] = (now + ttl, tier)
        return tier
    except Exception as exc:
        # A Telegram hiccup must not tell a paying member to go join the group.
        stale = membership_cache.get(int(user_id))
        log.error(
            "Bot API membership check failed: user=%s group=%s error=%s stale=%s",
            user_id, group_id, str(exc)[:400], stale[1] if stale else None,
        )
        return stale[1] if stale else None


async def has_access(user_id: int, force_refresh: bool = False) -> bool:
    return await get_membership_tier(user_id, force_refresh=force_refresh) is not None


# Private-chat image CAPTCHA. All state is stored in the existing PostgreSQL
# settings table so sessions survive process restarts and redeployments.
CAPTCHA_ENABLED_KEY = "captcha_enabled"
CAPTCHA_SESSION_TTL = 3600
CAPTCHA_CHALLENGE_TTL = 180
CAPTCHA_MAX_ATTEMPTS = 5
CAPTCHA_BLOCK_SECONDS = 900
# Prevent rapid/repeated button taps from creating duplicate CAPTCHA messages.
captcha_callback_locks = {}


def _captcha_setting_key(kind, user_id):
    return f"captcha_{kind}:{int(user_id)}"


async def captcha_is_enabled():
    return await db.get_setting(CAPTCHA_ENABLED_KEY) != "0"


async def captcha_session_valid(user_id):
    raw_value = await db.get_setting(_captcha_setting_key("session", user_id))
    try:
        session = json.loads(raw_value or "{}")
        return float(session.get("expires_at", 0)) > time.time()
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


async def captcha_notice_allowed(user_id, cooldown=12):
    """Limit repeated CAPTCHA chat notices caused by rapid user messages."""
    key = _captcha_setting_key("notice", user_id)
    now = time.time()
    raw_value = await db.get_setting(key)
    try:
        last_notice = float(raw_value or 0)
    except (TypeError, ValueError):
        last_notice = 0
    if now - last_notice < cooldown:
        return False
    await db.set_setting(key, str(now))
    return True


def captcha_choice_keyboard(user_id, challenge):
    options = challenge.get("options") or []
    buttons = []
    for start in range(0, len(options), 3):
        row = [
            InlineKeyboardButton(
                str(options[index]),
                callback_data=f"captcha_pick:{int(user_id)}:{index}",
            )
            for index in range(start, min(start + 3, len(options)))
        ]
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


async def handle_captcha_choice(client, query):
    user = query.from_user
    message = query.message
    if not user or user.is_bot or not message or message.chat.type != ChatType.PRIVATE:
        return
    try:
        _, owner_text, index_text = str(query.data or "").split(":", 2)
        owner_id, index = int(owner_text), int(index_text)
    except (TypeError, ValueError):
        await query.answer("验证按钮无效，请重新获取验证码。", show_alert=True)
        return
    if owner_id != int(user.id):
        await query.answer("这不是你的验证码。", show_alert=True)
        return
    if await is_admin(user.id) or not await captcha_is_enabled():
        await query.answer("无需验证。")
        return

    now = time.time()
    block_raw = await db.get_setting(_captcha_setting_key("block", user.id))
    try:
        blocked_until = float(block_raw or 0)
    except (TypeError, ValueError):
        blocked_until = 0
    if blocked_until > now:
        remaining = max(1, int((blocked_until - now + 59) // 60))
        await query.answer(f"验证失败次数过多，请 {remaining} 分钟后再试。", show_alert=True)
        return

    challenge_raw = await db.get_setting(_captcha_setting_key("challenge", user.id))
    try:
        challenge = json.loads(challenge_raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        challenge = {}
    if not challenge or float(challenge.get("expires_at", 0) or 0) <= now:
        await query.answer("验证码已过期，正在发送新的一张。", show_alert=True)
        try:
            await message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            await send_captcha_challenge(client, user.id)
        except Exception:
            log.exception("Could not resend expired CAPTCHA: user=%s", user.id)
            await message.reply_text("⚠️ 新验证码发送失败，请稍后重新发送 /start。")
        return

    options = challenge.get("options") or []
    # A correct answer at position 0 must not be read as "no answer".
    answer_index = stored_answer_index(challenge)
    if index < 0 or index >= len(options) or answer_index < 0 or answer_index >= len(options):
        await query.answer("按钮无效，请重新验证。", show_alert=True)
        return

    if index != answer_index:
        attempts = int(challenge.get("attempts", 0) or 0) + 1
        try:
            await message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        if attempts >= CAPTCHA_MAX_ATTEMPTS:
            await db.set_setting(_captcha_setting_key("challenge", user.id), "")
            await db.set_setting(
                _captcha_setting_key("block", user.id),
                str(now + CAPTCHA_BLOCK_SECONDS),
            )
            await query.answer("连续错误 5 次，已限制 15 分钟。", show_alert=True)
            await message.reply_text("⛔ 验证失败次数过多，请 15 分钟后再试。")
            return
        remaining = max(0, CAPTCHA_MAX_ATTEMPTS - attempts)
        # Answer the tap before regenerating the image: the click must get
        # feedback immediately even if the new CAPTCHA takes a second.
        await query.answer(
            f"❌ 选错了，还可以再试 {remaining} 次。已发一张新的验证码。",
            show_alert=True,
        )
        try:
            await send_captcha_challenge(
                client, user.id, attempts=attempts,
                notice=(
                    f"❌ 刚才那组不对。请看这张新图，点数字完全一致的那一组"
                    f"（还剩 {remaining} 次机会）。"
                ),
            )
        except Exception:
            log.exception(
                "Could not regenerate CAPTCHA after a wrong choice: user=%s", user.id
            )
            await message.reply_text(
                f"❌ 选错了，还可以再试 {remaining} 次。\n"
                "新验证码生成失败，请稍后重新发送 /start。"
            )
        return

    await db.set_setting(
        _captcha_setting_key("session", user.id),
        json.dumps({"verified_at": now, "expires_at": now + CAPTCHA_SESSION_TTL}),
    )
    await db.set_setting(_captcha_setting_key("challenge", user.id), "")
    await db.set_setting(_captcha_setting_key("block", user.id), "")
    await db.set_setting(_captcha_setting_key("notice", user.id), "")
    try:
        await message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await query.answer("✅ 验证成功！")
    await message.reply_text("✅ 验证成功！本次验证有效 1 小时。请发送 /start 进入机器人。")


async def send_captcha_challenge(client, user_id, attempts=0, notice=""):
    from captcha_image import build_captcha

    try:
        image, options, answer_index = build_captcha()
    except Exception:
        log.exception("CAPTCHA image generation failed for user %s", user_id)
        raise

    challenge = {
        "expires_at": time.time() + CAPTCHA_CHALLENGE_TTL,
        "attempts": int(attempts),
        "options": options,
        "answer_index": int(answer_index),
    }
    try:
        await db.set_setting(
            _captcha_setting_key("challenge", user_id),
            json.dumps(challenge, separators=(",", ":")),
        )
        await db.set_setting(_captcha_setting_key("notice", user_id), str(time.time()))
    except Exception:
        log.exception("CAPTCHA challenge database write failed for user %s", user_id)
        try:
            await db.set_setting(_captcha_setting_key("challenge", user_id), "")
            await db.set_setting(_captcha_setting_key("notice", user_id), "")
        except Exception:
            log.exception("Could not roll back partial CAPTCHA state for user %s", user_id)
        raise

    caption = (
        "🧩 安全验证\n\n"
        f"{notice + chr(10) + chr(10) if notice else ''}"
        "请在下方按钮中，点击与图片里 4 位数字完全相同的一组。\n"
        "验证码 3 分钟有效；验证通过后 1 小时内无需重复验证。"
    )
    try:
        await client.send_photo(
            int(user_id), photo=image, caption=caption,
            reply_markup=captcha_choice_keyboard(user_id, challenge)
        )
    except Exception:
        log.exception("CAPTCHA Telegram photo send failed for user %s", user_id)
        # Remove an unsent challenge so the next request can retry delivery.
        try:
            await db.set_setting(_captcha_setting_key("challenge", user_id), "")
            await db.set_setting(_captcha_setting_key("notice", user_id), "")
        except Exception:
            log.exception("Could not clear failed CAPTCHA challenge for user %s", user_id)
        raise


async def captcha_message_gate(client, message):
    user = message.from_user
    if not user or user.is_bot or await is_admin(user.id) or not await captcha_is_enabled():
        return
    user_id = int(user.id)
    now = time.time()

    block_raw = await db.get_setting(_captcha_setting_key("block", user_id))
    try:
        blocked_until = float(block_raw or 0)
    except (TypeError, ValueError):
        blocked_until = 0
    if blocked_until > now:
        remaining = max(1, int((blocked_until - now + 59) // 60))
        if await captcha_notice_allowed(user_id):
            await message.reply_text(f"⛔ 验证失败次数过多，请 {remaining} 分钟后再试。")
        raise StopPropagation
    if await captcha_session_valid(user_id):
        return

    challenge_raw = await db.get_setting(_captcha_setting_key("challenge", user_id))
    try:
        challenge = json.loads(challenge_raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        challenge = {}

    if (
        not challenge
        or not challenge.get("options")
        or float(challenge.get("expires_at", 0) or 0) <= now
    ):
        if await captcha_notice_allowed(user_id):
            try:
                await send_captcha_challenge(client, user_id)
            except Exception:
                log.exception("Could not create image CAPTCHA for user %s", user_id)
                await message.reply_text("⚠️ 验证码暂时无法生成，请稍后重试。")
        raise StopPropagation

    # CAPTCHA is completed only through the six inline buttons.
    # Ignore typed text, commands and media while a challenge is pending.
    raise StopPropagation


@app.on_message(filters.private, group=-99)
async def captcha_private_message_middleware(client, message):
    await captcha_message_gate(client, message)


@app.on_callback_query(group=-99)
async def captcha_private_callback_middleware(client, query):
    user = query.from_user
    message = query.message
    if query.data and str(query.data).startswith("captcha_pick:"):
        try:
            await handle_captcha_choice(client, query)
        except StopPropagation:
            raise
        except Exception:
            log.exception("CAPTCHA choice handling failed")
            try:
                await query.answer("验证时出错了，请重新发送 /start 再试。", show_alert=True)
            except Exception:
                log.debug("Could not answer failed CAPTCHA tap", exc_info=True)
        raise StopPropagation
    if (
        not user or user.is_bot or not message or message.chat.type != ChatType.PRIVATE
        or await is_admin(user.id) or not await captcha_is_enabled()
        or await captcha_session_valid(user.id)
    ):
        return

    user_id = int(user.id)
    lock = captcha_callback_locks.setdefault(user_id, asyncio.Lock())
    async with lock:
        # Re-check after acquiring the lock in case verification just completed.
        if await is_admin(user_id) or not await captcha_is_enabled():
            return
        if await captcha_session_valid(user_id):
            return

        now = time.time()
        block_raw = await db.get_setting(_captcha_setting_key("block", user_id))
        try:
            blocked_until = float(block_raw or 0)
        except (TypeError, ValueError):
            blocked_until = 0

        if blocked_until > now:
            remaining = max(1, int((blocked_until - now + 59) // 60))
            await query.answer(
                f"验证失败次数过多，请 {remaining} 分钟后再试。",
                show_alert=True,
            )
            raise StopPropagation

        challenge_raw = await db.get_setting(_captcha_setting_key("challenge", user_id))
        try:
            challenge = json.loads(challenge_raw or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            challenge = {}

        if (
            challenge and challenge.get("options")
            and float(challenge.get("expires_at", 0) or 0) > now
        ):
            # The CAPTCHA image is already in the chat. Do not send another
            # message for every catalogue or claim button tap.
            await query.answer("请先完成图片验证码。")
            raise StopPropagation

        try:
            await send_captcha_challenge(client, user_id)
            await query.answer("验证码已发送，请查看上方图片。")
        except Exception:
            log.exception("Could not create CAPTCHA after callback for user %s", user_id)
            await query.answer("验证码暂时无法生成，请稍后重试。", show_alert=True)
        raise StopPropagation


def media_details(message):
    # Telegram animations are document-backed internally; detect their
    # dedicated field before the generic document field.
    for kind in ("animation", "video", "audio", "photo", "voice", "document"):
        media = getattr(message, kind, None)
        if media:
            name = clean_file_name(getattr(media, "file_name", None) or f"{kind}_{message.id}")
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


async def notify_folder_subscribers(client, category, event="new_folder"):
    """Send opt-in folder/resource notifications without interrupting publishing."""
    try:
        subscribers = await db.list_notification_subscribers()
        if not subscribers:
            return {"sent": 0, "failed": 0}
        rows = await db.list_categories(category=str(category))
        if not rows or not rows[0]["share_key"]:
            log.warning("Skip subscriber notification: folder link unavailable for %s", category)
            return {"sent": 0, "failed": len(subscribers)}
        row = rows[0]
        link = folder_share_url(str(row["share_key"]))
        if event == "new_folder":
            body = (
                "📁 新文件夹上线\n\n"
                f"目录：{row['category']}\n"
                "新目录已经开放，点击下方按钮查看。"
            )
            button_text = "📂 查看新文件夹"
        else:
            body = (
                "🔔 文件夹有新资源\n\n"
                f"目录：{row['category']}\n"
                "管理员已发布新资源，点击下方按钮进入目录。"
            )
            button_text = "📂 查看新资源"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(button_text, url=link)]
        ])
        sent = failed = 0
        for subscriber_id in subscribers:
            try:
                await client.send_message(
                    int(subscriber_id), body, reply_markup=markup,
                    disable_web_page_preview=True,
                )
                sent += 1
            except Exception as exc:
                failed += 1
                log.info("Folder notification failed for user %s: %s", subscriber_id, exc)
        log.info(
            "Folder notification finished: category=%s event=%s sent=%s failed=%s",
            category, event, sent, failed,
        )
        return {"sent": sent, "failed": failed}
    except Exception:
        log.exception("Could not notify subscribers for folder %s", category)
        return {"sent": 0, "failed": 0}


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


CN_TZ = timezone(timedelta(hours=8))


def parse_cn_datetime(value):
    """Parse a stored UTC timestamp into a Beijing-time datetime, or None."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(CN_TZ)
    except (TypeError, ValueError, OverflowError):
        return None


def format_cn_datetime(value, fmt="%Y-%m-%d %H:%M:%S"):
    """Render a stored UTC timestamp in Beijing time; fall back to raw text."""
    parsed = parse_cn_datetime(value)
    if parsed is None:
        return str(value or "")[:19]
    return parsed.strftime(fmt)


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


MEDIA_TYPE_ICONS = {
    "photo": "🖼",
    "video": "🎬",
    "animation": "🎞",
    "audio": "🎵",
    "voice": "🎵",
}


def media_type_icon(media_type):
    """Emoji for one stored media_type; unknown types fall back to a document."""
    return MEDIA_TYPE_ICONS.get(str(media_type or "").lower(), "📄")


async def user_claimed_file_keys(user_id, files):
    """Which of these single files this member already received (one query)."""
    keys = [
        str(row["file_key"]) for row in files
        if row["file_key"] and not row["album_group_id"]
    ]
    if not keys:
        return set()
    try:
        return {str(key) for key in await db.get_user_history_keys(int(user_id), keys)}
    except Exception:
        log.exception("Could not read claimed keys for user %s", user_id)
        return set()


def folder_detail_text(category_row, files, page, member_group_id, claimed_keys=None):
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
        "📖 内容统计（已入库资源）：",
        f"　• 资源总数：{int(category_row['file_count'] or 0)} 个",
        f"　• 🗂 成组相册：{int(category_row.get('album_count', 0) or 0)} 组",
        f"　• 🖼 图片：{int(category_row['image_count'] or 0)} 张",
        f"　• 🎬 视频：{int(category_row['video_count'] or 0)} 个",
        f"　• 🎞 动图：{int(category_row['animation_count'] or 0)} 个",
        f"　• 🎵 音频/语音：{int(category_row['audio_count'] or 0)} 个",
        f"　• 📄 文档：{int(category_row['document_count'] or 0)} 个",
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
    file_labels = []
    seen_album_ids = set()
    claimed = claimed_keys or set()
    for row in files:
        album_id = row["album_group_id"]
        if album_id:
            if str(album_id) in seen_album_ids:
                continue
            seen_album_ids.add(str(album_id))
            file_labels.append(f"• 🗂 相册组（{int(row['album_size'] or 0)} 个文件）")
        else:
            file_name = str(row["file_name"] or "未命名资源")[:48]
            claimed_mark = "✅ " if str(row["file_key"]) in claimed else ""
            file_labels.append(
                f"• {claimed_mark}{media_type_icon(row['media_type'])} {file_name}"
            )
    body.append("\n".join(file_labels) if file_labels else "这个目录暂时没有资源。")
    return "\n".join(body)


async def get_saved_folder_offset(user_id, share_key, file_count):
    key = f"folder_view:{int(user_id)}:{share_key}"
    try:
        offset = max(0, int(await db.get_setting(key) or 0))
    except (TypeError, ValueError):
        offset = 0
    total = max(0, int(file_count or 0))
    if total <= 0:
        return 0
    return min((offset // 20) * 20, ((total - 1) // 20) * 20)


async def save_folder_offset(user_id, share_key, offset):
    await db.set_setting(
        f"folder_view:{int(user_id)}:{share_key}",
        str(max(0, (int(offset) // 20) * 20)),
    )


async def get_saved_folder_list_page(user_id, total_pages):
    key = f"folder_list_page:{int(user_id)}"
    try:
        page = max(0, int(await db.get_setting(key) or 0))
    except (TypeError, ValueError):
        page = 0
    return min(page, max(0, int(total_pages or 1) - 1))


async def save_folder_list_page(user_id, page):
    await db.set_setting(
        f"folder_list_page:{int(user_id)}",
        str(max(0, int(page))),
    )


async def get_folder_list_sort(user_id):
    value = str(await db.get_setting(f"folder_list_sort:{int(user_id)}") or "created")
    return value if value in ("created", "name", "count") else "created"


async def save_folder_list_sort(user_id, sort):
    if sort not in ("created", "name", "count"):
        sort = "created"
    await db.set_setting(f"folder_list_sort:{int(user_id)}", sort)
    return sort


def folder_detail_keyboard(category_row, page, has_next, is_favorite):
    share_key = str(category_row["share_key"] or "")
    rows = []
    if int(category_row["file_count"] or 0) > 0:
        rows.append([InlineKeyboardButton(
            "📥 获取资源（单文件每批20个）",
            callback_data=f"uc:getfolder:{share_key}:0",
        )])
    rows.append([InlineKeyboardButton(
        "⭐ 取消收藏目录" if is_favorite else "⭐ 收藏目录",
        callback_data=f"uc:folderstar:{share_key}",
    )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            "⬅️ 上一页",
            callback_data=f"uc:folderkey:{share_key}:{max(0, page * 20 - 20)}",
        ))
    if has_next:
        nav.append(InlineKeyboardButton(
            "下一页 ➡️",
            callback_data=f"uc:folderkey:{share_key}:{(page + 1) * 20}",
        ))
    if nav:
        rows.append(nav)
    total = int(category_row["file_count"] or 0)
    last_page = max(0, (total - 1) // 20) if total > 0 else 0
    jump = []
    if page > 0:
        jump.append(InlineKeyboardButton(
            "⏮ 回顶部",
            callback_data=f"uc:folderkey:{share_key}:0",
        ))
    if page < last_page:
        jump.append(InlineKeyboardButton(
            "⏭ 到末页",
            callback_data=f"uc:folderkey:{share_key}:{last_page * 20}",
        ))
    if jump:
        rows.append(jump)
    rows.append([InlineKeyboardButton("⬅️ 资源目录", callback_data="uc:folders")])
    return InlineKeyboardMarkup(rows)


def parse_category(message) -> str:
    caption = (message.caption or "").strip()
    # Manual uploads retain the existing explicit category behavior.
    for token in caption.split():
        if token.startswith("#"):
            category = token[1:].strip("，,。.!！?？:：;；()（）[]【】{}")
            if category and all(
                "\u3400" <= char <= "\u4dbf" or "\u4e00" <= char <= "\u9fff"
                for char in category
            ):
                return category[:40]
    first = caption.splitlines()[0].strip() if caption else ""
    if first.startswith("分类:") or first.startswith("分类："):
        return first[3:].strip()[:40] or "未分类"
    return "未分类"


def parse_chinese_album_category(messages) -> str:
    """Find a Chinese hashtag anywhere in any caption belonging to an album."""
    import re

    pattern = re.compile(
        r"(?<![A-Za-z0-9_])#([\u3400-\u4dbf\u4e00-\u9fff]"
        r"[\u3400-\u4dbf\u4e00-\u9fff0-9A-Za-z_-]{0,39})"
    )
    for message in messages:
        caption = (getattr(message, "caption", None) or "").strip()
        match = pattern.search(caption)
        if match:
            return match.group(1).rstrip("_-")[:40]
    return ""


def guess_category(message, media_type, file_name):
    # Kept for compatibility with any existing callers; automatic scanning
    # no longer guesses categories from filenames or media types.
    return parse_category(message)


async def run_history_scan(primary_channel_id, backup_channel_id=None):
    """Index source messages while creating directories only for tagged albums."""
    global scan_task
    current_message_id = None
    current_offset_id = None
    try:
        scan_direction = await db.get_setting("history_scan_direction")
        if scan_direction != "newest_first_v1":
            # Existing progress used the old custom pagination direction.
            # Restart only the cursor (not indexed rows) so the reliable
            # newest-to-oldest pass can revisit messages and fill missing rows.
            await db.reset_scan_progress_for_full_rescan(primary_channel_id)
            await db.set_setting("history_scan_direction", "newest_first_v1")
            log.warning(
                "Migrated history scan to newest-first mode; existing indexed data is preserved"
            )
        progress = await db.get_scan_progress(primary_channel_id)
        if not progress["completed"]:
            chat = await resolve_scanner_chat(primary_channel_id)
            resolved_channel_id = int(chat.id)
            offset_id = int(progress["last_message_id"] or 0)
            current_offset_id = offset_id
            log.info(
                "Primary history scan started: channel=%s title=%s offset=%s",
                resolved_channel_id, getattr(chat, "title", ""), offset_id
            )
            album_category_cache = {}
            async for item in iter_chat_history_newest_first(
                resolved_channel_id, before_message_id=offset_id
            ):
                current_message_id = int(item.id)
                group_id = getattr(item, "media_group_id", None)
                is_album_member = bool(group_id)
                group_key = str(group_id) if is_album_member else None
                album_key = (
                    f"cangku:{resolved_channel_id}:{group_key}"
                    if is_album_member else None
                )

                media_type, file_name, media_unique_id = media_details(item)
                if not media_type:
                    await db.advance_scan_cursor(primary_channel_id, current_message_id)
                    continue

                # Index each Telegram message independently. Members of the same
                # album share media_group_id; no get_media_group verification is
                # needed during history indexing.
                fingerprint = f"cangku:{resolved_channel_id}:{current_message_id}"
                caption_category = (
                    parse_chinese_album_category([item]) if is_album_member else ""
                )
                if caption_category and album_key:
                    album_category_cache[album_key] = caption_category
                    category = caption_category
                elif album_key:
                    category = album_category_cache.get(album_key, "")
                else:
                    category = ""

                created_at = (
                    item.date.isoformat() if getattr(item, "date", None)
                    else datetime.now(timezone.utc).isoformat()
                )
                await db.save_scanned_message(
                    primary_channel_id, None, primary_channel_id,
                    item.id, fingerprint, file_name, media_type, category,
                    created_at, config.ADMIN_ID,
                    secrets.token_urlsafe(9).replace("-", "").replace("_", ""),
                    album_group_id=(
                        album_key if album_key
                        else f"cangku-single:{resolved_channel_id}:{current_message_id}"
                    ),
                    # The real Telegram message ID is a stable ordering key. Do
                    # not assume album member IDs must be numerically consecutive.
                    album_position=current_message_id if album_key else 0,
                )
                if album_key:
                    if caption_category:
                        await db.update_cangku_album_metadata(
                            fingerprint, album_key, current_message_id,
                            category=caption_category,
                        )
                    else:
                        await db.update_cangku_album_metadata(
                            fingerprint, album_key, current_message_id
                        )
            await db.mark_scan_completed(primary_channel_id)
            if await db.get_setting("backup_full_scan_required") == "1":
                await db.set_setting("backup_full_scan_required", "0")

        category_catalog_cache["expires_at"] = 0
        categories = await db.list_categories()
        _, total_files, _, _ = await db.stats()
        primary_progress = await db.get_scan_progress(primary_channel_id)
        await app.send_message(
            config.ADMIN_ID,
            "✅ 主存储频道历史扫描完成\n"
            f"主频道已检查媒体：{primary_progress['scanned_count']}\n"
            f"已建立目录：{len(categories)} 个\n"
            f"目录资源总数：{total_files} 个\n\n"
            "自动目录只从完整相册组的中文 #标签 建立；单条文件及无中文标签的相册不建立目录。"
        )
    except asyncio.CancelledError:
        await app.send_message(
            config.ADMIN_ID, "⏸️ 主频道扫描已暂停。再次发送 /scan 可从上次进度继续。"
        )
        raise
    except Exception as exc:
        error_detail = f"{type(exc).__name__}: {exc}"
        # Revisit the message that raised the error on the next /scan.
        # The file row may already have been saved before a later album-metadata
        # update failed, so retrying this one message is intentionally idempotent.
        if current_message_id is not None:
            try:
                await db.advance_scan_cursor(
                    primary_channel_id, int(current_message_id) + 1
                )
                log.warning(
                    "Scan checkpoint rewound to retry failed message %s",
                    current_message_id,
                )
            except Exception:
                log.exception("Could not rewind scan checkpoint after failure")
        log.exception(
            "Primary history scan failed: channel=%s message_id=%s offset_id=%s error=%s",
            primary_channel_id, current_message_id, current_offset_id, error_detail,
        )
        try:
            await app.send_message(
                config.ADMIN_ID,
                "⚠️ 主频道扫描失败，已保留进度。\n"
                f"频道 ID：{primary_channel_id}\n"
                f"出错消息 ID：{current_message_id or '尚未读取到'}\n"
                f"错误：{error_detail[:900]}\n\n"
                "修复对应问题后发送 /scan 继续；不要点击从头开始备份。"
            )
        except Exception:
            log.exception("Could not notify admin about scan failure")
    finally:
        scan_task = None

async def run_backup_tag_folder_scan(client, admin_id, backup_channel_id, status_message):
    """Classify already-indexed backup-channel albums without duplicating catalog rows."""
    global directory_import_running
    scanned_messages = 0
    indexed_files = 0
    skipped_existing = 0
    unmatched_messages = 0
    album_groups = {}
    tagged_groups = 0
    assigned_files = 0
    try:
        chat = await resolve_scanner_chat(backup_channel_id)
        async for item in iter_chat_history_newest_first(chat.id):
            scanned_messages += 1
            media_type, file_name, _ = media_details(item)
            if not media_type:
                continue

            message_id = int(getattr(item, "id", 0) or 0)
            if not message_id:
                continue

            # This directory pass only classifies resources already saved by the
            # normal dual-channel upload flow. New uploads are still copied to
            # both storage channels and inserted by the upload handlers below.
            existing = await db.get_file_by_backup_message_id(message_id)
            if existing:
                skipped_existing += 1
            else:
                unmatched_messages += 1

            group_id = getattr(item, "media_group_id", None)
            if group_id:
                group_key = f"{int(chat.id)}:{group_id}"
                group = album_groups.setdefault(
                    group_key, {"message_ids": [], "category": ""}
                )
                if message_id not in group["message_ids"]:
                    group["message_ids"].append(message_id)
                if not group["category"]:
                    group["category"] = parse_chinese_album_category([item])

        # Apply a hashtag to the entire indexed album only after the scan has
        # collected its complete set of Telegram message IDs.
        for group in album_groups.values():
            message_ids = sorted(set(group["message_ids"]))
            category = str(group["category"] or "").strip()
            if not category or not 2 <= len(message_ids) <= 10:
                continue
            updated = await db.assign_backup_album_category(int(chat.id), message_ids, category)
            if updated == len(message_ids):
                tagged_groups += 1
                assigned_files += updated

        # This scan must not create duplicate catalog rows; normal new uploads
        # continue to use the dual-channel upload handlers.
        category_catalog_cache["expires_at"] = 0
        category_catalog_cache["rows"] = []
        categories = await db.list_categories()
        result_text = (
            "✅ 副频道扫描及标签目录整理完成\n\n"
            f"📨 已读取频道消息：{scanned_messages} 条\n"
            f"🔗 匹配到已有资源：{skipped_existing} 个\n"
            f"⚠️ 未匹配到数据库记录：{unmatched_messages} 条（不入库）\n"
            f"🖼️ 识别相册组：{len(album_groups)} 组\n"
            f"🏷️ 成功归类相册：{tagged_groups} 组\n"
            f"📁 归入目录资源：{assigned_files} 个\n"
            f"📂 当前目录总数：{len(categories)} 个\n\n"
            "本次仅整理副储存频道中已入库资源的目录，不重复创建已有记录，也不复制或删除文件。\n"
            "以后通过机器人上传的新文件，仍会按正常流程同时保存到主频道和副频道并入库。\n"
            "单条文件和无标签相册保留在未分类资源中。"
        )
        try:
            await status_message.edit_text(result_text, reply_markup=back_keyboard())
        except Exception:
            await client.send_message(admin_id, result_text, reply_markup=back_keyboard())
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.exception("Backup-channel direct indexing and hashtag scan failed")
        error_text = (
            "❌ 副频道扫描或标签目录整理失败。\n"
            f"原因：{type(exc).__name__}: {str(exc)[:350]}\n\n"
            "本次目录扫描不会重复创建已有资源记录；正常新上传仍会同时保存到主频道和副频道。已有数据保持不变。"
        )
        try:
            await status_message.edit_text(error_text, reply_markup=back_keyboard())
        except Exception:
            await client.send_message(admin_id, error_text, reply_markup=back_keyboard())
    finally:
        directory_import_running = False


async def reply_admin_only(message):
    await message.reply_text("⛔ 仅管理员可使用此命令。")


DEFAULT_HOME_BUTTONS = [
    {"key": "search", "text": "🔎 搜索资源", "type": "action", "enabled": True},
    {"key": "history", "text": "🕘 我的领取", "type": "action", "enabled": True},
    {"key": "folders", "text": "📁 资源目录", "type": "action", "enabled": True},
    {"key": "latest", "text": "🆕 最新入库", "type": "action", "enabled": True},
    {"key": "latest_new", "text": "🆕 最近上新", "type": "action", "enabled": True},
    {"key": "top", "text": "🔥 领取最多", "type": "action", "enabled": True},
    {"key": "random", "text": "🎲 随机探索", "type": "action", "enabled": True},
    {"key": "favorites", "text": "⭐ 收藏列表", "type": "action", "enabled": True},
    {"key": "notifications", "text": "🔔 新资源订阅", "type": "action", "enabled": True},
    {"key": "member", "text": "👤 会员中心", "type": "action", "enabled": True},
    {"key": "help", "text": "❔ 使用帮助", "type": "action", "enabled": True},
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
                    and item.get("key") != "random_again"
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
        if default["key"] in ("random", "latest", "latest_new", "top", "folders", "favorites", "search", "history", "notifications", "member", "help"):
            current["text"] = default["text"]
            current["type"] = "action"
            current["enabled"] = True
    # "再来一批" is a result-screen action, never a home-menu button.
    buttons = [item for item in buttons if item.get("key") != "random_again"]
    # Keep built-in actions in a clear two-column order; custom URL buttons follow.
    built_in_order = ["folders", "search", "latest", "random", "history", "help", "latest_new", "top", "favorites", "notifications", "member"]
    built_ins = {str(item.get("key")): item for item in buttons if item.get("key") in built_in_order}
    custom = [item for item in buttons if item.get("key") not in built_in_order]
    buttons = [built_ins[key] for key in built_in_order if key in built_ins] + custom
    return buttons


async def save_home_buttons(buttons):
    await db.set_setting("home_buttons", json.dumps(buttons, ensure_ascii=False))


async def user_home_keyboard():
    buttons = await get_home_buttons()
    enabled = [item for item in buttons if item.get("enabled", True)]
    by_key = {str(item.get("key")): item for item in enabled}

    def make_button(item):
        if item.get("type") == "url":
            return InlineKeyboardButton(str(item["text"])[:60], url=str(item["url"]))
        return InlineKeyboardButton(
            str(item["text"])[:60], callback_data=f"uc:{item['key']}"
        )

    # Four clean rows. 「我的领取」 and 「使用帮助」 moved into 会员中心 so the
    # grid never ends on a single lonely key.
    rows = []
    for group in (
        ("folders", "search"),
        ("latest_new", "top"),
        ("random", "favorites"),
        ("notifications", "member"),
    ):
        row = [make_button(by_key[key]) for key in group if key in by_key]
        if row:
            rows.append(row)

    # Preserve every administrator-created custom button below the built-ins.
    custom = [item for item in enabled if item.get("key") not in {
        "search", "history", "folders", "latest", "random", "favorites",
        "notifications", "member", "random_again", "latest_new", "top", "help",
    }]
    for index in range(0, len(custom), 2):
        rows.append([make_button(item) for item in custom[index:index + 2]])

    return InlineKeyboardMarkup(rows) if rows else InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 刷新首页", callback_data="uc:home")]
    ])


async def user_home_text(user_id):
    tier = await get_membership_tier(user_id)
    stats = await db.catalog_stats()
    announcement = await get_home_announcement()
    head = "📚 会员资源中心\n"
    head += "━━━━━━━━━━━━━━━━━━\n"
    if announcement:
        head += f"📢 {announcement}\n\n"
    else:
        head += "欢迎回来，选择功能开始浏览资源。\n\n"
    return (
        head
        + f"🏅 当前身份：{tier or '未开通'}\n\n"
        + "📊 资源库统计\n"
        + f"📁 文件夹：{stats['folders']} 个\n"
        + f"📦 资源总数：{stats['messages']} 条\n"
        + f"🖼 图片：{stats['images']} 张\n"
        + f"🎬 视频：{stats['videos']} 个\n"
        + "━━━━━━━━━━━━━━━━━━\n"
        + "🔎 支持搜索文件名、文件夹名称和中文标签。\n"
        + "请点击下方两列功能入口："
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


async def get_member_delivery_settings():
    protect = await db.get_setting("member_content_protection")
    delete_after = await db.get_setting("member_auto_delete_seconds")
    try:
        delete_after = max(0, min(172800, int(delete_after))) if delete_after is not None else 600
    except (TypeError, ValueError):
        delete_after = 600
    return (protect != "0", delete_after)


def seconds_until(timestamp_text):
    """Seconds left until an ISO timestamp; 0 when already due or unparseable."""
    try:
        when = datetime.fromisoformat(str(timestamp_text))
    except (TypeError, ValueError):
        return 0
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


async def _delete_later(client, chat_id, message_ids, delete_at):
    delay = seconds_until(delete_at)
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        await client.delete_messages(int(chat_id), message_ids)
        log.info("Auto-deleted member delivery: chat=%s count=%s", chat_id, len(message_ids))
    except Exception:
        log.exception(
            "Could not auto-delete member delivery: chat=%s ids=%s", chat_id, message_ids
        )
    finally:
        try:
            await db.remove_pending_deletions(chat_id, message_ids)
        except Exception:
            log.exception("Could not clear pending deletions: chat=%s", chat_id)


async def schedule_member_delivery_delete(client, chat_id, message_ids):
    ids = [int(value) for value in message_ids if value]
    _, delete_after = await get_member_delivery_settings()
    if not ids or delete_after <= 0:
        return

    delete_at = (datetime.now(timezone.utc) + timedelta(seconds=delete_after)).isoformat()
    try:
        await db.add_pending_deletions(chat_id, ids, delete_at)
    except Exception:
        log.exception("Could not persist pending deletion: chat=%s", chat_id)
    spawn_background("delivery-delete", _delete_later(client, chat_id, ids, delete_at))


async def startup_self_check(client):
    """One startup pass over the things that silently break resource delivery."""
    problems = []
    try:
        await db.get_setting("storage_primary_channel_id")
    except Exception:
        log.exception("Self-check: database is unreachable")
        log.error("Self-check: database unreachable; skipping the remaining checks")
        return ["数据库连接失败"]

    primary_id, backup_id = await get_storage_channels()
    if not primary_id:
        problems.append("主存储频道未绑定")
    if not backup_id:
        problems.append("备用存储未绑定")
    if primary_id and backup_id and int(primary_id) == int(backup_id):
        problems.append("主存储与备用存储是同一个频道")

    bot_id = BOT_ID
    if not bot_id:
        try:
            bot_id = (await app.get_me()).id
        except Exception:
            bot_id = 0
    for label, chat_id in (("主存储", primary_id), ("备用存储", backup_id)):
        if not chat_id or not bot_id:
            continue
        try:
            member = await telegram_bot_api(
                "getChatMember", {"chat_id": int(chat_id), "user_id": int(bot_id)},
            )
            status = str(member.get("status", ""))
            if status not in ("administrator", "creator"):
                problems.append(f"{label}频道里机器人不是管理员（当前状态：{status or '未知'}）")
        except Exception as exc:
            problems.append(f"无法检查{label}频道权限：{str(exc)[:120]}")

    group_id = await get_member_group_id()
    if not group_id:
        problems.append("会员群未绑定（会员校验会全部失败）")
    elif bot_id:
        try:
            member = await telegram_bot_api(
                "getChatMember",
                {"chat_id": int(group_id), "user_id": int(bot_id)},
            )
            status = str(member.get("status", ""))
            if status not in ("administrator", "creator"):
                problems.append(f"会员群里机器人不是管理员（当前状态：{status or '未知'}），会员校验会失败")
        except Exception as exc:
            problems.append(f"无法检查会员群权限：{str(exc)[:120]}")

    try:
        session = os.getenv("SCANNER_SESSION_STRING", "").strip()
        if not session:
            session = await db.get_setting(SCANNER_SESSION_SETTING_KEY)
        if session and (scanner is None or not scanner.is_connected):
            problems.append("扫描账号已保存但当前未连接（重启后可能需要重新 /scanlogin）")
    except Exception:
        log.exception("Self-check: could not read scanner session state")

    if problems:
        log.warning("Self-check found %s problem(s): %s", len(problems), "；".join(problems))
        try:
            admin_ids = []
            if config.ADMIN_ID:
                admin_ids.append(int(config.ADMIN_ID))
            stored = await db.get_admin_id()
            if stored and int(stored) not in admin_ids:
                admin_ids.append(int(stored))
            body = "⚠️ 机器人启动自检发现问题\n\n" + "\n".join(
                f"• {item}" for item in problems
            ) + "\n\n资源领取、上传或会员校验可能受影响，请尽快处理。"
            for admin_id in admin_ids:
                try:
                    await client.send_message(admin_id, body)
                except Exception:
                    log.exception("Self-check could not notify admin %s", admin_id)
        except Exception:
            log.exception("Self-check could not dispatch admin notice")
    else:
        log.info(
            "Self-check passed: db=ok primary=%s backup=%s member_group=%s",
            primary_id, backup_id, group_id,
        )
    return problems


async def resume_pending_deletions(client):
    """Re-arm deletions that were scheduled before the last restart."""
    try:
        rows = await db.list_pending_deletions(2000)
    except Exception:
        log.exception("Could not load pending deletions at startup")
        return 0
    if not rows:
        return 0
    batched = {}
    for row in rows:
        batched.setdefault((int(row["chat_id"]), str(row["delete_at"]),), []).append(
            int(row["message_id"])
        )
    for (chat_id, delete_at), ids in batched.items():
        spawn_background("delivery-delete", _delete_later(client, chat_id, ids, delete_at))
    log.info("Resumed %s pending member deletions across %s chats", len(rows), len(batched))
    return len(rows)


# A delivery unit that keeps failing must not pin the folder cursor forever.
def humanize_delivery_error(exc):
    """Turn a raw Telegram/DB error into a short, member-friendly reason."""
    text = str(exc or "").lower()
    if any(k in text for k in ("429", "floodwait", "too many requests", "retry_after")):
        return "发送太频繁，请稍等几分钟后再试。"
    if any(k in text for k in ("forbidden", "blocked", "deactivated", "chat not found", "user is deactivated", "can't initiate")):
        return "暂时联系不到该用户：可能已屏蔽机器人或没开启私聊，请提醒用户先打开机器人对话。"
    if any(k in text for k in ("message not found", "message_id_invalid", "msg_id_invalid", "message to forward not found", "deleted", "empty message")):
        return "原文件在存储频道已失效或被删除，请联系管理员补充资源。"
    if "400" in text or "bad request" in text:
        return "这条资源的格式不被 Telegram 接受，请联系管理员检查。"
    if "403" in text or "not enough rights" in text:
        return "机器人没有转发该资源的权限，请联系管理员检查存储频道设置。"
    return "该资源暂时无法取回，已记录，请稍后重试。"


# After this many failed attempts the unit is skipped (and reported), so the
# member can still receive the rest of the folder.
DELIVERY_MAX_ATTEMPTS = 3


async def send_catalog_file(client, user_id, file_key, return_message_id=False,
                            record=None, storage=None, settings=None,
                            defer_side_effects=False, watermark=None):
    record = record if record is not None else await db.get_file(file_key)
    if not record:
        return (False, "文件不存在或已被移除。", None) if return_message_id else (False, "文件不存在或已被移除。")

    fingerprint = str(record["source_fingerprint"] or "")
    if storage is None:
        storage = await get_storage_channels()
    primary_channel_id, backup_channel_id = storage
    copied_message_id = None
    primary_message_id = int(record["primary_message_id"] or 0)
    backup_message_id = int(record["backup_message_id"] or 0)

    # Legacy directory imports stored the original Cangku message ID in
    # primary_message_id even though it does not belong to the currently-bound
    # primary storage channel. Detect that legacy shape and never pair the
    # original message ID with the new primary channel ID.
    candidates = []
    cangku_source = None
    if fingerprint.startswith("cangku:"):
        parts = fingerprint.split(":", 2)
        if len(parts) == 3:
            try:
                cangku_source = (int(parts[1]), int(parts[2]))
            except ValueError:
                cangku_source = None
    legacy_original_pointer = bool(
        cangku_source and primary_message_id == cangku_source[1]
    )
    if primary_channel_id and primary_message_id > 0 and not legacy_original_pointer:
        candidates.append(("primary", int(primary_channel_id), primary_message_id))
    if backup_channel_id and backup_message_id > 0:
        candidates.append(("backup", int(backup_channel_id), backup_message_id))
    if cangku_source:
        candidates.append(("original", cangku_source[0], cangku_source[1]))

    if not candidates:
        reason = "这条资源还没存到可转发的位置，请联系管理员补充资源。"
        return (False, reason, None) if return_message_id else (False, reason)

    last_error = None
    watermark_code = watermark or make_watermark()
    caption_in_request = True
    for source_name, source_chat_id, source_message_id in candidates:
        try:
            if settings is None:
                settings = await get_member_delivery_settings()
            protect_content, _ = settings
            if not protect_content and await newcomer_guard_active(user_id):
                protect_content = True
            # copyMessage accepts a caption, so the watermark travels with the
            # file in the same request: no second edit call, no later rewrite.
            copy_caption = delivery_caption(
                str(record["category"] or ""), str(record["file_name"] or ""),
                watermark_code, user_id,
            )
            payload = {
                "chat_id": int(user_id),
                "from_chat_id": int(source_chat_id),
                "message_id": int(source_message_id),
                "protect_content": protect_content,
                "caption": copy_caption,
            }
            try:
                copied = await telegram_bot_api("copyMessage", payload)
                caption_in_request = True
            except Exception as caption_error:
                # If Telegram refuses the inline caption (older API behaviour or
                # a rejected payload), copy without it and stamp the message
                # afterwards so delivery never depends on that one field.
                log.warning(
                    "Inline caption rejected; retrying without it: file=%s error=%s",
                    file_key, str(caption_error)[:180],
                )
                copied = await telegram_bot_api(
                    "copyMessage",
                    {key: value for key, value in payload.items() if key != "caption"},
                )
                caption_in_request = False
            copied_message_id = int(copied["message_id"])
            log.info(
                "Delivered catalog file from %s storage: file=%s user=%s",
                source_name, file_key, user_id,
            )
            break
        except Exception as exc:
            last_error = exc
            log.warning(
                "Catalog source failed; trying next source: file=%s source=%s error=%s",
                file_key, source_name, str(exc)[:220],
            )

    if copied_message_id is None:
        # Keep Telegram's raw error in the log, not in the member-facing text.
        log.error(
            "All catalog sources failed: file=%s user=%s error=%s",
            file_key, user_id, str(last_error)[:220],
        )
        reason = humanize_delivery_error(last_error)
        return (False, reason, None) if return_message_id else (False, reason)

    if not defer_side_effects:
        await schedule_member_delivery_delete(client, user_id, [copied_message_id])
        await db.record_history(user_id, file_key)
        try:
            await db.record_delivery_audit_batch(
                secrets.token_urlsafe(12), user_id, "",
                [{
                    "file_key": file_key,
                    "category": str(record["category"] or ""),
                    "media_type": str(record["media_type"] or ""),
                    "source_chat_id": source_chat_id,
                    "source_message_id": source_message_id,
                    "delivery_method": "copyMessage",
                    "watermark": watermark_code,
                }],
            )
        except Exception:
            log.exception("Could not record standalone delivery audit: file=%s user=%s",
                          file_key, user_id)

    if not caption_in_request:
        spawn_background(
            "delivery-watermark",
            apply_delivery_watermark(
                client, user_id, [copied_message_id],
                str(record["category"] or ""), str(record["file_name"] or ""),
                watermark_code, user_id,
            ),
        )
    if return_message_id:
        return True, "文件已发送。", copied_message_id
    return True, "文件已发送。"


async def try_send_catalog_album(client, user_id, group, settings=None, schedule_delete=True):
    """Copy one original Telegram album as a unit without rebuilding unrelated media."""
    if len(group) < 2 or len(group) > 10:
        return False
    source_chats = {int(entry[2]) for entry in group}
    if len(source_chats) != 1:
        return False
    message_ids = [int(entry[3]) for entry in group]
    if settings is None:
        settings = await get_member_delivery_settings()
    protect_content, _ = settings
    if not protect_content and await newcomer_guard_active(user_id):
        protect_content = True
    copied = await telegram_bot_api("copyMessages", {
        "chat_id": int(user_id),
        "from_chat_id": source_chats.pop(),
        "message_ids": message_ids,
        "protect_content": protect_content,
    })
    if isinstance(copied, list) and len(copied) == len(group):
        if schedule_delete:
            await schedule_member_delivery_delete(
                client, user_id, [item.get("message_id") for item in copied]
            )
        return copied
    return False


async def send_catalog_batch(
    client, user_id, share_key, offset, batch, track_progress=True, ignore_history=False
):
    """Send complete media units and advance folder progress only over successful items."""
    sent = failed = skipped = already = 0
    next_offset = offset
    last_sent_message_id = None
    storage = await get_storage_channels()
    delivery_settings = await get_member_delivery_settings()
    primary_channel_id, backup_channel_id = storage
    records = await db.get_files_by_keys(item["file_key"] for item in batch)
    records_by_key = {row["file_key"]: row for row in records}
    delivered_keys = (
        await db.get_user_history_keys(
            user_id, [item["file_key"] for item in batch]
        )
        if track_progress and not ignore_history else set()
    )

    prepared = []
    for index, item in enumerate(batch):
        record = records_by_key.get(item["file_key"])
        source_chat_id = source_message_id = None
        if record:
            fingerprint = str(record["source_fingerprint"] or "")
            primary_id = int(record["primary_message_id"] or 0)
            backup_id = int(record["backup_message_id"] or 0)
            # A legacy Cangku import may have its original source message ID
            # in primary_message_id. Never pair that ID with the newly-bound
            # primary storage channel unless it is a genuine primary copy.
            legacy_source = None
            if fingerprint.startswith("cangku:"):
                parts = fingerprint.split(":", 2)
                if len(parts) == 3:
                    try:
                        legacy_source = (int(parts[1]), int(parts[2]))
                    except ValueError:
                        legacy_source = None
            legacy_pointer = bool(legacy_source and primary_id == legacy_source[1])
            if primary_channel_id and primary_id > 0 and not legacy_pointer:
                source_chat_id = int(primary_channel_id)
                source_message_id = primary_id
            elif backup_channel_id and backup_id > 0:
                source_chat_id = int(backup_channel_id)
                source_message_id = backup_id
            elif fingerprint.startswith("cangku:"):
                parts = fingerprint.split(":", 2)
                if len(parts) == 3:
                    source_chat_id = int(parts[1])
                    source_message_id = int(parts[2])
        prepared.append((index, item, record, source_chat_id, source_message_id))

    # Only files carrying the same stored Telegram album ID are sent together.
    # Unrelated photos/videos are never merged into an artificial album.
    grouped = {}
    units = []
    invalid_album_groups = set()
    for entry in prepared:
        album_id = entry[1]["album_group_id"]
        if album_id:
            grouped.setdefault(str(album_id), []).append(entry)
        else:
            units.append([entry])
    for album_id, entries in grouped.items():
        entries.sort(key=lambda entry: int(entry[1]["album_position"] or 0))
        expected = int(entries[0][1]["album_size"] or len(entries))
        if not 2 <= len(entries) <= 10 or len(entries) != expected:
            invalid_album_groups.add(album_id)
        # Keep the album as one delivery unit even if incomplete. It will be
        # skipped as a whole below, never degraded into individual messages.
        units.append(entries)

    units.sort(key=lambda unit: unit[0][0])
    completed = set()
    # Successful keys are cleared in one write at the end of the batch instead
    # of one DELETE per file.
    success_keys = []
    history_keys = []
    pending_delete_ids = []

    async def flush_batch_writes():
        if success_keys:
            keys = list(success_keys)
            success_keys.clear()
            try:
                await db.clear_delivery_failures(user_id, keys, share_key)
            except Exception:
                log.exception("Could not clear delivery failures: user=%s", user_id)
        if history_keys:
            keys = list(history_keys)
            history_keys.clear()
            try:
                await db.record_history_batch(user_id, keys)
            except Exception:
                log.exception("Could not record delivered history: user=%s", user_id)
        if pending_delete_ids:
            ids = list(pending_delete_ids)
            pending_delete_ids.clear()
            try:
                await schedule_member_delivery_delete(client, user_id, ids)
            except Exception:
                log.exception("Could not schedule delivery deletion: user=%s", user_id)

    async def advance_progress():
        nonlocal next_offset
        while next_offset - offset in completed:
            next_offset += 1
        if track_progress:
            await db.set_category_delivery_progress(user_id, share_key, next_offset)

    for unit in units:
        is_album = bool(unit[0][1]["album_group_id"])
        if track_progress:
            unit_keys = [entry[1]["file_key"] for entry in unit]
            # Albums are atomic: skip only when every member was previously
            # delivered successfully. Individual files can be safely skipped.
            if (is_album and all(key in delivered_keys for key in unit_keys)) or (
                not is_album and unit_keys[0] in delivered_keys
            ):
                already += len(unit)
                completed.update(entry[0] for entry in unit)
                await advance_progress()
                continue
        audit_id = secrets.token_urlsafe(12)
        unit_watermark = make_watermark()
        audit_entries = []
        for audit_entry in unit:
            audit_item = audit_entry[1]
            audit_entries.append({
                "file_key": audit_item.get("file_key"),
                "category": audit_item.get("category") or (
                    audit_entry[2]["category"] if audit_entry[2] else ""
                ),
                "media_type": audit_entry[2]["media_type"] if audit_entry[2] else "",
                "album_group_id": audit_item.get("album_group_id") or "",
                "source_chat_id": audit_entry[3],
                "source_message_id": audit_entry[4],
                "delivery_method": "copyMessages" if is_album else "copyMessage",
                "watermark": unit_watermark,
            })
        try:
            await db.record_delivery_audit_batch(
                audit_id, user_id, share_key, audit_entries
            )
        except Exception:
            log.exception("Could not persist delivery audit attempt: user=%s", user_id)

        delivered = False
        album_error = None
        if is_album and str(unit[0][1]["album_group_id"]) not in invalid_album_groups:
            album_records = [entry[2] for entry in unit]
            album_candidates = []
            if primary_channel_id and all(
                row
                and int(row["primary_message_id"] or 0) > 0
                and not (
                    str(row["source_fingerprint"] or "").startswith("cangku:")
                    and len(str(row["source_fingerprint"] or "").split(":", 2)) == 3
                    and int(row["primary_message_id"] or 0)
                    == int(str(row["source_fingerprint"]).split(":", 2)[2])
                )
                for row in album_records
            ):
                album_candidates.append((
                    "primary",
                    int(primary_channel_id),
                    [int(row["primary_message_id"]) for row in album_records],
                ))
            if backup_channel_id and all(
                row and int(row["backup_message_id"] or 0) > 0
                for row in album_records
            ):
                album_candidates.append((
                    "backup",
                    int(backup_channel_id),
                    [int(row["backup_message_id"]) for row in album_records],
                ))
            original_sources = []
            for row in album_records:
                fingerprint = str(row["source_fingerprint"] or "") if row else ""
                parts = fingerprint.split(":", 2)
                if not fingerprint.startswith("cangku:") or len(parts) != 3:
                    original_sources = []
                    break
                try:
                    original_sources.append((int(parts[1]), int(parts[2])))
                except ValueError:
                    original_sources = []
                    break
            if original_sources and len({source[0] for source in original_sources}) == 1:
                album_candidates.append((
                    "original",
                    original_sources[0][0],
                    [source[1] for source in original_sources],
                ))

            for source_name, source_chat_id, source_message_ids in album_candidates:
                album_entries = [
                    (entry[1], entry[2], source_chat_id, source_message_id)
                    for entry, source_message_id in zip(unit, source_message_ids)
                ]
                try:
                    delivered = await try_send_catalog_album(
                        client, user_id, album_entries, settings=delivery_settings,
                        schedule_delete=False,
                    )
                    if delivered:
                        log.info(
                            "Delivered catalog album from %s storage: user=%s count=%s",
                            source_name, user_id, len(unit),
                        )
                        break
                except Exception as exc:
                    album_error = exc
                    log.warning(
                        "Catalog album source failed; trying next source: "
                        "user=%s source=%s count=%s error=%s",
                        user_id, source_name, len(unit), str(exc)[:220],
                    )

        if delivered:
            try:
                await db.finish_delivery_audit(
                    audit_id, "success",
                    [item.get("message_id") for item in delivered],
                )
            except Exception:
                log.exception("Could not finalize successful album audit: user=%s", user_id)
            last_sent_message_id = int(delivered[-1]["message_id"])
            first_record = unit[0][2]
            spawn_background(
                "delivery-watermark",
                apply_delivery_watermark(
                    client, user_id,
                    [int(item["message_id"]) for item in delivered if item.get("message_id")],
                    str((first_record or {}).get("category") or ""),
                    str((first_record or {}).get("file_name") or ""),
                    unit_watermark, user_id,
                ),
            )
            keys = [entry[1]["file_key"] for entry in unit]
            history_keys.extend(keys)
            pending_delete_ids.extend(
                int(item["message_id"]) for item in delivered if item.get("message_id")
            )
            success_keys.extend(keys)
            sent += len(unit)
            completed.update(entry[0] for entry in unit)
            await advance_progress()
            continue

        unit_keys = [entry[1]["file_key"] for entry in unit]
        attempts = await db.record_delivery_failure(
            user_id, unit_keys, share_key,
            str(album_error)[:220] if is_album else "",
        )
        if attempts >= DELIVERY_MAX_ATTEMPTS:
            try:
                await db.finish_delivery_audit(
                    audit_id, "failed", error=str(album_error or "资源不可用或 Telegram 发送失败")
                )
            except Exception:
                log.exception("Could not finalize failed delivery audit: user=%s", user_id)
            # Repeatedly unreachable unit: skip it so the folder keeps flowing.
            skipped += len(unit)
            completed.update(entry[0] for entry in unit)
            await advance_progress()
            log.error(
                "Giving up on unreachable delivery unit after %s attempts: user=%s keys=%s",
                attempts, user_id, unit_keys,
            )
            continue
        if is_album:
            try:
                await db.finish_delivery_audit(
                    audit_id, "failed", error=str(album_error or "相册整组发送失败")
                )
            except Exception:
                log.exception("Could not finalize pending album audit: user=%s", user_id)
            # Never split an album. A failed unit must remain pending; for
            # paginated folder delivery, stop here so the next request retries
            # this exact album instead of advancing past it.
            failed += len(unit)
            log.error(
                "Album delivery failed and remains pending: user=%s group=%s count=%s attempts=%s",
                user_id, unit[0][1]["album_group_id"], len(unit), attempts,
            )
            await flush_batch_writes()
            return sent, failed, skipped, already, next_offset, last_sent_message_id

        # Only genuine single files use the individual copy path.
        for entry in unit:
            position, item = entry[0], entry[1]
            ok, error_text, copied_message_id = await send_catalog_file(
                client, user_id, item["file_key"], return_message_id=True,
                record=records_by_key.get(item["file_key"]),
                storage=storage, settings=delivery_settings,
                defer_side_effects=True, watermark=unit_watermark,
            )
            if ok:
                try:
                    await db.finish_delivery_audit(
                        audit_id, "success", [copied_message_id]
                    )
                except Exception:
                    log.exception("Could not finalize successful file audit: user=%s", user_id)
                sent += 1
                last_sent_message_id = int(copied_message_id)
                pending_delete_ids.append(int(copied_message_id))
                history_keys.append(item["file_key"])
                success_keys.append(item["file_key"])
                completed.add(position)
                await advance_progress()
            else:
                try:
                    await db.finish_delivery_audit(
                        audit_id, "failed", error=str(error_text or "Telegram 发送失败")
                    )
                except Exception:
                    log.exception("Could not finalize failed file audit: user=%s", user_id)
                failed += 1
                log.warning(
                    "File delivery failed and remains pending: user=%s share_key=%s offset=%s file=%s reason=%s",
                    user_id, share_key, offset + position,
                    item["file_key"], error_text,
                )
                await flush_batch_writes()
                return sent, failed, skipped, already, next_offset, last_sent_message_id

    await flush_batch_writes()
    return sent, failed, skipped, already, next_offset, last_sent_message_id


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client, message):
    user = message.from_user
    if not user:
        log.warning("Received /start without from_user")
        return

    log.info("Received /start from user_id=%s", user.id)
    # User statistics must never prevent the bot from answering /start.
    try:
        await db.touch_user(user.id)
    except Exception:
        log.exception("Could not update user activity for /start user_id=%s", user.id)

    try:
        payload = message.command[1] if len(message.command) > 1 else ""
        if await is_admin(user.id):
            await message.reply_text(
                await admin_home_text(),
                reply_markup=admin_home_keyboard(),
            )
            return
        if payload.startswith("folder_"):
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
                folder_detail_text(
                    category_row, files, 0, await get_member_group_id(),
                    await user_claimed_file_keys(user.id, files),
                ),
                reply_markup=folder_detail_keyboard(
                    category_row, 0, len(files) == 20,
                    await db.is_category_favorite(user.id, category_row["share_key"])
                ),
                disable_web_page_preview=True,
            )
            return
        if payload.startswith("file_"):
            if not await has_access(user.id, force_refresh=True):
                await reply_membership_gate(message)
                return
            pace = None if await is_admin(user.id) else delivery_pace_hint(user.id)
            if pace:
                await message.reply_text(pace)
                return
            ok, result = await send_catalog_file(client, user.id, payload[5:])
            if not ok:
                await message.reply_text(f"❌ {result}")
            return

        await message.reply_text(
            await user_home_text(user.id),
            reply_markup=await user_home_keyboard(),
        )
    except Exception:
        log.exception("Unhandled /start handler error for user_id=%s", user.id)
        try:
            await message.reply_text("⚠️ 机器人暂时遇到内部错误，请稍后再发送 /start。")
        except Exception:
            log.exception("Could not send /start error reply to user_id=%s", user.id)


async def _flush_album_upload(client, key):
    # Telegram delivers album items as separate updates. Wait for the group to
    # settle, then ask Telegram for the complete media group instead of trusting
    # the locally received updates (which may still be incomplete).
    await asyncio.sleep(2.0)
    queued = list(pending_album_uploads.get(key, []))
    if not queued:
        return
    queued.sort(key=lambda item: item.id)
    anchor = queued[0]
    try:
        fetched = await client.get_media_group(
            chat_id=anchor.chat.id,
            message_id=anchor.id,
        )
    except Exception:
        log.exception(
            "Could not retrieve complete Telegram media group: chat=%s group=%s",
            anchor.chat.id, key[2],
        )
        fetched = None

    if not fetched or len(fetched) < 2 or any(
        str(getattr(item, "media_group_id", "")) != str(key[2])
        for item in fetched
    ):
        pending_album_uploads.pop(key, None)
        if pending_album_uploads.get((key, "scheduled")) is asyncio.current_task():
            pending_album_uploads.pop((key, "scheduled"), None)
        log.error(
            "Refusing album upload without Telegram-confirmed complete group: "
            "user=%s chat=%s group=%s",
            key[0], key[1], key[2],
        )
        record_upload_failure(
            key[0], f"相册（{len(queued)} 个）", "相册完整性校验",
            "Telegram 未能确认整组相册完整",
        )
        try:
            await anchor.reply_text(
                "⚠️ Telegram 未能确认这组相册完整，暂未入库。请重新发送整组相册。"
            )
        except Exception:
            log.exception("Could not notify user about incomplete album")
        return

    # Merge API results with updates already received; message IDs guarantee
    # each album member is indexed exactly once and in Telegram's original order.
    merged = {item.id: item for item in fetched}
    for item in pending_album_uploads.get(key, []):
        merged[item.id] = item
    messages = sorted(merged.values(), key=lambda item: item.id)
    if len(messages) < 2:
        # A message carrying media_group_id must never fall back to the
        # single-file path: doing so would publish a broken, partial album.
        pending_album_uploads.pop(key, None)
        if pending_album_uploads.get((key, "scheduled")) is asyncio.current_task():
            pending_album_uploads.pop((key, "scheduled"), None)
        log.error(
            "Refusing to split incomplete album: user=%s chat=%s group=%s received=%s",
            key[0], key[1], key[2], len(messages),
        )
        record_upload_failure(
            key[0], f"相册（{len(messages)} 个）", "相册完整性校验",
            "相册成员不完整，拒绝拆分入库",
        )
        try:
            await anchor.reply_text(
                "⚠️ 这组相册没有完整读取，暂未入库。请重新发送整组相册，避免相册被拆分。"
            )
        except Exception:
            log.exception("Could not notify user about incomplete album")
        return

    # Include any final updates that arrived while get_media_group was running.
    for item in pending_album_uploads.get(key, []):
        merged[item.id] = item
    messages = sorted(merged.values(), key=lambda item: item.id)
    pending_album_uploads.pop(key, None)
    if pending_album_uploads.get((key, "scheduled")) is asyncio.current_task():
        pending_album_uploads.pop((key, "scheduled"), None)

    user = messages[0].from_user
    if not user or not await has_access(user.id):
        return
    active_batch = await db.get_upload_batch(user.id) if await is_admin(user.id) else None
    selected_category = (
        active_batch["category"] if active_batch
        else pending_upload_categories.get(user.id) if await is_admin(user.id)
        else None
    )
    is_admin_batch = bool(await is_admin(user.id) and selected_category)
    status = None if is_admin_batch else await messages[0].reply_text(
        f"⏳ 收到一组相册（{len(messages)} 个），正在整组保存…"
    )
    saved = 0
    failed = 0
    backup_failed = 0
    try:
        primary_channel_id, backup_channel_id = await get_storage_channels()
        if not primary_channel_id or not backup_channel_id or primary_channel_id == backup_channel_id:
            raise RuntimeError("主存储或备用存储未正确绑定")
        category = str(selected_category or parse_category(messages[0]))[:120]
        if is_admin_batch and not active_batch:
            await db.start_upload_batch(user.id, category)
        if not BOT_ID:
            me = await client.get_me()
            globals()["BOT_ID"] = me.id
        await validate_storage_chat(primary_channel_id, "primary")
        await validate_storage_chat(backup_channel_id, "backup")

        album_group_id = secrets.token_urlsafe(12)
        # Copy the Telegram album as one operation. copyMessages preserves the
        # album grouping in the destination channel; copyMessage per item does not.
        if not 2 <= len(messages) <= 10:
            raise RuntimeError(f"相册数量异常（{len(messages)} 条），拒绝拆分入库")
        prepared_items = []
        for position, message in enumerate(messages):
            media_type, file_name, fingerprint = media_details(message)
            if not media_type:
                raise RuntimeError(f"相册第 {position + 1} 条不是支持的媒体，整组取消入库")
            if is_admin_batch:
                fingerprint = f"upload:{message.chat.id}:{message.id}"
                if (
                    await db.get_upload_batch_item_by_fingerprint(fingerprint)
                    or await db.get_file_by_fingerprint(fingerprint)
                ):
                    raise RuntimeError("相册中存在已处理的重复文件，整组跳过，避免重复复制")
            elif fingerprint and await db.get_file_by_fingerprint(fingerprint):
                raise RuntimeError("相册中存在已入库的重复文件，整组取消，避免只保存部分相册")
            media_object = getattr(message, media_type, None)
            file_size = int(getattr(media_object, "file_size", 0) or 0)
            prepared_items.append((message, media_type, file_name, fingerprint, file_size))

        source_message_ids = [int(item[0].id) for item in prepared_items]
        primary_result = await telegram_bot_api("copyMessages", {
            "chat_id": primary_channel_id,
            "from_chat_id": int(messages[0].chat.id),
            "message_ids": source_message_ids,
            "remove_caption": True,
        })
        if not isinstance(primary_result, list) or len(primary_result) != len(prepared_items):
            raise RuntimeError(
                f"主频道整组复制不完整（预期 {len(prepared_items)} 条），整组未入库"
            )
        primary_ids = [int(item["message_id"]) for item in primary_result]
        # copyMessages(remove_caption=True) clears source captions; tag only album item 1.
        await queue_storage_message_caption(
            primary_channel_id, primary_ids[0], folder_storage_caption(category)
        )

        backup_ids = [0] * len(prepared_items)
        backup_error = None
        try:
            backup_result = await telegram_bot_api("copyMessages", {
                "chat_id": backup_channel_id,
                "from_chat_id": int(messages[0].chat.id),
                "message_ids": source_message_ids,
                "remove_caption": True,
            })
            if not isinstance(backup_result, list) or len(backup_result) != len(prepared_items):
                raise RuntimeError("备份频道整组复制返回数量不完整")
            backup_ids = [int(item["message_id"]) for item in backup_result]
            await queue_storage_message_caption(
                backup_channel_id, backup_ids[0], folder_storage_caption(category)
            )
        except Exception as exc:
            backup_error = exc
            backup_failed = len(prepared_items)
            log.exception(
                "Album backup copy failed; primary album retained for retry: user=%s",
                user.id,
            )

        album_group_id = secrets.token_urlsafe(12)
        staged_rows = []
        for position, item in enumerate(prepared_items):
            message, media_type, file_name, fingerprint, file_size = item
            key_file = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
            error_text = (
                f"{type(backup_error).__name__}: {str(backup_error)[:400]}"
                if backup_error else ""
            )
            staged_rows.append((
                key_file, user.id, file_name, media_type, category,
                primary_channel_id, primary_ids[position],
                backup_channel_id, backup_ids[position],
                fingerprint, file_size, error_text, album_group_id, position,
            ))

        # Keep the existing per-message file records for compatibility with
        # catalog/search/history, but every row references the same album group.
        for row in staged_rows:
            (key_file, owner_id, file_name, media_type, category,
             p_channel, p_id, b_channel, b_id, fingerprint, file_size,
             error_text, group_id, position) = row
            if is_admin_batch:
                inserted = await db.stage_upload_batch_item(
                    key_file, owner_id, file_name, media_type, category,
                    p_channel, p_id, b_channel, b_id, fingerprint, file_size,
                    error_text, group_id, position,
                )
            else:
                inserted = await db.add_file(
                    key_file, owner_id, file_name, media_type,
                    p_id, b_id, category, fingerprint, file_size,
                    album_group_id=group_id, album_position=position,
                )
            if inserted:
                saved += 1
                if backup_error and not is_admin_batch:
                    await db.enqueue_backup_retry(
                        key_file, p_channel, p_id, b_channel, error_text
                    )

        if not is_admin_batch:
            category_catalog_cache["expires_at"] = 0
        if status is not None:
            await status.edit_text(
                f"✅ 相册处理完成\n📂 目录：{category}\n"
                f"📥 收到：{len(messages)} 个\n📦 成功保存：{saved} 个\n"
                f"⚠️ 主频道保存失败：{failed} 个"
                + (f"\n⚠️ 备份失败：{backup_failed} 个（已安排重试）" if backup_failed else "")
            )
    except Exception as exc:
        log.exception("Grouped album upload failed for user %s", user.id)
        duplicate = "相册中存在已入库的重复文件" in str(exc)
        record_upload_failure(
            user.id, f"相册（{len(messages)} 个）",
            "重复去重" if duplicate else "相册入库", exc,
        )
        if status is not None:
            try:
                if "相册中存在已入库的重复文件" in str(exc):
                    # Duplicate album: skip silently; do not expose internal dedupe details to users.
                    await status.delete()
                else:
                    await status.edit_text(
                        "❌ 相册处理失败，本组未能完整入库。请稍后重试或联系管理员。"
                    )
            except Exception:
                log.exception("Could not report album upload failure")



BROADCAST_CONFIRM_KEYBOARD = InlineKeyboardMarkup([
    [InlineKeyboardButton("✅ 确认群发", callback_data="mc:broadcast:confirm"),
     InlineKeyboardButton("❌ 取消", callback_data="mc:broadcast:cancel")]
])


async def _finish_broadcast_album(client, user_id, key):
    try:
        await asyncio.sleep(1.5)
        messages = sorted(broadcast_album_buffers.pop(key, []), key=lambda item: int(item.id))
        if not messages:
            return
        uid = int(user_id)
        pending_broadcasts[uid] = {
            "chat_id": int(messages[0].chat.id),
            "message_ids": [int(item.id) for item in messages],
            "is_album": True,
        }
        broadcast_input_mode.discard(uid)
        await client.send_message(
            uid,
            f"📢 已收到相册（{len(messages)} 项）。上方内容就是发送预览。\n"
            "确认后将整组发送给所有已记录用户。",
            reply_markup=BROADCAST_CONFIRM_KEYBOARD,
        )
    except asyncio.CancelledError:
        return
    except Exception:
        log.exception("Could not prepare broadcast album preview for user %s", user_id)
        broadcast_input_mode.discard(int(user_id))


async def capture_broadcast_message(client, message):
    user = message.from_user
    if not user or int(user.id) not in broadcast_input_mode:
        return False
    uid = int(user.id)
    group_id = getattr(message, "media_group_id", None)
    if group_id:
        key = (uid, int(message.chat.id), str(group_id))
        broadcast_album_buffers.setdefault(key, []).append(message)
        previous = broadcast_album_tasks.get(key)
        if previous and not previous.done():
            previous.cancel()
        broadcast_album_tasks[key] = asyncio.create_task(
            _finish_broadcast_album(client, uid, key)
        )
        return True

    broadcast_input_mode.discard(uid)
    pending_broadcasts[uid] = {
        "chat_id": int(message.chat.id),
        "message_ids": [int(message.id)],
        "is_album": False,
    }
    await message.reply_text(
        "📢 已收到广播内容。上方消息就是发送预览。\n"
        "确认后将发送给所有已记录用户。",
        reply_markup=BROADCAST_CONFIRM_KEYBOARD,
    )
    return True


async def run_broadcast_guarded(client, admin_id, status):
    try:
        await run_broadcast(client, admin_id, status)
    finally:
        broadcast_running.discard(int(admin_id))


async def run_broadcast(client, admin_id, status):
    pending = pending_broadcasts.get(int(admin_id))
    if not pending:
        await status.edit_text("⚠️ 没有待发送的广播内容，请重新进入广播管理。")
        return
    target_ids = await db.list_user_ids()
    sent = 0
    failed = 0
    total = len(target_ids)
    await status.edit_text(f"⏳ 正在广播：0/{total}")
    for index, target_id in enumerate(target_ids, 1):
        try:
            while True:
                try:
                    if pending["is_album"]:
                        await client.copy_media_group(
                            chat_id=int(target_id),
                            from_chat_id=int(pending["chat_id"]),
                            message_id=int(pending["message_ids"][0]),
                        )
                    else:
                        await client.copy_message(
                            chat_id=int(target_id),
                            from_chat_id=int(pending["chat_id"]),
                            message_id=int(pending["message_ids"][0]),
                        )
                    sent += 1
                    break
                except FloodWait as exc:
                    await asyncio.sleep(max(1, int(exc.value)))
        except Exception:
            failed += 1
            log.warning("Broadcast delivery failed: admin=%s user=%s", admin_id, target_id, exc_info=True)
        if index % 25 == 0 or index == total:
            try:
                await status.edit_text(
                    f"⏳ 广播进行中：{index}/{total}\n"
                    f"✅ 成功：{sent}  ❌ 失败：{failed}"
                )
            except Exception:
                pass
        await asyncio.sleep(0.08)
    pending_broadcasts.pop(int(admin_id), None)
    await status.edit_text(
        f"✅ 广播完成\n\n👥 目标用户：{total}\n"
        f"📨 发送成功：{sent}\n⚠️ 发送失败：{failed}\n\n"
        "失败用户记录已保留，没有自动删除任何用户。"
    )


async def upload_handler(client, message):
    user = message.from_user
    if not user:
        return
    if int(user.id) in broadcast_input_mode:
        await capture_broadcast_message(client, message)
        return
    if not await is_admin(user.id):
        # Members browse and retrieve only. Ignore their uploads completely:
        # no reply, nothing stored, no database row.
        log.info(
            "Ignored member upload: user=%s message=%s",
            user.id, getattr(message, "id", None),
        )
        return
    group_id = getattr(message, "media_group_id", None)
    if not group_id:
        user = message.from_user
        if user and await is_admin(user.id):
            active_batch = await db.get_upload_batch(user.id)
            selected_category = active_batch["category"] if active_batch else pending_upload_categories.get(user.id)
            if selected_category:
                upload_received_counts[int(user.id)] = upload_received_counts.get(int(user.id), 0) + 1
                track_upload_task(user.id, asyncio.create_task(_run_single_upload_task(client, message)))
                schedule_upload_confirmation(client, user.id, message)
                return
        await _upload_single_handler(client, message)
        return
    user = message.from_user
    if not user:
        return
    if await is_admin(user.id):
        active_batch = await db.get_upload_batch(user.id)
        selected_category = active_batch["category"] if active_batch else pending_upload_categories.get(user.id)
        if selected_category:
            upload_received_counts[int(user.id)] = upload_received_counts.get(int(user.id), 0) + 1
    key = (int(user.id), int(message.chat.id), str(group_id))
    pending_album_uploads.setdefault(key, []).append(message)
    scheduled = pending_album_uploads.get((key, "scheduled"))
    if scheduled and not scheduled.done():
        scheduled.cancel()
    task = asyncio.create_task(_run_album_upload_task(client, key))
    pending_album_uploads[(key, "scheduled")] = task
    track_upload_task(user.id, task)
    if await is_admin(user.id):
        active_batch = await db.get_upload_batch(user.id)
        selected_category = active_batch["category"] if active_batch else pending_upload_categories.get(user.id)
        if selected_category:
            schedule_upload_confirmation(client, user.id, message)


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
    if int(user.id) in broadcast_input_mode:
        await capture_broadcast_message(client, message)
        return
    if not await is_admin(user.id):
        # Preserve the existing normal-upload flow for non-admin users.
        await upload_handler(client, message)
        return

    # Only intercept an actual Cangku JSON export. Ordinary media uploads
    # must go straight to the silent batch pipeline without creating/deleting
    # temporary Telegram status messages.
    document = message.document
    if not document or not (document.file_name or "").lower().endswith(".json"):
        await upload_handler(client, message)
        return
    status = await message.reply_text("⏳ 已收到 JSON 文件，正在检查目录迁移清单…")
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
                "media_group_id": str(item.get("mediaGroupId") or "").strip(),
                "album_group_id": None,
                "album_position": 0,
            })
        # Preserve every known mediaGroupId. Telegram album members may have
        # non-consecutive message IDs; group identity and member count determine
        # eligibility, while the actual IDs are passed together to copyMessages.
        album_members = {}
        for resource in clean_resources:
            group_id = resource["media_group_id"]
            if group_id:
                group_key = f'{resource["chat_id"]}:{group_id}'
                album_members.setdefault(group_key, []).append(resource)
        for group_key, members in album_members.items():
            members.sort(key=lambda row: int(row["message_id"]))
            message_ids = [int(row["message_id"]) for row in members]
            complete_album = 2 <= len(members) <= 10 and len(set(message_ids)) == len(message_ids)
            prefix = "cangku:" if complete_album else "cangku-incomplete:"
            album_group_id = f"{prefix}{group_key}"
            for position, resource in enumerate(members):
                resource["album_group_id"] = album_group_id
                resource["album_position"] = position
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
        changed_categories = set()
        for index, item in enumerate(pending["resources"], 1):
            fingerprint = item["source_fingerprint"]
            try:
                # The unique source-fingerprint index makes a separate lookup
                # unnecessary; INSERT OR IGNORE handles duplicates atomically.
                file_key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
                inserted = await db.add_file(
                    file_key, user.id, item["file_name"], item["media_type"],
                    0, 0, item["category"], fingerprint,
                    item.get("file_size", 0), item.get("created_at"),
                    album_group_id=item.get("album_group_id"),
                    album_position=item.get("album_position", 0),
                    refresh_stats=False,
                )
                # Re-importing an updated export enriches existing rows with
                # album metadata without replacing their keys or other data.
                if item.get("album_group_id"):
                    await db.update_cangku_album_metadata(
                        fingerprint, item["album_group_id"], item["album_position"]
                    )
                if inserted:
                    changed_categories.add(item["category"])
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

        # Recalculate each affected folder once after the import, not once per file.
        await db.refresh_category_stats(changed_categories)
        category_catalog_cache["expires_at"] = 0
        # Preserve intentionally-created empty folders from the import or admin panel.
        await db.list_categories()

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


async def discover_cangku_album_metadata(candidate, force=False):
    """Use the logged-in scanner account to identify source albums before any single copy."""
    if scanner is None or not scanner.is_connected:
        return False
    fingerprint = str(candidate["source_fingerprint"] or "")
    parts = fingerprint.split(":", 2)
    if len(parts) != 3 or parts[0] != "cangku":
        return False
    loop = asyncio.get_running_loop()
    if not force and scanner_metadata_cooldowns.get(fingerprint, 0.0) > loop.time():
        return False
    try:
        chat_id, message_id = int(parts[1]), int(parts[2])
        source_message = await scanner_api_call(scanner.get_messages, chat_id, message_id)
        if not source_message or getattr(source_message, "empty", False):
            raise RuntimeError("Scanner could not read the original Cangku message")
        group_id = getattr(source_message, "media_group_id", None)
        if not group_id:
            # Explicitly mark verified singles so future idle cycles do not re-read them.
            await db.update_cangku_album_metadata(
                fingerprint, f"cangku-single:{chat_id}:{message_id}", 0
            )
            return True
        group = await scanner_api_call(scanner.get_media_group, chat_id, message_id)
        if not 2 <= len(group) <= 10 or any(
            str(getattr(item, "media_group_id", "")) != str(group_id)
            for item in group
        ):
            raise RuntimeError("Telegram did not return a complete, consistent source album")
        group = sorted(group, key=lambda item: int(item.id))
        ids = [int(item.id) for item in group]
        if len(set(ids)) != len(ids):
            raise RuntimeError("Telegram returned duplicate source album message IDs")
        # Repair incomplete legacy indexes from the Telegram-confirmed source album.
        # A missing imported row must not make every member of a real album permanently
        # unbackable. Insert only missing source fingerprints; existing file keys,
        # backup IDs, user history, and other records remain untouched.
        album_key = f"cangku:{chat_id}:{group_id}"
        existing_rows = []
        for source_id in ids:
            row = await db.get_file_by_fingerprint(f"cangku:{chat_id}:{source_id}")
            if row:
                existing_rows.append(row)
        inherited_category = next(
            (str(row["category"] or "") for row in existing_rows if row["category"]),
            "",
        )
        category = inherited_category or parse_chinese_album_category(group) or "未分类"
        repaired = 0
        for position, member in enumerate(group):
            source_id = int(member.id)
            source_fp = f"cangku:{chat_id}:{source_id}"
            row = await db.get_file_by_fingerprint(source_fp)
            if not row:
                media_type, file_name, _ = media_details(member)
                if not media_type:
                    raise RuntimeError(
                        f"Telegram-confirmed album member {source_id} has no supported media"
                    )
                media = getattr(member, media_type, None)
                created_at = (
                    member.date.isoformat() if getattr(member, "date", None)
                    else datetime.now(timezone.utc).isoformat()
                )
                await db.add_file(
                    secrets.token_urlsafe(9).replace("-", "").replace("_", ""),
                    config.ADMIN_ID, file_name or f"{media_type}_{source_id}",
                    media_type, source_id, 0, category, source_fp,
                    getattr(media, "file_size", 0) or 0, created_at,
                    album_group_id=album_key, album_position=position,
                    refresh_stats=False,
                )
                row = await db.get_file_by_fingerprint(source_fp)
                if not row:
                    raise RuntimeError(
                        f"Could not restore missing album index row for source message {source_id}"
                    )
                repaired += 1
            await db.update_cangku_album_metadata(source_fp, album_key, position)
        scanner_metadata_cooldowns.pop(fingerprint, None)
        log.info(
            "Discovered Cangku source album via scanner: group=%s files=%s repaired_index_rows=%s",
            album_key, len(ids), repaired,
        )
        return True
    except Exception:
        scanner_metadata_cooldowns[fingerprint] = loop.time() + 900
        log.exception("Could not safely identify Cangku source media group: %s", fingerprint)
        # Do not let an unreadable/partial source fall through to copyMessage.
        try:
            await db.update_cangku_album_metadata(
                fingerprint, f"cangku-incomplete:{parts[1]}:{parts[2]}", 0
            )
        except Exception:
            log.exception("Could not mark unverified Cangku source as incomplete")
        return False


async def find_deleted_source_messages(chat_id, message_ids):
    """Return source IDs Telegram confirms are missing/empty; API errors still raise."""
    if scanner is None or not scanner.is_connected:
        raise RuntimeError("扫描账号尚未连接，无法核实原始消息是否存在")
    requested = [int(value) for value in message_ids]
    if not requested:
        return []
    result = await scanner_api_call(
        scanner.get_messages,
        int(chat_id),
        requested if len(requested) > 1 else requested[0],
    )
    messages = result if isinstance(result, list) else [result]
    found_ids = set()
    for message in messages:
        if message is None or getattr(message, "empty", False):
            continue
        message_id = getattr(message, "id", None)
        if message_id is not None:
            found_ids.add(int(message_id))
    return [message_id for message_id in requested if message_id not in found_ids]


async def settings_retention_worker():
    """Drop stale page cursors and CAPTCHA state so the settings table stays bounded."""
    while True:
        try:
            removed = await db.purge_stale_settings(
                TEMP_SETTING_PREFIXES, SETTING_RETENTION_SECONDS
            )
            if removed:
                log.info("Purged %s stale per-user setting rows", removed)
        except Exception:
            log.exception("Setting retention pass failed")
        await asyncio.sleep(SETTING_CLEANUP_INTERVAL)


async def backup_retry_worker(client):
    """Resume durable jobs and queue Cangku singles/albums only while idle."""
    global backup_retry_manual_requested, scan_task
    consecutive_cycle_failures = 0
    while True:
        try:
            if await db.get_setting("backup_retry_enabled") == "0":
                await asyncio.sleep(5)
                continue
            manual_requested = bool(backup_retry_manual_requested)
            cycle_failure_notices = []
            loop = asyncio.get_running_loop()
            busy = (
                directory_import_running
                or bool(scan_task and not scan_task.done())
                or bool(category_delivery_in_progress)
                or bool(cangku_backup_in_progress)
                or bool(upload_finish_in_progress)
                or bool(pending_album_uploads)
                or any(tasks for tasks in pending_upload_tasks.values())
                or upload_copy_semaphore._value < 2
                or (not manual_requested and loop.time() - float(last_user_activity or 0) < 30)
            )
            if busy:
                await asyncio.sleep(5)
                continue
            if manual_requested:
                backup_retry_manual_requested = False

            # A full restart is a two-phase operation: complete a fresh source
            # history scan first, then freeze that indexed set into durable retry jobs.
            primary_channel_id, backup_channel_id = await get_storage_channels()
            full_restart_pending = (
                backup_channel_id
                and await db.get_setting("backup_retry_full_restart_pending") == "1"
            )
            if full_restart_pending:
                if scanner is None or not scanner.is_connected:
                    log.warning("Full backup restart is waiting for the scanner account to connect")
                    await asyncio.sleep(5)
                    continue
                scan_started = await db.get_setting("backup_full_scan_started")
                if scan_started != "1":
                    if not primary_channel_id:
                        log.error("Full backup restart cannot scan: primary channel is not bound")
                        await asyncio.sleep(10)
                        continue
                    check = await inspect_scan_channel(primary_channel_id)
                    if not check["ok"]:
                        log.error("Full backup restart is waiting for source channel access: %s", format_scan_check(check, "主存储频道"))
                        await asyncio.sleep(10)
                        continue
                    await db.reset_scan_progress_for_full_rescan(primary_channel_id)
                    await db.set_setting("backup_full_scan_started", "1")
                    scan_task = spawn_background(
                        "history-scan", run_history_scan(primary_channel_id)
                    )
                    log.warning(
                        "Full backup restart: fresh source history scan started at offset_id=0 "
                        "for channel=%s",
                        primary_channel_id,
                    )
                    await asyncio.sleep(2)
                    continue
                scan_progress = await db.get_scan_progress(primary_channel_id)
                if not scan_progress["completed"]:
                    # A failed/paused scan never becomes a partial backup manifest.
                    await asyncio.sleep(5)
                    continue
                reset_count = await db.restart_backup_retry_queue(
                    backup_channel_id, full_restart=True
                )
                await db.set_setting("backup_full_scan_required", "0")
                await db.set_setting("backup_full_scan_started", "0")
                await db.set_setting("backup_retry_full_restart_pending", "0")
                log.warning(
                    "Full backup scan completed; locked %s indexed source items into the durable copy queue",
                    reset_count,
                )
            # Strict two-phase backup: do not enqueue OR deliver any source item
            # until the complete primary-channel history scan has finished.
            # This also pauses previously queued jobs if a scan is resumed/interrupted.
            if backup_channel_id and primary_channel_id:
                source_scan_progress = await db.get_scan_progress(primary_channel_id)
                if not source_scan_progress.get("completed"):
                    await asyncio.sleep(5)
                    continue

            if backup_channel_id:
                # Only after the full source manifest is complete may messages
                # be forwarded to the backup channel with their original source.
                candidates = await db.list_unbacked_cangku_files(limit=1)
                for candidate in candidates:
                    fingerprint = str(candidate["source_fingerprint"] or "")
                    parts = fingerprint.split(":", 2)
                    if len(parts) != 3:
                        continue
                    try:
                        source_chat_id, source_message_id = int(parts[1]), int(parts[2])
                    except ValueError:
                        continue
                    # Re-check every legacy "single" against Telegram before
                    # queueing copyMessage. Older scans/imports may have lost the
                    # media_group_id; blindly copying here would split a real album.
                    refreshed = await db.get_file(str(candidate["file_key"]))
                    group_id = str(refreshed["album_group_id"] or "") if refreshed else ""
                    if group_id.startswith("cangku-single:"):
                        verified = await discover_cangku_album_metadata(candidate)
                        if not verified:
                            log.warning(
                                "Backup candidate held until source media type/group is verified: file=%s",
                                candidate["file_key"],
                            )
                            continue
                        refreshed = await db.get_file(str(candidate["file_key"]))
                        group_id = str(refreshed["album_group_id"] or "") if refreshed else ""
                    if group_id.startswith("cangku-single:"):
                        await db.enqueue_backup_retry(
                            str(candidate["file_key"]), source_chat_id,
                            source_message_id, int(backup_channel_id),
                            "Bot API direct Cangku single backup (source-verified)",
                        )
                    elif group_id.startswith("cangku:"):
                        log.info(
                            "Promoted legacy backup candidate to complete album group: file=%s group=%s",
                            candidate["file_key"], group_id,
                        )

                # Never queue a partial album while an oldest-first history scan
                # is incomplete. A failed scan can resume later without splitting it.
                scan_progress = (
                    await db.get_scan_progress(primary_channel_id)
                    if primary_channel_id else {"completed": 0}
                )
                album_candidates = (
                    await db.list_unbacked_cangku_albums(limit=1)
                    if scan_progress.get("completed") else []
                )
                for candidate in album_candidates:
                    fingerprint = str(candidate["source_fingerprint"] or "")
                    parts = fingerprint.split(":", 2)
                    if len(parts) != 3:
                        continue
                    try:
                        source_chat_id, source_message_id = int(parts[1]), int(parts[2])
                    except ValueError:
                        continue
                    await db.enqueue_backup_retry(
                        str(candidate["file_key"]), source_chat_id,
                        source_message_id, int(backup_channel_id),
                        "Bot API direct Cangku album backup",
                    )

            jobs = await db.list_due_backup_retries(limit=5)
            for pending_job in jobs:
                # A full restart may be requested while this worker is already
                # holding a batch of due jobs. Stop before claiming the next old
                # job; the next loop will run the full scan/rebuild phase first.
                if await db.get_setting("backup_retry_full_restart_pending") == "1":
                    log.warning(
                        "Stopping the old backup queue because a full restart was requested"
                    )
                    break

                # Re-check activity before every copy so member requests always win.
                loop = asyncio.get_running_loop()
                if (
                    directory_import_running
                    or bool(scan_task and not scan_task.done())
                    or bool(category_delivery_in_progress)
                    or bool(upload_finish_in_progress)
                    or any(tasks for tasks in pending_upload_tasks.values())
                    or (
                        not manual_requested
                        and loop.time() - float(last_user_activity or 0) < 30
                    )
                ):
                    break
                file_key = str(pending_job["file_key"])
                pending_record = await db.get_file(file_key)
                pending_album_group = (
                    str(pending_record["album_group_id"] or "")
                    if pending_record else ""
                )
                if pending_album_group.startswith("cangku:"):
                    scan_progress = (
                        await db.get_scan_progress(primary_channel_id)
                        if primary_channel_id else {"completed": 0}
                    )
                    if not scan_progress.get("completed"):
                        # Leave the durable album job untouched until the full
                        # source index is complete; never copy a partial group.
                        continue
                job = await db.claim_backup_retry(file_key, lease_seconds=300)
                if not job:
                    continue
                try:
                    record = await db.get_file(file_key)
                    if not record or int(record["backup_message_id"] or 0) > 0:
                        await db.complete_backup_retry(file_key)
                        continue
                    source_fingerprint = str(record["source_fingerprint"] or "")
                    album_group_id = str(record["album_group_id"] or "")
                    if source_fingerprint.startswith("cangku:") and (
                        not album_group_id
                        or album_group_id.startswith("cangku-incomplete:")
                        or album_group_id.startswith("cangku-single:")
                    ):
                        # Repair legacy/missing album metadata with the already
                        # logged-in personal scanner before deciding whether this
                        # source is a single message or a media group. Never guess.
                        verified = await discover_cangku_album_metadata(record, force=True)
                        if not verified:
                            raise RuntimeError(
                                "Source album metadata could not be verified; waiting for scanner access"
                            )
                        record = await db.get_file(file_key)
                        if not record:
                            raise RuntimeError("Source index row disappeared during album metadata repair")
                        source_fingerprint = str(record["source_fingerprint"] or "")
                        album_group_id = str(record["album_group_id"] or "")
                        if not album_group_id or album_group_id.startswith("cangku-incomplete:"):
                            raise RuntimeError(
                                "Source album metadata remains incomplete after scanner verification"
                            )
                    if album_group_id.startswith("cangku-incomplete:"):
                        raise RuntimeError(
                            "Cangku album metadata is incomplete; refusing to back it up as separate files"
                        )
                    if album_group_id.startswith("cangku:"):
                        album_rows = await db.get_cangku_album_backup_group(
                            album_group_id
                        )
                        if not 2 <= len(album_rows) <= 10:
                            raise RuntimeError("Album metadata is incomplete; refusing to split the source group")
                        source_chat_ids = set()
                        source_message_ids = []
                        for album_row in album_rows:
                            source_fingerprint = str(album_row["source_fingerprint"] or "")
                            source_parts = source_fingerprint.split(":", 2)
                            if len(source_parts) != 3 or not source_fingerprint.startswith("cangku:"):
                                raise RuntimeError("Album contains a missing or non-Cangku source pointer")
                            source_chat_ids.add(int(source_parts[1]))
                            source_message_ids.append(int(source_parts[2]))
                        if len(source_chat_ids) != 1:
                            raise RuntimeError("Album source messages belong to different chats")
                        if len(set(source_message_ids)) != len(source_message_ids):
                            raise RuntimeError("Album source message IDs contain duplicates")
                        existing_backup_ids = [
                            int(row["backup_message_id"] or 0) for row in album_rows
                        ]
                        source_chat_id = source_chat_ids.pop()
                        backup_chat_id = int(job["backup_channel_id"])

                        # A partially completed legacy album must never be repaired
                        # with forwardMessage: that would leave its final members as
                        # standalone posts. Re-forward the complete source album in
                        # one forwardMessages call, then replace every saved mapping.
                        needs_album_repair = not all(value > 0 for value in existing_backup_ids)
                        if all(value > 0 for value in existing_backup_ids):
                            # Verify existing destination messages when the scanner can
                            # read them. If verification is unavailable, preserve the
                            # existing mappings rather than creating duplicate backups.
                            try:
                                destination_group = await scanner_api_call(
                                    scanner.get_media_group,
                                    chat_id=backup_chat_id,
                                    message_id=existing_backup_ids[0],
                                )
                                destination_ids = {
                                    int(item.id) for item in (destination_group or [])
                                }
                                needs_album_repair = not (
                                    2 <= len(destination_group or []) <= 10
                                    and set(existing_backup_ids).issubset(destination_ids)
                                    and len({
                                        str(getattr(item, "media_group_id", "") or "")
                                        for item in destination_group
                                    }) == 1
                                    and bool(getattr(destination_group[0], "media_group_id", None))
                                )
                            except Exception:
                                log.warning(
                                    "Could not verify existing backup album; preserving mappings: group=%s",
                                    album_group_id,
                                    exc_info=True,
                                )
                                needs_album_repair = False

                        if not needs_album_repair:
                            await db.complete_backup_retry(file_key)
                            continue

                        # Confirm every original album member still exists before
                        # forwarding. Telegram returns empty/missing messages for
                        # deleted source posts; never split or recreate a partial album.
                        deleted_source_ids = await find_deleted_source_messages(
                            source_chat_id, source_message_ids
                        )
                        if deleted_source_ids:
                            raise RuntimeError(
                                "原始相册消息已被删除或不存在，整组放弃转发；缺失消息 ID："
                                + "、".join(str(value) for value in deleted_source_ids)
                            )

                        # The scanner user account can read the primary channel;
                        # the bot account may not be a member and would get
                        # Bot API 400 "chat not found". Forward albums with the
                        # same authorized scanner account used for history scans.
                        if scanner is None or not scanner.is_connected:
                            raise RuntimeError(
                                "Scanner account is not connected; cannot forward source album"
                            )
                        # Warm the scanner account's private-channel peer cache for
                        # the destination too. Knowing only the numeric channel ID is
                        # insufficient for Pyrogram to address a private channel.
                        await resolve_scanner_chat(backup_chat_id)
                        copied = await scanner_api_call(
                            scanner.forward_messages,
                            chat_id=backup_chat_id,
                            from_chat_id=source_chat_id,
                            message_ids=source_message_ids,
                        )
                        if not isinstance(copied, list) or len(copied) != len(source_message_ids):
                            raise RuntimeError(
                                f"Complete album forward returned "
                                f"{len(copied) if isinstance(copied, list) else 0} "
                                f"messages for {len(source_message_ids)} source items"
                            )
                        copied_ids = [
                            int(item.id if hasattr(item, "id") else item["message_id"])
                            for item in copied
                        ]

                        # Remove old standalone/partial copies only after the complete
                        # album has been forwarded successfully. A delete failure is
                        # non-fatal; the new complete album mapping is still saved.
                        old_backup_ids = [value for value in existing_backup_ids if value > 0]
                        if old_backup_ids:
                            try:
                                await telegram_bot_api("deleteMessages", {
                                    "chat_id": backup_chat_id,
                                    "message_ids": old_backup_ids,
                                })
                            except Exception:
                                log.warning(
                                    "Could not remove old standalone album copies: group=%s ids=%s",
                                    album_group_id, old_backup_ids, exc_info=True,
                                )

                        await db.update_album_backup_message_ids(
                            album_group_id, copied_ids, replace_existing=True
                        )
                        await db.complete_backup_retry(file_key)
                        log.info(
                            "Idle/durable album backup completed: group=%s files=%s",
                            album_group_id, len(copied_ids),
                        )
                    else:
                        # Verify the original post immediately before forwarding.
                        # Deleted/empty source messages are terminal skips, not retries.
                        source_parts = source_fingerprint.split(":", 2)
                        if len(source_parts) == 3 and source_parts[0] == "cangku":
                            deleted_source_ids = await find_deleted_source_messages(
                                int(source_parts[1]), [int(source_parts[2])]
                            )
                            if deleted_source_ids:
                                raise RuntimeError(
                                    "原始消息已被删除或不存在，直接放弃转发；消息 ID："
                                    + str(deleted_source_ids[0])
                                )

                        # Use the same logged-in scanner account for source access
                        # and forwarding. The bot account may not be a member of the
                        # private primary channel, causing Bot API "chat not found".
                        if scanner is None or not scanner.is_connected:
                            raise RuntimeError(
                                "Scanner account is not connected; cannot forward source file"
                            )
                        # Resolve the destination as well as the source so the
                        # scanner session has the private channel access_hash cached.
                        backup_chat_id = int(job["backup_channel_id"])
                        await resolve_scanner_chat(backup_chat_id)
                        copied = await scanner_api_call(
                            scanner.forward_messages,
                            chat_id=backup_chat_id,
                            from_chat_id=int(job["primary_channel_id"]),
                            message_ids=int(job["primary_message_id"]),
                        )
                        if copied is None or not getattr(copied, "id", None):
                            raise RuntimeError(
                                "Scanner did not return a valid forwarded source message"
                            )
                        copied_message_id = int(copied.id)
                        updated = await db.update_backup_message_id(file_key, copied_message_id)
                        if not updated:
                            latest = await db.get_file(file_key)
                            if not latest or int(latest["backup_message_id"] or 0) <= 0:
                                raise RuntimeError("Backup copy succeeded but database update was not confirmed")
                        await db.complete_backup_retry(file_key)
                        log.info(
                            "Idle/durable backup completed for file %s (message %s)",
                            file_key, copied_message_id,
                        )
                except Exception as exc:
                    error_text = f"{type(exc).__name__}: {str(exc)[:400]}"
                    try:
                        latest = await db.get_file(file_key)
                        source_fingerprint = str(latest["source_fingerprint"] or "") if latest else ""
                        album_group_id = str(latest["album_group_id"] or "") if latest else ""
                        source_ids = []
                        source_chat_id = int(job["primary_channel_id"])
                        if album_group_id.startswith("cangku:"):
                            failed_album_rows = await db.get_cangku_album_backup_group(album_group_id)
                            for album_row in failed_album_rows:
                                parts = str(album_row["source_fingerprint"] or "").split(":", 2)
                                if len(parts) == 3 and parts[0] == "cangku":
                                    source_chat_id = int(parts[1])
                                    source_ids.append(int(parts[2]))
                        if not source_ids:
                            parts = source_fingerprint.split(":", 2)
                            if len(parts) == 3 and parts[0] == "cangku":
                                source_chat_id = int(parts[1])
                                source_ids = [int(parts[2])]
                        if not source_ids:
                            source_ids = [int(job["primary_message_id"])]
                        file_name = str(latest["file_name"] or file_key) if latest else file_key
                        category = str(latest["category"] or "") if latest else ""
                        source_label = "相册整组" if album_group_id.startswith("cangku:") else "单条文件"
                        source_text = "、".join(str(value) for value in source_ids)
                        notice = (
                            "⚠️ 自动备份失败，已跳过并继续下一条\n\n"
                            f"📄 文件：{file_name}\n"
                            f"📁 目录：{category or '未分类'}\n"
                            f"类型：{source_label}\n"
                            f"来源聊天 ID：{source_chat_id}\n"
                            f"消息 ID：{source_text}\n"
                            f"文件编号：{file_key}\n"
                            f"失败原因：{error_text}\n\n"
                            "请管理员从原始来源手动转发到备用频道。"
                        )
                        # Persist the failure before removing the retry job. If
                        # the database write fails, leave the leased job to be retried
                        # after its lease expires instead of silently losing the report.
                        should_notify = await db.record_backup_failure(
                            file_key, file_name, category, source_fingerprint,
                            album_group_id, source_ids, error_text,
                        )
                        await db.complete_backup_retry(file_key)
                        if should_notify:
                            cycle_failure_notices.append(notice)
                    except Exception:
                        log.exception("Could not persist/report skipped backup file %s", file_key)
                    log.warning("Backup retry skipped after failure: file=%s error=%s", file_key, error_text)
                await asyncio.sleep(2.0)

            # Send one grouped admin DM per worker cycle, not one message per file.
            if cycle_failure_notices:
                summary = "⚠️ 自动备份失败汇总（本轮已跳过失败项）\n\n" + "\n\n".join(cycle_failure_notices)
                try:
                    await client.send_message(
                        int(config.ADMIN_ID), summary[:3900],
                        disable_web_page_preview=True,
                    )
                except Exception:
                    log.exception(
                        "Could not send grouped backup failure summary (%s items)",
                        len(cycle_failure_notices),
                    )
            consecutive_cycle_failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("Backup retry worker cycle failed")
            consecutive_cycle_failures += 1
            if consecutive_cycle_failures in (3, 10) or consecutive_cycle_failures % 20 == 0:
                schedule_alert(
                    "🚨 自动备份队列连续失败",
                    f"已连续 {consecutive_cycle_failures} 轮出错，备份队列可能已停摆。\n"
                    f"最新错误：{type(exc).__name__}: {exc}",
                    key="backup-cycle",
                )
        await asyncio.sleep(15)


async def _upload_single_handler(client, message):
    global BOT_USERNAME, BOT_ID
    user = message.from_user
    if not user:
        return
    await db.touch_user(user.id)
    if not await has_access(user.id):
        return

    media_type, file_name, source_fingerprint = media_details(message)
    if not media_type:
        return

    active_batch = await db.get_upload_batch(user.id) if await is_admin(user.id) else None
    selected_category = (
        active_batch["category"] if active_batch
        else pending_upload_categories.get(user.id) if await is_admin(user.id)
        else None
    )
    is_admin_batch = bool(await is_admin(user.id) and selected_category)
    if is_admin_batch:
        # Each Telegram message is a separate upload, even if its media bytes
        # match another message sent earlier in the same batch. Telegram may
        # redeliver the same update, so check the staging table before copying.
        source_fingerprint = f"upload:{message.chat.id}:{message.id}"
        if (
            await db.get_upload_batch_item_by_fingerprint(source_fingerprint)
            or await db.get_file_by_fingerprint(source_fingerprint)
        ):
            log.info(
                "Skipped already-staged Telegram upload: user=%s chat=%s message=%s",
                user.id, message.chat.id, message.id,
            )
            return
    elif source_fingerprint and await db.get_file_by_fingerprint(source_fingerprint):
        log.info("Silently skipped duplicate upload: user=%s file=%s", user.id, file_name)
        return

    primary_channel_id, backup_channel_id = await get_storage_channels()
    if not primary_channel_id or not backup_channel_id:
        if is_admin_batch:
            log.error("Upload batch missing primary/backup channel binding for user %s", user.id)
        else:
            await message.reply_text("⚠️ 网盘尚未完成主存储和备用存储绑定，请管理员先使用 /bindmain 和 /bindbackup。")
        return
    if primary_channel_id == backup_channel_id:
        if is_admin_batch:
            log.error("Upload batch primary and backup channels are identical for user %s", user.id)
        else:
            await message.reply_text("❌ 主存储和备用存储不能是同一个群/频道，请管理员检查 /storage。")
        return

    category = str(selected_category or parse_category(message))[:120]
    if await is_admin(user.id) and selected_category:
        if not active_batch:
            await db.start_upload_batch(user.id, category)
    is_admin_batch = bool(await is_admin(user.id) and selected_category)
    status = None if is_admin_batch else await message.reply_text("⏳ 正在检查存储权限并保存文件…")
    primary_id = None
    backup_id = 0
    step = "检查主频道权限"
    try:
        # Startup already caches the bot identity. Reuse it for every upload
        # instead of calling users.GetFullUser repeatedly and triggering FloodWait.
        if not BOT_ID or not BOT_USERNAME:
            me = await client.get_me()
            BOT_ID = BOT_ID or me.id
            BOT_USERNAME = BOT_USERNAME or me.username
        bot_id = BOT_ID
        if not bot_id:
            raise RuntimeError("机器人身份尚未初始化，请稍后重试")
        await validate_storage_chat(primary_channel_id, "primary")
        await validate_storage_chat(backup_channel_id, "backup")

        step = "保存主频道"
        primary = await telegram_bot_api("copyMessage", {
            "chat_id": primary_channel_id,
            "from_chat_id": message.chat.id,
            "message_id": message.id,
        })
        primary_id = int(primary["message_id"])
        await queue_storage_message_caption(
            primary_channel_id, primary_id, folder_storage_caption(category)
        )

        step = "保存备用存储"
        backup_error = None
        try:
            backup = await telegram_bot_api("copyMessage", {
                "chat_id": backup_channel_id,
                "from_chat_id": message.chat.id,
                "message_id": message.id,
            })
            backup_id = int(backup["message_id"])
            await queue_storage_message_caption(
                backup_channel_id, backup_id, folder_storage_caption(category)
            )
        except Exception as exc:
            backup_error = exc
            log.exception("Backup-channel copy failed; keeping primary copy")

        step = "写入文件数据库"
        key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
        media_object = getattr(message, media_type, None)
        file_size = int(getattr(media_object, "file_size", 0) or 0)
        if await is_admin(user.id) and selected_category:
            inserted = await db.stage_upload_batch_item(
                key, user.id, file_name, media_type, category,
                primary_channel_id, primary_id, backup_channel_id, backup_id,
                source_fingerprint, file_size,
                f"{type(backup_error).__name__}: {str(backup_error)[:400]}" if backup_error else "",
            )
        else:
            inserted = await db.add_file(
                key, user.id, file_name, media_type,
                primary_id, backup_id, category, source_fingerprint,
                file_size=file_size,
            )
        if inserted and not (await is_admin(user.id) and selected_category):
            category_catalog_cache["expires_at"] = 0
        if not inserted:
            log.info(
                "Silently skipped concurrent duplicate upload: user=%s file=%s",
                user.id, file_name,
            )
            if status is not None:
                try:
                    await status.delete()
                except Exception:
                    log.debug("Could not delete duplicate-upload status message")
            return

        if is_admin_batch:
            return

        step = "生成分享链接"
        link = share_url(key)
        if backup_error:
            await db.enqueue_backup_retry(
                key, primary_channel_id, primary_id, backup_channel_id,
                f"{type(backup_error).__name__}: {str(backup_error)[:400]}",
            )
            await status.edit_text(
                "⚠️ 文件已保存到主频道，分享链接已生成；备份任务已加入持久化重试队列。\n"
                f"📂 分类：{category}\n📄 文件：{file_name}\n\n"
                f"🔗 分享链接：\n{link}\n\n"
                f"备份原因：{type(backup_error).__name__}: {str(backup_error)[:250]}",
                disable_web_page_preview=True,
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
        record_upload_failure(user.id, file_name, step, exc)
        # Never delete a successfully stored channel copy on a later-stage error.
        detail = f"{type(exc).__name__}: {str(exc)[:350]}"
        if status is not None:
            try:
                await status.edit_text(
                    f"❌ 文件保存失败（步骤：{step}）\n{detail}\n\n"
                    + (f"主频道消息已保存，消息 ID：{primary_id}。请管理员检查数据库/分享链接步骤；未删除该副本。"
                       if primary_id is not None else
                       "主频道尚未确认保存成功。请管理员检查频道管理员身份、发布消息权限及 Telegram 网络。")
                )
            except Exception:
                log.exception("Could not report single-file upload failure")


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
    if is_group_binding:
        allowed_types = (ChatType.GROUP, ChatType.SUPERGROUP)
    elif setting_key == "storage_primary_channel_id":
        allowed_types = (ChatType.CHANNEL,)
    else:
        allowed_types = (ChatType.CHANNEL, ChatType.GROUP, ChatType.SUPERGROUP)
    if source.type not in allowed_types:
        expected = "会员群" if is_group_binding else ("私有频道" if setting_key == "storage_primary_channel_id" else "频道或群组")
        await message.reply_text(f"❌ 来源类型不正确，请转发{expected}中的普通消息。")
        return

    if setting_key in ("storage_primary_channel_id", "storage_backup_channel_id"):
        try:
            chat = await validate_storage_chat(source.id, "primary" if setting_key == "storage_primary_channel_id" else "backup")
        except Exception as exc:
            await message.reply_text(f"❌ 绑定验证失败：{str(exc)[:300]}")
            return
        other_key = "storage_backup_channel_id" if setting_key == "storage_primary_channel_id" else "storage_primary_channel_id"
        other_id = await db.get_setting(other_key)
        if other_id and int(other_id) == source.id:
            await message.reply_text("❌ 主存储和备用存储不能绑定为同一个群/频道。")
            return
    else:
        me = await client.get_me()
        try:
            member = await client.get_chat_member(source.id, me.id)
        except Exception:
            await message.reply_text("❌ 机器人无法访问这个会员群，请先把机器人添加到群内。")
            return
        valid_statuses = (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER, ChatMemberStatus.RESTRICTED)
        if member.status not in valid_statuses:
            await message.reply_text("❌ 机器人尚未加入这个会员群，请先添加后重新绑定。")
            return

    await db.set_setting(setting_key, str(source.id))
    pending_channel_bindings.pop(user.id, None)
    labels = {
        "storage_primary_channel_id": "主存储频道",
        "storage_backup_channel_id": "备用存储（频道/群组）",
        "member_group_id": "会员群",
    }
    label = labels.get(setting_key, "目标群/频道")
    await message.reply_text(
        f"✅ {label}绑定成功！\n"
        f"名称：{source.title or '未命名'}\n"
        f"ID：{source.id}\n\n"
        "可发送 /storage 查看绑定状态，或 /admin 打开管理面板。"
    )


async def bind_storage_by_id(message, setting_key, raw_chat_id):
    raw_chat_id = str(raw_chat_id or "").strip()
    if not raw_chat_id or not raw_chat_id.lstrip("-").isdigit():
        await message.reply_text("❌ Chat ID 格式不正确，请发送类似 -1001234567890 的数字 ID。")
        return
    chat_id = int(raw_chat_id)
    if chat_id >= 0:
        await message.reply_text("❌ 请填写 Telegram 群组/频道的负数 Chat ID（通常以 -100 开头）。")
        return
    if setting_key == "storage_primary_channel_id":
        role = "primary"
    else:
        role = "backup"
    try:
        chat = await validate_storage_chat(chat_id, role)
    except Exception as exc:
        await message.reply_text(f"❌ 绑定验证失败：{str(exc)[:300]}\n请确认 ID 正确，并已将机器人设为管理员。")
        return
    other_key = "storage_backup_channel_id" if setting_key == "storage_primary_channel_id" else "storage_primary_channel_id"
    other_id = await db.get_setting(other_key)
    if other_id and int(other_id) == chat_id:
        await message.reply_text("❌ 主存储和备用存储不能是同一个群/频道。")
        return
    await db.set_setting(setting_key, str(chat_id))
    pending_channel_bindings.pop(message.from_user.id, None)
    label = "主存储频道" if role == "primary" else "备用存储"
    await message.reply_text(
        f"✅ {label}绑定成功！\n名称：{chat.get('title') or '未命名'}\n"
        f"类型：{chat.get('type')}\nChat ID：{chat_id}\n\n"
        "可发送 /storage 查看绑定状态。"
    )


@app.on_message(filters.private & filters.command("bindmain"))
async def bind_main_channel_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    args = message.command[1:] if len(message.command) > 1 else []
    if args:
        await bind_storage_by_id(message, "storage_primary_channel_id", args[0])
        return
    pending_channel_bindings[user.id] = "storage_primary_channel_id"
    await message.reply_text(
        "📥 绑定主存储频道：\n"
        "直接发送频道 Chat ID（例如 -1001234567890），或把频道普通消息转发给我。\n"
        "机器人必须是该频道管理员并拥有发布消息权限。"
    )


@app.on_message(filters.private & filters.command("bindbackup"))
async def bind_backup_channel_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    args = message.command[1:] if len(message.command) > 1 else []
    if args:
        await bind_storage_by_id(message, "storage_backup_channel_id", args[0])
        return
    pending_channel_bindings[user.id] = "storage_backup_channel_id"
    await message.reply_text(
        "📥 绑定备用存储：\n"
        "发送频道或群组 Chat ID（例如 -1001234567890），也可以直接转发其中一条普通消息。\n"
        "机器人必须先加入目标并设为管理员。"
    )


@app.on_message(filters.private & filters.text & ~filters.forwarded & pending_binding_filter)
async def bind_storage_id_text_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        return
    setting_key = pending_channel_bindings.get(user.id)
    if setting_key not in ("storage_primary_channel_id", "storage_backup_channel_id"):
        return
    await bind_storage_by_id(message, setting_key, message.text)


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


@app.on_message(filters.group & filters.new_chat_members)
async def welcome_new_members_handler(client, message):
    group_id = await get_member_group_id()
    if not group_id or message.chat.id != group_id:
        return

    enabled, template = await get_welcome_settings()
    if not enabled:
        return

    new_members = getattr(message, "new_chat_members", None) or []
    me_id = BOT_ID
    if not me_id:
        try:
            me_id = (await client.get_me()).id
        except Exception:
            me_id = None

    names = []
    for member in new_members:
        if getattr(member, "is_bot", False):
            continue
        if me_id and getattr(member, "id", None) == me_id:
            continue
        first_name = " ".join((getattr(member, "first_name", "") or "").split())[:32]
        names.append(first_name or "新成员")

    if not names:
        return

    bot_url = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else ""
    text = render_welcome_message(template, names, bot_url)
    sent = None
    try:
        sent = await message.reply_text(
            text, parse_mode=None, disable_web_page_preview=True
        )
    except Exception:
        log.exception("Could not send welcome message to group %s", group_id)
        return

    # The greeting is only meant to catch the newcomer's eye; keeping dozens of
    # them in the group history makes the member list unreadable.
    delete_after = await get_welcome_delete_seconds()
    if not delete_after or sent is None or not getattr(sent, "message_id", None):
        return
    delete_at = (
        datetime.now(timezone.utc) + timedelta(seconds=delete_after)
    ).isoformat()
    try:
        await db.add_pending_deletions(group_id, [sent.message_id], delete_at)
    except Exception:
        log.exception("Could not persist welcome auto-deletion: group=%s", group_id)
    spawn_background(
        "welcome-delete",
        _delete_later(client, group_id, [sent.message_id], delete_at),
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


SCANNER_QR_POLL_INTERVAL = 3.0
SCANNER_QR_REFRESH_MARGIN = 10.0


def _scanner_qr_deep_link(token: bytes) -> str:
    token_b64 = base64.urlsafe_b64encode(token).rstrip(b"=").decode()
    return f"tg://login?token={token_b64}"


def _render_scanner_qr_png(link: str) -> io.BytesIO:
    qr = qrcode.QRCode(box_size=6, border=2)
    qr.add_data(link)
    qr.make(fit=True)
    buf = io.BytesIO()
    qr.make_image(fill_color="black", back_color="white").save(buf, format="PNG")
    buf.seek(0)
    return buf


async def _activate_scanner_session(message, login_client):
    """Validate the authorized login client and persist its session string."""
    global scanner
    user = message.from_user
    candidate_scanner = None
    try:
        me = await login_client.get_me()
        if getattr(me, "is_bot", False):
            await login_client.log_out()
            await message.reply_text("❌ 不能使用机器人账号，请使用普通 Telegram 用户账号。")
            return
        session_string = await login_client.export_session_string()
        if not session_string:
            raise RuntimeError("Pyrogram returned an empty scanner session string")
        # First prove that the exported string can independently restore an
        # authorized Pyrogram client. Do not report success based on export alone.
        await login_client.disconnect()
        candidate_scanner = Client(
            "member_cloud_scanner",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=session_string,
        )
        await candidate_scanner.start()
        scanner_me = await candidate_scanner.get_me()
        if scanner_me.is_bot:
            raise RuntimeError("Scanner login must be a normal user account")
        # Persist only after the independent restore test succeeds, then read
        # the value back from the same database used by the next deployment.
        await db.set_setting(SCANNER_SESSION_SETTING_KEY, session_string)
        persisted_session = await db.get_setting(SCANNER_SESSION_SETTING_KEY)
        if persisted_session != session_string:
            raise RuntimeError("Scanner session database write/read verification failed")
        scanner = candidate_scanner
        log.info("Scanner session persisted and read-back verified for user %s", scanner_me.id)
        scanner_login_clients.pop(user.id, None)
        scanner_login_states.pop(user.id, None)
        await message.reply_text(
            "✅ 扫描账号登录成功！\n"
            f"账号 ID：{scanner_me.id}\n"
            "登录会话已安全保存到机器人数据库，后续重新部署会自动恢复，不需要重复登录。\n\n"
            "现在可以发送 /scan 只扫描主存储频道。"
        )
    except Exception:
        log.exception("Could not activate or persist scanner after QR login")
        try:
            await login_client.disconnect()
        except Exception:
            pass
        try:
            if candidate_scanner is not None and candidate_scanner.is_connected:
                await candidate_scanner.stop()
        except Exception:
            log.exception("Could not stop unpersisted scanner validation client")
        scanner = None
        scanner_login_clients.pop(user.id, None)
        scanner_login_states.pop(user.id, None)
        await message.reply_text(
            "⚠️ 验证通过，但扫描账号启动失败。请检查部署日志；可重新部署后自动恢复，必要时再执行 /scanlogin。"
        )


async def _complete_scanner_dc_migration(login_client, result):
    """Complete Telegram's QR-login DC migration and move the live client session."""
    if not isinstance(result, raw.types.auth.LoginTokenMigrateTo):
        return result

    source_session = login_client.session
    dc_id = int(result.dc_id)
    test_mode = await login_client.storage.test_mode()
    target_session = Session(
        login_client,
        dc_id,
        await Auth(login_client, dc_id, test_mode).create(),
        test_mode,
    )
    try:
        await target_session.start()
        try:
            migrated = await target_session.invoke(
                raw.functions.auth.ImportLoginToken(token=result.token)
            )
        except Exception as exc:
            if exc.__class__.__name__ != "SessionPasswordNeeded":
                raise
            # The QR token is accepted, but Telegram requires the account's
            # 2FA password on this migrated DC. Keep this authorized-in-progress
            # session attached so Client.check_password() can finish SRP auth.
            await source_session.stop()
            await login_client.storage.dc_id(dc_id)
            await login_client.storage.auth_key(target_session.auth_key)
            await login_client.storage.user_id(0)
            await login_client.storage.is_bot(False)
            await login_client.storage.date(0)
            await login_client.storage.save()
            login_client.session = target_session
            log.info("Scanner QR login requires the account 2FA password on DC %s", dc_id)
            raise
        if not isinstance(migrated, raw.types.auth.LoginTokenSuccess):
            raise RuntimeError(
                "Telegram did not complete QR login after DC migration: "
                + type(migrated).__name__
            )

        user = migrated.authorization.user
        await source_session.stop()
        await login_client.storage.dc_id(dc_id)
        await login_client.storage.auth_key(target_session.auth_key)
        await login_client.storage.user_id(user.id)
        await login_client.storage.is_bot(bool(user.bot))
        await login_client.storage.date(0)
        await login_client.storage.save()
        login_client.session = target_session
        log.info("Scanner QR login migrated to Telegram DC %s", dc_id)
        return migrated
    except Exception:
        try:
            await target_session.stop()
        except Exception:
            log.exception("Could not stop failed scanner migration session")
        raise


async def _refresh_scanner_qr(message, login_client, state) -> bool:
    """Request a fresh login token and post its QR code. Returns True if authorized."""
    result = await login_client.invoke(
        raw.functions.auth.ExportLoginToken(
            api_id=config.API_ID, api_hash=config.API_HASH, except_ids=[]
        )
    )
    result = await _complete_scanner_dc_migration(login_client, result)
    if isinstance(result, raw.types.auth.LoginTokenSuccess):
        await _activate_scanner_session(message, login_client)
        return True
    if not isinstance(result, raw.types.auth.LoginToken):
        raise RuntimeError(
            "Unexpected Telegram QR login response: " + type(result).__name__
        )
    state["token"] = result.token
    state["expires"] = int(result.expires)
    link = _scanner_qr_deep_link(result.token)
    buf = _render_scanner_qr_png(link)
    await message.reply_photo(
        buf,
        caption="📷 用登录扫描号的设备扫码：设置 → 设备 → 关联桌面设备。二维码过期会自动刷新。",
    )
    return False


async def _scanner_qr_poll(message, login_client, state):
    try:
        while True:
            await asyncio.sleep(SCANNER_QR_POLL_INTERVAL)
            if not login_client.is_connected:
                return
            result = await login_client.invoke(
                raw.functions.auth.ExportLoginToken(
                    api_id=config.API_ID, api_hash=config.API_HASH, except_ids=[]
                )
            )
            result = await _complete_scanner_dc_migration(login_client, result)
            if isinstance(result, raw.types.auth.LoginTokenSuccess):
                await _activate_scanner_session(message, login_client)
                return
            if not isinstance(result, raw.types.auth.LoginToken):
                raise RuntimeError(
                    "Unexpected Telegram QR login response: " + type(result).__name__
                )
            state["token"] = result.token
            state["expires"] = int(result.expires)
            if state["expires"] - time.time() < SCANNER_QR_REFRESH_MARGIN:
                await _refresh_scanner_qr(message, login_client, state)
    except asyncio.CancelledError:
        raise
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if exc.__class__.__name__ == "SessionPasswordNeeded":
            state["step"] = "password"
            state["expires"] = 0
            try:
                hint = await login_client.get_password_hint()
            except Exception:
                hint = ""
            prompt = (
                "🔐 Telegram 账号已开启两步验证，请发送该账号的云密码以完成登录。\n"
                "密码消息会立即尝试删除，只用于本次验证，不会保存到数据库或日志。\n"
                "发送 /scancancel 可取消。"
            )
            if hint:
                prompt += f"\n密码提示：{hint}"
            await message.reply_text(prompt)
            return
        log.exception("Scanner QR login poll failed")
        try:
            await message.reply_text("❌ 二维码登录中断，请重新发送 /scanlogin。")
        except Exception:
            pass


async def _start_scanner_qr_login(message):
    user = message.from_user
    if scanner is not None and scanner.is_connected:
        await message.reply_text("✅ 扫描账号已经登录。可以直接发送 /scan 开始扫描。")
        return
    old_client = scanner_login_clients.pop(user.id, None)
    old_state = scanner_login_states.pop(user.id, None)
    if old_client:
        try:
            await old_client.disconnect()
        except Exception:
            pass
    if old_state and old_state.get("poll_task"):
        old_state["poll_task"].cancel()

    # Keep the temporary login in memory. Its authorized session is exported
    # to the database after verification, so no local disk persistence is needed.
    login_client = Client(
        "member_cloud_scanner_login",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        in_memory=True,
    )
    try:
        await login_client.connect()
    except Exception:
        try:
            await login_client.disconnect()
        except Exception:
            pass
        raise
    state = {"step": "qr", "token": None, "expires": 0, "poll_task": None}
    scanner_login_clients[user.id] = login_client
    scanner_login_states[user.id] = state

    await message.reply_text(
        "🔐 扫描账号二维码登录\n\n"
        "请用**已经登录扫描号**的 Telegram 设备批准：\n"
        "设置 → 设备 → 关联桌面设备，扫描下方二维码。\n"
        "发送 /scancancel 可取消。"
    )
    if not await _refresh_scanner_qr(message, login_client, state):
        state["poll_task"] = spawn_background(
            "scanner-qr-poll", _scanner_qr_poll(message, login_client, state)
        )


@app.on_message(filters.private & filters.command("scanlogin"))
async def scanner_login_start_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    try:
        await _start_scanner_qr_login(message)
    except Exception:
        log.exception("Could not start scanner QR login")
        await message.reply_text("❌ 无法启动登录流程，请稍后重试。")


@app.on_message(filters.private & filters.command("scanstatus"))
async def scanner_login_status_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scanner is not None and scanner.is_connected:
        await message.reply_text("✅ 扫描账号已经完成登录，当前连接正常。")
        return
    state = scanner_login_states.get(user.id)
    if not state:
        await message.reply_text("当前没有进行中的扫描账号登录。发送 /scanlogin 开始登录。")
        return
    await message.reply_text(
        "🔐 扫描账号登录状态：等待已登录扫描号的设备扫码批准。\n"
        "可发送 /scanresend 刷新二维码，/scancancel 取消。"
    )


@app.on_message(filters.private & filters.command("scanresend"))
async def scanner_login_resend_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    state = scanner_login_states.get(user.id)
    login_client = scanner_login_clients.get(user.id)
    if not state or state.get("step") != "qr" or not login_client:
        await message.reply_text("当前没有进行中的二维码登录。请先发送 /scanlogin。")
        return
    try:
        await _refresh_scanner_qr(message, login_client, state)
    except Exception:
        log.exception("Could not refresh scanner QR code")
        await message.reply_text("❌ 二维码刷新失败，请重新发送 /scanlogin。")


@app.on_message(filters.private & filters.command("uploadcancel"))
async def cancel_upload_category_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    selected = pending_upload_categories.pop(user.id, None)
    upload_failures.pop(user.id, None)
    await message.reply_text(
        f"✅ 已取消目录选择（原目录：{selected}）。后续上传将按文件说明自动分类。"
        if selected else "当前没有选择上传目录。"
    )


@app.on_message(filters.private & filters.command("cancel"))
async def cancel_pending_admin_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        return
    if pending_admin_actions.pop(user.id, None):
        await message.reply_text("已取消添加管理员。")
    if int(user.id) in broadcast_input_mode or int(user.id) in pending_broadcasts:
        broadcast_input_mode.discard(int(user.id))
        pending_broadcasts.pop(int(user.id), None)
        for key, task in list(broadcast_album_tasks.items()):
            if key[0] == int(user.id):
                if task and not task.done():
                    task.cancel()
                broadcast_album_tasks.pop(key, None)
                broadcast_album_buffers.pop(key, None)
        await message.reply_text("已取消广播流程。")
    if int(user.id) in pending_welcome_edit:
        pending_welcome_edit.discard(int(user.id))
        await message.reply_text("已取消编辑欢迎文案。")
    if int(user.id) in pending_announce_edit:
        pending_announce_edit.discard(int(user.id))
        await message.reply_text("已取消编辑公告。")
    if int(user.id) in pending_member_group_url:
        pending_member_group_url.discard(int(user.id))
        await message.reply_text("已取消设置群邀请链接。")


@app.on_message(filters.private & filters.command("scancancel"))
async def scanner_login_cancel_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    login_client = scanner_login_clients.pop(user.id, None)
    state = scanner_login_states.pop(user.id, None)
    if state and state.get("poll_task"):
        state["poll_task"].cancel()
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
    user = message.from_user
    if not user or not await is_admin(user.id):
        return
    if int(user.id) in broadcast_input_mode:
        await capture_broadcast_message(client, message)
        return
    state = scanner_login_states.get(user.id)
    login_client = scanner_login_clients.get(user.id)
    value = (message.text or "").strip()
    if not value:
        return

    if state and state.get("step") == "password" and login_client:
        # Remove the incoming password message before attempting SRP verification.
        try:
            await message.delete()
        except Exception:
            log.warning("Could not delete scanner 2FA password message")
        try:
            await login_client.check_password(value)
            state["step"] = "activating"
            await _activate_scanner_session(message, login_client)
        except Exception as exc:
            if exc.__class__.__name__ in ("PasswordHashInvalid", "PasswordEmpty"):
                await message.reply_text("❌ 云密码不正确，请重新输入；发送 /scancancel 可取消。")
            else:
                log.exception("Scanner 2FA password verification failed")
                await message.reply_text("❌ 两步验证处理失败，请检查日志；可重新输入或发送 /scancancel。")
        return

    if int(user.id) in pending_welcome_edit:
        pending_welcome_edit.discard(int(user.id))
        await db.set_setting(WELCOME_MESSAGE_KEY, value)
        bot_url = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else ""
        preview = render_welcome_message(value, ["新成员"], bot_url)
        await message.reply_text(
            f"✅ 欢迎文案已保存。\n\n预览：\n{preview}",
            reply_markup=back_keyboard(),
        )
        return

    if int(user.id) in pending_announce_edit:
        pending_announce_edit.discard(int(user.id))
        await db.set_setting(ANNOUNCEMENT_KEY, value[:ANNOUNCEMENT_MAX_LEN])
        await message.reply_text(
            "✅ 首页公告已保存。\n\n当前公告：\n" + value[:ANNOUNCEMENT_MAX_LEN],
            reply_markup=back_keyboard(),
        )
        return

    if int(user.id) in pending_member_group_url:
        if value in ("清空", "清除", "无", "删除"):
            pending_member_group_url.discard(int(user.id))
            await db.set_setting(MEMBER_GROUP_URL_KEY, "")
            await message.reply_text(
                "✅ 已清空会员群邀请链接，未入群提示将恢复纯文本。",
                reply_markup=back_keyboard(),
            )
            return
        url = normalize_member_group_url(value)
        if not url:
            # Keep the input state so the admin can simply resend a valid link.
            await message.reply_text(
                "❌ 链接格式不正确。请发送以 http://、https:// 或 t.me/ 开头的链接，"
                "或发送「清空」。",
                reply_markup=back_keyboard(),
            )
            return
        pending_member_group_url.discard(int(user.id))
        await db.set_setting(MEMBER_GROUP_URL_KEY, url)
        await message.reply_text(
            f"✅ 会员群邀请链接已保存：\n{url}\n\n"
            "未入群提示会附带「👥 加入会员群」按钮。",
            reply_markup=back_keyboard(),
        )
        return

    if value == "✅ 结束上传":
        uid = int(user.id)
        if uid in upload_finish_in_progress:
            await message.reply_text("⏳ 正在结束上传，请稍候。", reply_markup=upload_active_keyboard())
            return
        upload_finish_in_progress.add(uid)
        timer = pending_upload_confirmation_tasks.get(uid)
        if timer and not timer.done():
            timer.cancel()
        try:
            status = await message.reply_text(
                f"⏳ 正在结束上传……\n📥 已接收：{upload_received_counts.get(uid, 0)} 个文件\n"
                "正在等待后台保存任务完成，请不要重复点击。",
                reply_markup=upload_active_keyboard(),
            )
            while True:
                tasks = list(pending_upload_tasks.get(uid, set()))
                if not tasks:
                    await asyncio.sleep(0.2)
                    tasks = list(pending_upload_tasks.get(uid, set()))
                    if not tasks:
                        break
                await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            received_count = int(upload_received_counts.get(uid, 0))
            try:
                result = await db.publish_upload_batch(uid)
            except Exception:
                log.exception("Could not publish upload batch for user %s", uid)
                await status.reply_text(
                    "❌ 后台保存已结束，但发布入库失败。本次批次和接收数量已保留，请检查部署日志后重试。",
                    reply_markup=upload_active_keyboard(),
                )
                return
            if result.get("category") and int(result.get("published", 0)) > 0:
                await notify_folder_subscribers(client, result["category"], "new_resources")
            failure_suffix = render_failures(upload_failures.get(uid))
            upload_received_counts.pop(uid, None)
            upload_progress_state.pop(uid, None)
            pending_upload_categories.pop(uid, None)
            category_catalog_cache["expires_at"] = 0
            upload_status_messages.pop(uid, None)
            upload_confirmation_messages.pop(uid, None)
            upload_latest_messages.pop(uid, None)
            upload_failures.pop(uid, None)
            if result.get("category") is None:
                completion_text = "目前没有待发布的上传批次。"
            else:
                published = int(result.get("published", 0))
                completion_text = (
                    f"✅ 本次上传完成\n📥 收到文件：{received_count} 个\n"
                    f"📦 成功入库：{published} 个文件"
                    + (f"\n⚠️ 未成功入库：{max(0, received_count - published)} 个，请检查部署日志。" if received_count > published else "")
                )
            completion_text += failure_suffix
            await status.reply_text(completion_text, reply_markup=admin_home_keyboard())
        finally:
            upload_finish_in_progress.discard(uid)
        return
    if value == "📁 创建文件夹":
        pending_category_actions[user.id] = {"action": "create"}
        await message.reply_text(
            "📁 请发送新文件夹名称。创建后会自动选中该目录。\n"
            "如不想创建，点击下方「❌ 取消建立」。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ 取消建立", callback_data="mc:categorycancel")]
            ]),
        )
        return
    if value == "📋 文件夹列表":
        categories = await db.list_categories()
        rows = []
        for index, row in enumerate(categories[:20]):
            rows.append([InlineKeyboardButton(
                f"📁 {row['category'][:32]}（{int(row['file_count'] or 0)}）",
                callback_data=f"mc:directory:{index}",
            )])
        rows.append([InlineKeyboardButton("📂 查看全部目录 / 翻页", callback_data="mc:directories")])
        if not categories:
            rows.insert(0, [InlineKeyboardButton("➕ 新建目录", callback_data="mc:dirnew")])
        await message.reply_text(
            f"📋 管理员目录列表（共 {len(categories)} 个）\n\n"
            "点击目录可查看详情、修改名称或上传文件；目录超过 20 个时，点击下方按钮翻页。",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return
    if value == "📤 上传文件":
        categories = await db.list_categories()
        rows = []
        for index, row in enumerate(categories[:20]):
            rows.append([InlineKeyboardButton(
                f"📁 {row['category'][:32]}（{int(row['file_count'] or 0)}）",
                callback_data=f"mc:dirupload:{index}",
            )])
        rows.append([InlineKeyboardButton("📂 查看全部目录 / 翻页", callback_data="mc:directories")])
        if not categories:
            rows.insert(0, [InlineKeyboardButton("➕ 新建目录", callback_data="mc:dirnew")])
        await message.reply_text(
            f"📤 选择上传目录（共 {len(categories)} 个）\n\n"
            "选择后直接发送文件。发送暂停后，机器人会自动显示数量，并提供「结束上传」或「继续上传」按钮。",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return
    if value == "🛠 管理设置":
        await message.reply_text("🛠 完整管理设置：", reply_markup=admin_keyboard())
        return

    pending_admin_action = pending_admin_actions.get(user.id)
    if pending_admin_action:
        if pending_admin_action.get("action") == "add":
            try:
                target_id = int(value)
                if target_id <= 0:
                    raise ValueError()
            except ValueError:
                await message.reply_text("❌ 用户 ID 必须是正整数，请重新发送，或发送 /cancel 取消。")
                return
            superadmin_id = await db.get_admin_id()
            if target_id in (int(config.ADMIN_ID), int(superadmin_id or 0)):
                await message.reply_text("⚠️ 这个账号已经是超级管理员，无需重复添加。")
                pending_admin_actions.pop(user.id, None)
                return
            added = await db.add_admin(target_id, user.id)
            pending_admin_actions.pop(user.id, None)
            await message.reply_text(
                f"✅ 已添加管理员：{target_id}" if added
                else f"ℹ️ 用户 {target_id} 已经是管理员。"
            )
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
            new_category = value[:120].strip()
            if not new_category:
                await message.reply_text("❌ 文件夹名称不能为空，请重新发送名称。")
                return
            active_batch = await db.get_upload_batch(user.id)
            created_new = await db.save_category(new_category)
            category_catalog_cache["expires_at"] = 0
            pending_category_actions.pop(user.id, None)
            if created_new:
                await notify_folder_subscribers(client, new_category, "new_folder")
            if active_batch:
                await message.reply_text(
                    f"✅ 已创建目录：{new_category}\n"
                    "空目录会立即显示在会员文件夹列表中。\n\n"
                    f"⚠️ 你当前还有未结束的上传批次：{active_batch['category']}。"
                    "请先在上传确认卡片点击「结束上传」发布本批文件，再选择新目录。",
                    reply_markup=upload_active_keyboard(),
                )
            else:
                pending_upload_categories[user.id] = new_category
                await db.start_upload_batch(user.id, new_category)
                upload_received_counts[int(user.id)] = 0
                await message.reply_text(
                    f"✅ 已创建目录：{new_category}\n"
                    "📤 已自动选中这个目录，现在可以直接发送图片、视频或文件。\n\n"
                    "支持连续上传单文件和成组相册。发送暂停后，"
                    "机器人会在文件下方显示已接收数量，并提供内联「结束上传」按钮；"
                    "确认数量无误后再点击发布。",
                    reply_markup=upload_active_keyboard(),
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

    # QR login waits for scan approval in a background poll; text input is not consumed.
    return


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


@app.on_message(filters.private & filters.command("audit"))
async def delivery_audit_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    try:
        totals, top = await db.get_delivery_audit_summary(hours=24, limit=10)
        total = int(totals.get("total") or 0)
        success = int(totals.get("success") or 0)
        failed = int(totals.get("failed") or 0)
        pending = int(totals.get("pending") or 0)
        lines = [
            "📊 资源发放审计（最近24小时）",
            f"发放记录：{total} 条",
            f"成功：{success} 条｜失败：{failed} 条｜未完成：{pending} 条",
            "",
            "🔎 高频资源（按发放次数）",
        ]
        if not top:
            lines.append("暂无审计记录。新版本部署后开始累计。")
        else:
            for index, row in enumerate(top, 1):
                name = str(row.get("file_name") or row.get("file_key") or "未知资源")[:55]
                category = str(row.get("category") or "未分类")[:35]
                lines.append(
                    f"{index}. {name}\n"
                    f"目录：{category}｜发送：{int(row.get('sends') or 0)} 次"
                    f"｜用户：{int(row.get('users') or 0)}"
                    f"｜失败：{int(row.get('failures') or 0)}"
                )
        lines.extend([
            "",
            "说明：审计用于关联资源、目录、用户和发送时间；Telegram 不会通过机器人接口公开官方封禁的具体原因或文件内部标记。",
        ])
        await message.reply_text("\n".join(lines))
    except Exception:
        log.exception("Could not generate delivery audit report")
        await message.reply_text("⚠️ 审计报告暂时无法读取，请检查数据库初始化和部署日志。")




def watermark_detail_text(code, rows):
    """Shared rendering for the /wm command and the admin button detail page."""
    if not rows:
        return (
            f"没有找到编号 {code} 的发放记录。\n\n"
            "请确认编号抄写正确（六位字母或数字，不含 O/0/I/1）。"
        )
    status_names = {"success": "成功", "failed": "失败", "attempted": "未完成"}
    lines = [f"🆔 领取编号：{code}", "━━━━━━━━━━━━━━"]
    for row in rows:
        stamp = format_cn_datetime(row["created_at"])
        status = status_names.get(str(row["status"]), str(row["status"] or "未知"))
        lines.extend([
            f"👤 用户：{int(row['user_id'])}",
            f"📄 资源：{str(row['file_key'])[:60]}",
            f"📁 目录：{str(row['category'] or '未分类')}",
            f"🎬 类型：{str(row['media_type'] or '未知')}",
            f"📌 结果：{status}"
            + (f"（{str(row['error_text'])[:80]}）" if row["error_text"] else ""),
            f"🕒 时间：{stamp}",
            "─────────────",
        ])
    lines.append("用途：会员外传或转卖文件时，凭此编号定位到领取人。")
    return "\n".join(lines)[:3800]


@app.on_message(filters.private & filters.command("wm"))
async def watermark_lookup_handler(client, message):
    """Trace a delivery code printed in a member's caption back to its recipient."""
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    parts = (message.text or "").split(maxsplit=1)
    code = parts[1].strip().upper() if len(parts) > 1 else ""
    if not code:
        await message.reply_text(
            "用法：/wm 领取编号\n\n"
            "领取编号是会员收到文件时说明里那行「🆔 领取编号」的六位字符。\n"
            "示例：/wm AB23CD"
        )
        return
    try:
        rows = await db.find_delivery_by_watermark(code, limit=10)
    except Exception:
        log.exception("Could not look up delivery watermark: %s", code)
        await message.reply_text("⚠️ 查询失败，请检查数据库状态。")
        return
    await message.reply_text(watermark_detail_text(code, rows))


@app.on_message(filters.private & filters.command("auditdetail"))
async def delivery_audit_detail_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    try:
        parts = (message.text or "").split()
        limit = int(parts[1]) if len(parts) > 1 else 20
        limit = max(1, min(50, limit))
        rows = await db.list_delivery_audit_details(limit=limit)
        if not rows:
            await message.reply_text("暂无逐条发送记录。部署审计版本后产生的发送才会记录。")
            return
        lines = [f"🧾 最近 {len(rows)} 条资源发送明细（最多50条）"]
        for row in rows:
            stamp = format_cn_datetime(row.get("created_at"))
            name = str(row.get("file_name") or row.get("file_key") or "未知资源")[:65]
            category = str(row.get("category") or "未分类")[:30]
            status = {"success": "成功", "failed": "失败", "attempted": "未完成"}.get(
                str(row.get("status")), str(row.get("status") or "未知")
            )
            source = ""
            if row.get("source_chat_id") is not None or row.get("source_message_id") is not None:
                source = f"\n来源：{row.get('source_chat_id') or '-'} / 消息 {row.get('source_message_id') or '-'}"
            lines.append(
                f"\n⏱ {stamp}\n📄 {name}\n📁 {category}"
                f"\n接收用户：{row.get('user_id')}｜结果：{status}{source}"
            )
        # Keep Telegram messages below its practical text-size limit.
        chunks, current = [], ""
        for line in lines:
            if len(current) + len(line) > 3500:
                chunks.append(current)
                current = ""
            current += line + "\n"
        if current:
            chunks.append(current)
        for chunk in chunks:
            await message.reply_text(chunk)
    except (ValueError, IndexError):
        await message.reply_text("格式：/auditdetail 或 /auditdetail 50（最多50条）")
    except Exception:
        log.exception("Could not generate delivery audit detail report")
        await message.reply_text("⚠️ 发送明细暂时无法读取，请检查数据库和部署日志。")


@app.on_message(filters.private & filters.command("auditban"))
async def delivery_audit_ban_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply_text(
            "请填写 Telegram 封禁发生时间（必须带时区），例如：\n"
            "/auditban 2026-11-13T02:00:00+09:00\n"
            "默认分析封禁前24小时；也可指定小时数：\n"
            "/auditban 2026-11-13T02:00:00+09:00 6"
        )
        return
    try:
        ban_time = datetime.fromisoformat(parts[1])
        if ban_time.tzinfo is None or ban_time.utcoffset() is None:
            raise ValueError("timezone required")
        hours = int(parts[2]) if len(parts) > 2 else 24
        if hours < 1 or hours > 168:
            raise ValueError("hours out of range")
        totals, resources, recipients, latest = await db.get_delivery_audit_before(
            ban_time.astimezone(timezone.utc), hours=hours, limit=10
        )
        start_time = (ban_time - timedelta(hours=hours)).isoformat()
        total = int(totals.get("total") or 0)
        lines = [
            f"🛡 封禁时间关联分析（前 {hours} 小时）",
            f"封禁时间：{ban_time.isoformat()}",
            f"分析区间：{start_time} 至 {ban_time.isoformat()}",
            f"发送记录：{total}｜成功：{int(totals.get('success') or 0)}"
            f"｜失败：{int(totals.get('failed') or 0)}｜未完成：{int(totals.get('pending') or 0)}",
            f"涉及接收用户：{int(totals.get('users') or 0)} 人",
            "",
            "🔎 高频资源（最多10项）",
        ]
        if resources:
            for index, row in enumerate(resources, 1):
                lines.append(
                    f"{index}. {str(row.get('file_name') or row.get('file_key') or '未知资源')[:55]}"
                    f"\n目录：{str(row.get('category') or '未分类')[:30]}"
                    f"｜发送 {int(row.get('sends') or 0)} 次"
                    f"｜用户 {int(row.get('users') or 0)}"
                    f"\n最后发送：{format_cn_datetime(row.get('last_sent'))}"
                )
        else:
            lines.append("该时间段没有审计记录。")
        lines.extend(["", "👥 接收用户（最多20人）"])
        if recipients:
            for row in recipients:
                lines.append(
                    f"用户 {row.get('user_id')}：{int(row.get('sends') or 0)} 次"
                    f"｜{int(row.get('resources') or 0)} 项资源"
                    f"｜最后发送 {format_cn_datetime(row.get('last_sent'))}"
                )
        else:
            lines.append("无记录。")
        if latest:
            lines.extend([
                "",
                "🕒 封禁前最后一笔发送",
                f"时间：{format_cn_datetime(latest.get('created_at'))}",
                f"资源：{latest.get('file_name')}",
                f"用户：{latest.get('user_id')}｜结果：{latest.get('status')}",
                f"来源消息：{latest.get('source_chat_id')} / {latest.get('source_message_id')}",
            ])
        lines.extend([
            "",
            "注意：这是发送时间与封禁时间的统计关联，不代表这些资源或用户导致封禁。Telegram 不向机器人开放官方封禁原因。"
        ])
        report = "\n".join(lines)
        if len(report) <= 3900:
            await message.reply_text(report)
        else:
            # Split long reports without losing any rows.
            chunk = ""
            for line in lines:
                if len(chunk) + len(line) > 3500:
                    await message.reply_text(chunk)
                    chunk = ""
                chunk += line + "\n"
            if chunk:
                await message.reply_text(chunk)
    except (ValueError, TypeError):
        await message.reply_text(
            "时间格式不正确。请使用带时区的 ISO 时间，例如：\n"
            "/auditban 2026-11-13T02:00:00+09:00 24"
        )
    except Exception:
        log.exception("Could not generate ban-time delivery audit report")
        await message.reply_text("⚠️ 封禁关联报告暂时无法读取，请检查数据库和部署日志。")


@app.on_message(filters.private & filters.command("catalogreset"))
async def catalog_reset_prompt_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scan_task and not scan_task.done():
        await message.reply_text("⚠️ 请先发送 /scanstop 暂停扫描，再执行目录重建。")
        return
    await message.reply_text(
        "⚠️ 即将清空旧资源索引并重新扫描主频道。\n\n"
        "会清理：文件索引、目录、扫描记录、文件收藏及领取进度。\n"
        "会保留：用户、会员、管理员、封禁名单、机器人设置及扫描账号登录会话。\n"
        "不会删除 Telegram 主频道或备份频道里的任何原始消息。\n\n"
        "确认执行请发送：/catalogresetconfirm\n"
        "不执行则忽略本消息。"
    )


@app.on_message(filters.private & filters.command("catalogresetconfirm"))
async def catalog_reset_confirm_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scan_task and not scan_task.done():
        await message.reply_text("⚠️ 扫描正在运行，请先发送 /scanstop。")
        return
    await db.reset_catalog_for_rescan()
    category_catalog_cache["expires_at"] = 0
    category_catalog_cache["rows"] = []
    await message.reply_text(
        "✅ 旧资源索引和目录已清理。用户、会员、管理员及机器人设置均已保留。\n\n"
        "现在发送 /scan，即可只从主存储频道重新建立资源目录。"
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
    primary, _ = await get_storage_channels()
    if not primary:
        await message.reply_text("❌ 尚未绑定主存储频道，请先绑定主频道。")
        return
    await message.reply_text("🔎 正在检查扫描账号读取主存储频道历史的权限……")
    result = await inspect_scan_channel(primary)
    body = "🔎 主频道扫描检查结果\n\n" + format_scan_check(result, "主存储频道")
    body += "\n\n" + ("✅ 可以发送 /scan 开始扫描。" if result["ok"] else
                         "⚠️ 请先修复扫描账号的主频道访问权限。")
    await message.reply_text(body)


@app.on_message(filters.private & filters.command("scan"))
async def scan_history_handler(client, message):
    global scan_task
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    if scanner is None or not scanner.is_connected:
        await message.reply_text(
            "⚠️ 扫描账号尚未登录或连接已断开。请先发送 /scanlogin 登录。"
        )
        return
    if scan_task and not scan_task.done():
        await message.reply_text("⏳ 主频道历史扫描正在运行。发送 /scanstatus 查看进度。")
        return
    primary, _ = await get_storage_channels()
    if not primary:
        await message.reply_text("❌ 请先绑定主存储频道。")
        return
    check = await inspect_scan_channel(primary)
    if not check["ok"]:
        await message.reply_text(
            "❌ 主频道检查未通过：\n\n" + format_scan_check(check, "主存储频道")
            + "\n\n修复权限后先运行 /scancheck，再发送 /scan。"
        )
        return
    progress = await db.get_scan_progress(primary)
    if progress["completed"]:
        categories = await db.list_categories()
        _, total_files, _, _ = await db.stats()
        await message.reply_text(
            "✅ 主频道历史扫描已经完成。\n"
            f"📁 当前目录：{len(categories)} 个\n"
            f"📦 当前资源：{total_files} 个\n\n"
            "如需重新建立索引，请先发送 /catalogreset，再发送 /scan。"
        )
        return
    scan_task = spawn_background("history-scan", run_history_scan(primary))
    await message.reply_text(
        "🚀 已启动主存储频道历史扫描。\n"
        "只读取主频道并建立数据库索引，不扫描备份频道、不下载或删除原始文件。\n\n"
        "发送 /scanstatus 查看进度；发送 /scanstop 可暂停，之后可继续。"
    )


@app.on_message(filters.private & filters.command("scanstatus"))
async def scan_status_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    primary, _ = await get_storage_channels()
    if not primary:
        await message.reply_text("尚未绑定主存储频道。")
        return
    progress = await db.get_scan_progress(primary)
    running = bool(scan_task and not scan_task.done())
    if progress["completed"]:
        state = "已完成"
    elif not progress["last_message_id"] and not progress["scanned_count"]:
        state = "未开始"
    else:
        state = f"待继续（最近消息 ID：{progress['last_message_id']}）"
    await message.reply_text(
        f"📊 主频道扫描状态：{'运行中' if running else '未运行'}\n"
        f"进度：{state}\n已检查媒体：{progress['scanned_count']}\n\n"
        "备份频道：不参与本次扫描。"
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
    pending_state = scanner_login_states.pop(user.id, None)
    if pending_state and pending_state.get("poll_task"):
        pending_state["poll_task"].cancel()
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
    page_size = 10
    rows = [[InlineKeyboardButton("➕ 新建目录", callback_data="mc:dirnew")]]
    for index, row in enumerate(categories[:page_size]):
        rows.append([InlineKeyboardButton(
            f"{index + 1}. 📁 {row['category'][:36]}（{int(row['file_count'] or 0)}）",
            callback_data=f"mc:directory:{index}",
        )])
    for chunk_start in range(0, min(len(categories), page_size), 5):
        rows.append([
            InlineKeyboardButton(str(index + 1), callback_data=f"mc:directory:{index}")
            for index in range(chunk_start, min(chunk_start + 5, len(categories), page_size))
        ])
    if len(categories) > page_size:
        rows.append([InlineKeyboardButton("下一页 ➡️", callback_data="mc:directories:1")])
    rows.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
    list_text = (
        f"📁 文件夹列表（共 {len(categories)} 个，第 1 页）\n\n"
        "点击文件夹名称可复制名称；点击下方数字查看统计和管理。"
    )
    copy_rows = [[{"text": "➕ 新建目录", "callback_data": "mc:dirnew"}]]
    for index, row in enumerate(categories[:page_size]):
        copy_rows.append([{
            "text": f"{index + 1}. 📁 {row['category'][:36]}（{int(row['file_count'] or 0)}）",
            "copy_text": {"text": str(row["category"])},
        }])
    for chunk_start in range(0, min(len(categories), page_size), 5):
        copy_rows.append([{
            "text": str(index + 1),
            "callback_data": f"mc:directory:{index}",
        } for index in range(chunk_start, min(chunk_start + 5, len(categories), page_size))])
    if len(categories) > page_size:
        copy_rows.append([{"text": "下一页 ➡️", "callback_data": "mc:directories:1"}])
    copy_rows.append([{"text": "⬅️ 返回管理面板", "callback_data": "mc:home"}])
    try:
        await telegram_bot_api("sendMessage", {
            "chat_id": message.chat.id,
            "text": list_text,
            "reply_markup": {"inline_keyboard": copy_rows},
        })
    except Exception:
        log.exception("Could not render initial admin folder copy buttons")
        await message.reply_text(list_text, reply_markup=InlineKeyboardMarkup(rows))


@app.on_message(filters.private & filters.command("admin"))
async def admin_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return
    await message.reply_text(
        await admin_home_text(),
        reply_markup=admin_home_keyboard(),
    )


async def build_favorites_view(user_id):
    files = await db.list_favorites(user_id, 12)
    folders = await db.list_favorite_categories(user_id, 20)
    rows = []
    sections = ["⭐ 我的收藏"]
    if files:
        sections.extend(["", "📄 收藏的文件"])
        for item in files:
            rows.append([
                InlineKeyboardButton(
                    f"📄 {item['file_name'][:30]}",
                    callback_data=f"uc:open:{item['file_key']}",
                ),
                InlineKeyboardButton(
                    "取消⭐",
                    callback_data=f"uc:star:{item['file_key']}:favorites",
                ),
            ])
    if folders:
        sections.extend(["", "📁 收藏的文件夹"])
        for item in folders:
            rows.append([InlineKeyboardButton(
                f"📁 {item['name'][:32]}",
                callback_data=f"uc:folderlink:{item['share_key']}",
            )])
    if not files and not folders:
        sections.extend([
            "",
            "你还没有收藏文件或文件夹。",
            "去「资源目录」打开目录并点「⭐ 收藏目录」，或在资源消息里点「⭐ 收藏」。",
        ])
    rows.append([InlineKeyboardButton("⬅️ 返回首页", callback_data="uc:home")])
    return "\n".join(sections), InlineKeyboardMarkup(rows)


async def build_catalog_search_page(user_id, query_text, offset=0, token=None):
    query_text = str(query_text or "").strip()[:80]
    offset = max(0, int(offset or 0))
    page_size = 8
    if not token:
        token = secrets.token_urlsafe(6).replace("-", "").replace("_", "")
        # Keep the in-memory navigation cache bounded.
        while len(search_sessions) >= 300:
            search_sessions.pop(next(iter(search_sessions)), None)
        search_sessions[token] = {
            "user_id": int(user_id),
            "query": query_text,
        }

    # Search saved folders separately so a folder can be opened directly.
    all_categories = await db.list_categories()
    tokens = db.split_search_query(query_text)

    def folder_matches(row, mode="all"):
        name = str(row["category"] or "").casefold()
        if not tokens:
            return True
        if mode == "all":
            return all(token.casefold() in name for token in tokens)
        return any(token.casefold() in name for token in tokens)

    matched_categories = [
        row for row in all_categories if folder_matches(row, "all")
    ][:8]

    records, _, total = await db.public_catalog(
        query=query_text, limit=page_size, offset=offset
    )

    # No exact match: suggest folders that share at least one keyword, else the
    # first few folders as a fallback so the search page never feels dead.
    recommended_categories = []
    if not records and not matched_categories:
        recommended_categories = [
            row for row in all_categories if folder_matches(row, "any")
        ][:5]
        if not recommended_categories:
            recommended_categories = list(all_categories)[:5]
    buttons = []
    if matched_categories:
        for category_row in matched_categories:
            folder_name = str(category_row["category"] or "未命名文件夹")
            share_key = str(category_row["share_key"] or "")
            if not share_key:
                continue
            buttons.append([InlineKeyboardButton(
                f"📁 {folder_name[:32]}（{int(category_row['file_count'] or 0)}）",
                callback_data=f"uc:folderlink:{share_key}",
            )])

    share_key_by_category = {
        str(row["category"] or ""): str(row["share_key"] or "")
        for row in all_categories
    }
    matched_names = {str(row["category"] or "") for row in matched_categories}
    # Group one page of search hits by folder so the text and the keyboard read
    # in the same order: folder heading first, then its files.
    record_groups = {}
    number = offset
    for row in records:
        number += 1
        group_name = str(row["category"] or "未分类")
        record_groups.setdefault(group_name, []).append((number, row))

    for group_name, group_records in record_groups.items():
        share_key = share_key_by_category.get(group_name, "")
        if share_key and group_name not in matched_names:
            buttons.append([InlineKeyboardButton(
                f"📁 {group_name[:28]}（{len(group_records)}）",
                callback_data=f"uc:folderlink:{share_key}",
            )])
        for seq, row in group_records:
            file_key = str(row["file_key"] or "")
            if not file_key:
                continue
            name = str(row["file_name"] or "未命名资源")
            buttons.append([InlineKeyboardButton(
                f"{seq}. {media_type_icon(row['media_type'])} {name[:32]}",
                callback_data=f"uc:open:{file_key}",
            )])

    for category_row in recommended_categories:
        folder_name = str(category_row["category"] or "未命名文件夹")
        share_key = str(category_row["share_key"] or "")
        if not share_key:
            continue
        buttons.append([InlineKeyboardButton(
            f"💡 {folder_name[:32]}（{int(category_row['file_count'] or 0)}）",
            callback_data=f"uc:folderlink:{share_key}",
        )])

    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton(
            "⬅️ 上一页",
            callback_data=f"uc:searchpage:{token}:{max(0, offset - page_size)}",
        ))
    if offset + page_size < total:
        nav.append(InlineKeyboardButton(
            "下一页 ➡️",
            callback_data=f"uc:searchpage:{token}:{offset + page_size}",
        ))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("🏠 返回首页", callback_data="uc:home")])

    sections = [f"🔎 搜索：{query_text}"]
    if matched_categories:
        sections.extend([
            "",
            f"📁 找到 {len(matched_categories)} 个匹配文件夹（最多显示 8 个）",
            "点击文件夹名称即可进入目录，再选择获取资源。",
        ])
    if records:
        sections.extend([
            "",
            f"📄 找到 {total} 个匹配资源（第 {offset // page_size + 1} 页）",
        ])
        for group_name, group_records in record_groups.items():
            sections.extend(["", f"📁 {group_name[:40]}"])
            for seq, row in group_records:
                sections.append(
                    f"{seq}. {media_type_icon(row['media_type'])} "
                    f"{str(row['file_name'] or '未命名资源')[:48]}"
                )
    elif not matched_categories:
        if recommended_categories:
            sections.extend([
                "",
                "没有同时包含全部关键词的资源或文件夹。",
                "可以先看看这些相近目录：",
            ])
        else:
            sections.extend(["", "暂时没有找到匹配的文件夹或资源。"])

    body = "\n".join(sections)
    if records or matched_categories or recommended_categories:
        body += "\n\n点击上方文件夹进入目录，或点击资源按钮直接获取文件。"
    return body, InlineKeyboardMarkup(buttons), token

async def send_catalog_search_results(message, query_text):
    query_text = str(query_text or "").strip()[:80]
    if not query_text:
        await message.reply_text("请输入要搜索的关键词。")
        return
    user = getattr(message, "from_user", None)
    if not user:
        await message.reply_text("无法识别用户，请重新打开机器人搜索。")
        return
    body, markup, _ = await build_catalog_search_page(user.id, query_text)
    await message.reply_text(body, reply_markup=markup, disable_web_page_preview=True)


@app.on_message(filters.private & filters.command("search"))
async def catalog_search_handler(client, message):
    user = message.from_user
    if not user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        pending_search_users.add(int(user.id))
        await message.reply_text(
            "🔎 资源搜索\n\n"
            "请直接发送你要查找的资源名称、关键词或编号。\n"
            "例如：电影、写真、视频编号\n\n"
            "无需输入 /search，直接发送文字即可。"
        )
        return
    await send_catalog_search_results(message, parts[1].strip())


@app.on_message(filters.private & filters.text & ~filters.regex(r"^/"), group=-2)
async def catalog_search_text_handler(client, message):
    user = message.from_user
    if not user:
        return
    user_id = int(user.id)
    if user_id not in pending_search_users:
        return

    query_text = str(message.text or "").strip()
    log.info(
        "Catalog search input received: user=%s chars=%s",
        user_id, len(query_text),
    )
    if not query_text:
        await message.reply_text("请输入要搜索的关键词。")
        return

    try:
        await send_catalog_search_results(message, query_text)
        pending_search_users.discard(user_id)
    except Exception as exc:
        # Keep the pending state so the user can retry without reopening search.
        log.exception("Catalog search failed for user %s", user_id)
        try:
            await message.reply_text(
                "⚠️ 搜索暂时失败，资源数据没有被修改。请稍后重新发送关键词；"
                "如果连续失败，请联系管理员检查运行日志。"
            )
        except Exception:
            log.exception("Could not send catalog search error notice to user %s", user_id)


@app.on_callback_query(filters.regex(r"^uc:"))
async def user_catalog_callback_handler(client, query):
    user = query.from_user
    action = query.data.split(":")
    message = query.message
    if not user or not message:
        await query.answer("无法识别用户或消息，请重新打开机器人。", show_alert=True)
        return

    if action[1] == "getfolder":
        # Apply a per-user cooldown across all folders, not just one folder.
        # This prevents rapid repeated taps from starting consecutive batches.
        now = asyncio.get_running_loop().time()
        last_click = float(last_getfolder_click_at.get(int(user.id), 0.0))
        remaining_cooldown = 5.0 - (now - last_click)
        if remaining_cooldown > 0:
            await query.answer(
                f"⏳ 操作太快，请 {remaining_cooldown:.1f} 秒后再获取资源。",
                show_alert=True,
            )
            return
        last_getfolder_click_at[int(user.id)] = now
        await query.answer("📥 正在检查会员群资格并准备资源…")
        if not await has_access(user.id, force_refresh=True):
            await reply_membership_gate(message)
            return

    if action[1] == "home":
        await query.answer()
        await message.edit_text(
            await user_home_text(user.id),
            reply_markup=await user_home_keyboard(),
        )
        return

    if action[1] == "member":
        await query.answer()
        tier = await get_membership_tier(user.id)
        stats = await db.catalog_stats()
        await message.edit_text(
            "👤 会员中心\n\n"
            f"🏅 会员级别：{tier or '未开通'}\n"
            f"🆔 用户 ID：{user.id}\n\n"
            "📊 资源库概况\n"
            f"📁 文件夹：{stats['folders']} 个\n"
            f"📦 资源消息：{stats['messages']} 条\n"
            f"🖼 图片：{stats['images']} 张\n"
            f"🎬 视频：{stats['videos']} 个\n\n"
            "感谢使用会员资源中心。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🕘 我的领取", callback_data="uc:history"),
                 InlineKeyboardButton("❔ 使用帮助", callback_data="uc:help")],
                [InlineKeyboardButton("🏠 返回主页", callback_data="uc:home")]
            ]),
        )
        return

    if action[1] == "notifications":
        enabled = await db.toggle_notification_subscription(user.id)
        await query.answer("已开启新资源提醒" if enabled else "已关闭新资源提醒")
        await message.edit_text(
            "🔔 新资源通知设置\n\n" + (
                "✅ 已开启。管理员发布新资源后，你会收到提醒。"
                if enabled else "🔕 当前已关闭。开启后可及时收到新资源提醒。"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔕 关闭通知" if enabled else "🔔 开启通知", callback_data="uc:notifications")],
                [InlineKeyboardButton("🏠 返回主页", callback_data="uc:home")],
            ]),
        )
        return

    if action[1] == "random_again":
        action[1] = "random"

    if action[1] == "help":
        await query.answer()
        await message.edit_text(
            "❔ 使用帮助\n\n"
            "📁 资源目录：打开文件夹列表，选择文件夹查看内容。\n"
            "🔎 搜索资源：输入文件名、文件夹名称或关键词；搜索结果可直接进入文件夹或领取单个资源。\n"
            "🆕 最新入库：查看最近入库的资源。\n"
            "🎲 随机探索：每次随机发送一项；如果该资源属于相册，会整组发送。\n"
            "🕘 我的领取：查看最近领取记录并可再次打开资源。\n\n"
            "🔒 领取资格：需要加入管理员绑定的会员群。离开会员群后，系统会按群成员状态限制领取。\n"
            "📩 如遇到资源无法发送，请稍后重试或联系管理员。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📁 打开资源目录", callback_data="uc:folders")],
                [InlineKeyboardButton("🏠 返回首页", callback_data="uc:home")],
            ]),
        )
        return

    if action[1] == "latest":
        await query.answer("正在整理最新入库…")
        records, _, total = await db.public_catalog(limit=12, offset=0)
        rows = [[InlineKeyboardButton(
            f"📄 {str(item['file_name'])[:38]}",
            callback_data=f"uc:open:{item['file_key']}",
        )] for item in records]
        rows.append([InlineKeyboardButton("🎲 随机探索", callback_data="uc:random")])
        rows.append([InlineKeyboardButton("🏠 返回主页", callback_data="uc:home")])
        body = "🆕 最新入库（按入库时间排序）\n"
        body += f"共 {total} 个资源，以下显示最新 12 个。\n\n"
        body += ("\n".join(
            f"• {item['file_name'][:48]}（{item['category']}）" for item in records
        ) if records else "暂时没有资源。")
        await message.edit_text(body, reply_markup=InlineKeyboardMarkup(rows))
        return

    if action[1] == "latest_new":
        await query.answer("正在整理最近上新…")
        records = await db.list_categories_latest(limit=12)
        rows = [[InlineKeyboardButton(
            f"📁 {str(item['category'])[:36]}（{int(item['file_count'] or 0)}）",
            callback_data=f"uc:folderstats:{item['share_key']}:0",
        )] for item in records]
        rows.append([InlineKeyboardButton("🏠 返回主页", callback_data="uc:home")])
        body = "🆕 最近上新（按目录内最新资源排序）\n\n"
        body += ("\n".join(
            f"{i}. {item['category']}（{int(item['file_count'] or 0)} 个资源）"
            for i, item in enumerate(records, start=1)
        ) if records else "暂时没有目录。")
        await message.edit_text(body, reply_markup=InlineKeyboardMarkup(rows))
        return

    if action[1] == "top":
        await query.answer("正在整理热门目录…")
        records = await db.get_top_categories(limit=12)
        rows = [[InlineKeyboardButton(
            f"📁 {str(item['category'])[:34]}（领 {int(item['sends'] or 0)} 次）",
            callback_data=f"uc:folderstats:{item['share_key']}:0",
        )] for item in records]
        rows.append([InlineKeyboardButton("🏠 返回主页", callback_data="uc:home")])
        body = "🔥 领取最多（按成功领取次数排序）\n\n"
        body += ("\n".join(
            f"{i}. {item['category']} — {int(item['sends'] or 0)} 次领取，{int(item['users'] or 0)} 位会员"
            for i, item in enumerate(records, start=1)
        ) if records else "暂时还没有领取记录。")
        await message.edit_text(body, reply_markup=InlineKeyboardMarkup(rows))
        return

    if action[1] == "random":
        # Acknowledge the button immediately; database and Telegram delivery
        # must not leave the user's callback spinner hanging.
        await query.answer("🎲 正在随机选择资源（相册整组发送）")
        try:
            if not await has_access(user.id, force_refresh=True):
                await reply_membership_gate(message)
                return
            total = await db.catalog_total()
            if not total:
                await message.reply_text("📭 资源库暂时没有可随机发送的文件。")
                return

            # Pick exactly one catalog item. If it belongs to a Telegram album,
            # complete_album_groups expands it to the entire original album.
            # Thus one tap sends one single file OR one complete media group.
            start = secrets.randbelow(int(total))
            rows, _, _ = await db.public_catalog(limit=1, offset=start)
            rows = await db.complete_album_groups(rows)
            if not rows:
                await message.reply_text("📭 暂时没有找到可发送的资源，请稍后重试。")
                return

            pace = None if await is_admin(user.id) else delivery_pace_hint(user.id)
            if pace:
                await message.reply_text(pace)
                return
            sent, failed, skipped, _already, _, _last_sent_message_id = await send_catalog_batch(
                client, user.id, "random", 0, rows, track_progress=False
            )
            summary = f"🎲 随机发送完成：成功 {sent}/{len(rows)} 个文件。"
            if failed:
                summary += f"\n\n⚠️ 本次有 {failed} 个文件发送失败，失败资源没有记为已领取；请检查存储来源后再次尝试。"
            # Send the completion summary through the same direct Bot API
            # transport used by the reliable file-copy path. The raw JSON
            # reply_markup makes it explicit that Telegram receives buttons.
            result_message = await telegram_bot_api("sendMessage", {
                "chat_id": int(user.id),
                "text": summary,
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "🔄 再来一批", "callback_data": "uc:random_again"}],
                        [{"text": "🏠 返回主页", "callback_data": "uc:home"}],
                    ]
                },
            })
            log.info(
                "Random completion sent via Bot API with inline keyboard: user=%s message_id=%s sent=%s total=%s failed=%s",
                user.id, result_message.get("message_id") if isinstance(result_message, dict) else None,
                sent, len(rows), failed,
            )
        except Exception:
            log.exception("Random exploration failed for user %s", user.id)
            await message.reply_text(
                "❌ 随机发送失败，请稍后重试；管理员可检查机器人运行日志。"
            )
        return

    if action[1] == "search":
        await query.answer()
        pending_search_users.add(int(user.id))
        await message.reply_text(
            "🔎 资源搜索\n\n"
            "请直接发送你要查找的资源名称、关键词或编号。\n"
            "例如：电影、写真、视频编号\n\n"
            "无需输入 /search，直接发送文字即可。"
        )
        return

    if action[1] == "searchpage" and len(action) == 4:
        token = action[2]
        try:
            offset = max(0, int(action[3]))
        except ValueError:
            await query.answer("页码无效", show_alert=True)
            return
        session = search_sessions.get(token)
        if not session or int(session["user_id"]) != int(user.id):
            await query.answer("搜索已过期，请重新发送 /search 关键词", show_alert=True)
            return
        search_query = session["query"]
        body, markup, _ = await build_catalog_search_page(
            user.id, search_query, offset=offset, token=token
        )
        await query.answer()
        await message.edit_text(
            body,
            reply_markup=markup,
            disable_web_page_preview=True,
        )
        return

    if action[1] in ("folders", "foldersort"):
        await query.answer()
        if action[1] == "foldersort":
            folder_sort = await save_folder_list_sort(
                user.id, str(action[2]) if len(action) > 2 else "created"
            )
        else:
            folder_sort = await get_folder_list_sort(user.id)
        now = asyncio.get_running_loop().time()
        if category_catalog_cache["expires_at"] > now:
            categories = category_catalog_cache["rows"]
        else:
            categories = await db.list_categories()
            category_catalog_cache["rows"] = [dict(row) for row in categories]
            category_catalog_cache["expires_at"] = now + 30
            categories = category_catalog_cache["rows"]
        # Sort cached folder summaries in memory; no extra catalog scan is needed.
        categories = sorted(
            categories,
            key=lambda row: str(row["category"] or "").casefold(),
        )
        if folder_sort == "created":
            categories.sort(
                key=lambda row: str(row["created_at"] or ""), reverse=True
            )
        elif folder_sort == "count":
            categories.sort(
                key=lambda row: int(row["file_count"] or 0), reverse=True
            )

        page_size = 10
        total_pages = (len(categories) + page_size - 1) // page_size
        if action[1] == "folders" and len(action) > 2:
            try:
                page = max(0, int(action[2]))
            except (TypeError, ValueError):
                page = 0
        else:
            # Opening the folder list from the home menu always starts at page 1.
            # Explicit pagination callbacks still preserve their requested page.
            page = 0
        page = min(page, max(0, total_pages - 1))
        await save_folder_list_page(user.id, page)
        start_at = page * page_size
        selected = categories[start_at:start_at + page_size]
        rows = [
            [InlineKeyboardButton(
                f"{index + 1}. 📁 {row['category']}（{int(row['file_count'] or 0)}）",
                callback_data=f"uc:folderstats:{row['share_key']}:{page}",
            )]
            for index, row in enumerate(selected)
        ]
        for chunk_start in range(0, len(selected), 5):
            chunk = selected[chunk_start:chunk_start + 5]
            rows.append([
                InlineKeyboardButton(
                    str(index),
                    callback_data=f"uc:folderstats:{row['share_key']}:{page}",
                )
                for index, row in enumerate(chunk, start=chunk_start + 1)
            ])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"uc:folders:{page - 1}"))
        if start_at + page_size < len(categories):
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"uc:folders:{page + 1}"))
        sort_labels = {
            "created": "最新创建",
            "name": "名称排序",
            "count": "资源数量",
        }
        sort_buttons = [
            InlineKeyboardButton(
                ("✅ " if folder_sort == key else "") + label,
                callback_data=f"uc:foldersort:{key}",
            )
            for key, label in (
                ("created", "最新创建"),
                ("name", "名称排序"),
                ("count", "资源数量"),
            )
        ]
        rows.append(sort_buttons)
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton("⬅️ 返回首页", callback_data="uc:home")])
        list_text = (
            f"🗂️ 文件夹列表（共 {len(categories)} 个，第 {page + 1} 页）\n"
            f"↕️ 当前排序：{sort_labels[folder_sort]}\n\n"
            "点击文件夹名称复制目录名；点击下方数字打开目录详情。"
            if categories else "🗂️ 暂无文件夹分类。"
        )
        copy_rows = [
            [{
                "text": f"{index + 1}. 📁 {row['category']}（{int(row['file_count'] or 0)}）",
                "copy_text": {"text": str(row["category"])},
            }]
            for index, row in enumerate(selected)
        ]
        for chunk_start in range(0, len(selected), 5):
            chunk = selected[chunk_start:chunk_start + 5]
            copy_rows.append([{
                "text": str(index),
                "callback_data": f"uc:folderstats:{row['share_key']}:{page}",
            } for index, row in enumerate(chunk, start=chunk_start + 1)])
        nav_buttons = []
        if page > 0:
            nav_buttons.append({"text": "⬅️ 上一页", "callback_data": f"uc:folders:{page - 1}"})
        if start_at + page_size < len(categories):
            nav_buttons.append({"text": "下一页 ➡️", "callback_data": f"uc:folders:{page + 1}"})
        copy_rows.append([{
            "text": ("✅ " if folder_sort == key else "") + label,
            "callback_data": f"uc:foldersort:{key}",
        } for key, label in (
            ("created", "最新创建"),
            ("name", "名称排序"),
            ("count", "资源数量"),
        )])
        if nav_buttons:
            copy_rows.append(nav_buttons)
        copy_rows.append([{"text": "⬅️ 返回首页", "callback_data": "uc:home"}])
        try:
            await telegram_bot_api("editMessageText", {
                "chat_id": message.chat.id,
                "message_id": message.id,
                "text": list_text,
                "reply_markup": {"inline_keyboard": copy_rows},
            })
        except Exception:
            log.exception("Could not render folder copy buttons; showing selectable folder names")
            fallback_names = "\n".join(
                f"{start_at + index + 1}. {row['category']}（{int(row['file_count'] or 0)}）"
                for index, row in enumerate(selected)
            )
            fallback_text = list_text + (
                "\n\n" + fallback_names + "\n\n可长按上方目录名称复制。"
                if fallback_names else ""
            )
            fallback_rows = rows[len(selected):]
            await message.edit_text(
                fallback_text,
                reply_markup=InlineKeyboardMarkup(fallback_rows),
            )
        return

    if action[1] == "folderstats" and len(action) >= 3:
        try:
            share_key = str(action[2])
            page = max(0, int(action[3])) if len(action) > 3 else 0
            category_ref = await db.get_category_by_share_key(share_key)
            if not category_ref and share_key.isdigit():
                category_ref = await db.get_category_by_index(int(share_key))
            if not category_ref:
                raise IndexError("folder no longer exists")

            # get_category_by_share_key returns the compact DB row (name/file_count),
            # while the folder detail view needs the complete cached summary fields.
            category_rows = await db.list_categories(category=str(category_ref["name"]))
            if not category_rows:
                raise IndexError("folder summary no longer exists")
            category_row = dict(category_rows[0])
            sizes = await db.get_category_storage_stats(category_row["category"])
        except (ValueError, IndexError):
            await query.answer("目录已更新，请重新打开列表", show_alert=True)
            return
        except Exception:
            log.exception(
                "Could not load member folder details: user=%s share_key=%s",
                user.id, share_key,
            )
            await message.reply_text("❌ 文件夹详情读取失败，请稍后重试。")
            return

        total = int(category_row["file_count"] or 0)
        measured = int(sizes["sized_count"] or 0)
        body = [
            f"📁 #{category_row['category']}",
            "",
            "📊 文件夹统计",
            f"📦 资源总数：{total} 个",
            f"🎬 视频：{int(category_row['video_count'] or 0)} 个，约 {format_folder_size(sizes['video_size'])}",
            f"🖼 图片：{int(category_row['image_count'] or 0)} 张，约 {format_folder_size(sizes['image_size'])}",
            f"🎞 动图：{int(category_row['animation_count'] or 0)} 个",
            f"🎵 音频：{int(category_row['audio_count'] or 0)} 个",
            f"📄 文档及其他：{int(category_row['document_count'] or 0)} 个，约 {format_folder_size(sizes['other_size'])}",
            f"💾 已统计容量：{format_folder_size(category_row['total_size'])}",
            f"📏 文件大小已知：{measured}/{total} 个",
        ]
        if measured < total:
            body.extend(["", "⚠️ 部分历史文件没有可用大小信息，容量为已知文件的合计，不代表完整总量。"])
        await message.edit_text(
            "\n".join(body),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📂 查看内容（直接发送文件）", callback_data=f"uc:getfolder:{category_row['share_key']}:0")],
                [InlineKeyboardButton("📁 文件夹列表", callback_data=f"uc:folders:{page}")],
            ]),
        )
        return

    if action[1] in ("folder", "folderkey", "folderresume") and len(action) >= 3:
        try:
            if action[1] == "folderkey":
                category_row = await db.get_category_by_share_key(str(action[2]))
                if not category_row:
                    raise IndexError("folder no longer exists")
                index = -1
            else:
                index = int(action[2])
                cached_rows = category_catalog_cache["rows"]
                if category_catalog_cache["expires_at"] > asyncio.get_running_loop().time() and 0 <= index < len(cached_rows):
                    category_row = dict(cached_rows[index])
                else:
                    category_row = await db.get_category_by_index(index)
            if not category_row:
                raise IndexError("folder index no longer exists")
            category = category_row["category"]
            if action[1] == "folderresume":
                offset, group_id, is_favorite = await asyncio.gather(
                    get_saved_folder_offset(
                        user.id, category_row["share_key"], category_row["file_count"]
                    ),
                    get_member_group_id(),
                    db.is_category_favorite(user.id, category_row["share_key"]),
                )
            else:
                offset = max(0, int(action[3])) if len(action) > 3 else 0
                offset = min(
                    (offset // 20) * 20,
                    max(0, ((int(category_row["file_count"] or 0) - 1) // 20) * 20),
                )
                group_id, is_favorite = await asyncio.gather(
                    get_member_group_id(),
                    db.is_category_favorite(user.id, category_row["share_key"]),
                )
        except (ValueError, IndexError):
            await query.answer("分类已更新，请重新打开列表", show_alert=True)
            return
        files, _ = await asyncio.gather(
            db.list_category_files(category, limit=20, offset=offset),
            save_folder_offset(user.id, category_row["share_key"], offset),
        )
        page = offset // 20
        category_row = dict(category_row)
        category_row["category_index"] = index
        claimed = await user_claimed_file_keys(user.id, files)
        await message.edit_text(
            folder_detail_text(category_row, files, page, group_id, claimed),
            reply_markup=folder_detail_keyboard(
                category_row, page, len(files) == 20, is_favorite
            ),
            disable_web_page_preview=True,
        )
        return

    if action[1] in ("history", "favorites"):
        await query.answer()
        if action[1] == "history":
            records = await db.list_user_history(user.id, 12)
            rows = [[InlineKeyboardButton(
                f"📄 {row['file_name'][:32]}",
                callback_data=f"uc:open:{row['file_key']}",
            )] for row in records]
            body = "🕘 我的领取记录\n\n" + (
                "\n".join(f"• {row['file_name'][:48]}（{row['category']}）" for row in records)
                if records else "你还没有获取过资源。\n"
                "去首页「资源目录」里挑一个目录领取，领取记录会保存在这里。"
            )
            rows.append([InlineKeyboardButton("⬅️ 返回首页", callback_data="uc:home")])
            await message.edit_text(body, reply_markup=InlineKeyboardMarkup(rows))
        else:
            body, markup = await build_favorites_view(user.id)
            await message.edit_text(body, reply_markup=markup)
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
            offset = await get_saved_folder_offset(user.id, share_key, row["file_count"])
            await save_folder_offset(user.id, share_key, offset)
            files = await db.list_category_files(row["category"], limit=20, offset=offset)
            page = offset // 20
            await message.edit_text(
                folder_detail_text(
                    row, files, page, await get_member_group_id(),
                    await user_claimed_file_keys(user.id, files),
                ),
                reply_markup=folder_detail_keyboard(row, page, len(files) == 20, added),
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
        offset = await get_saved_folder_offset(user.id, action[2], row["file_count"])
        await save_folder_offset(user.id, action[2], offset)
        files = await db.list_category_files(row["category"], limit=20, offset=offset)
        page = offset // 20
        await query.answer()
        await message.edit_text(
            folder_detail_text(
                row, files, page, await get_member_group_id(),
                await user_claimed_file_keys(user.id, files),
            ),
            reply_markup=folder_detail_keyboard(
                row, page, len(files) == 20,
                await db.is_category_favorite(user.id, action[2])
            ),
            disable_web_page_preview=True,
        )
        return

    if action[1] == "resend" and len(action) == 4:
        # Explicitly re-send a batch that was skipped as "already delivered".
        # Progress is left untouched: this must never move the folder cursor.
        share_key = action[2]
        try:
            start = max(0, int(action[3]))
        except ValueError:
            await query.answer("参数无效，请重新打开目录。")
            return
        now = asyncio.get_running_loop().time()
        remaining = 5.0 - (now - float(last_getfolder_click_at.get(int(user.id), 0.0)))
        if remaining > 0:
            await query.answer(f"⏳ 操作太快，请 {remaining:.1f} 秒后再试。", show_alert=True)
            return
        last_getfolder_click_at[int(user.id)] = now
        category = await db.get_category_by_share_key(share_key)
        if not category:
            await query.answer("文件夹不存在或已被移除。", show_alert=True)
            return
        if not await has_access(user.id, force_refresh=True):
            await query.answer("⛔ 请先加入管理员指定的会员群后再获取资源。", show_alert=True)
            return
        pace = None if await is_admin(user.id) else delivery_pace_hint(user.id)
        if pace:
            await query.answer(pace, show_alert=True)
            return
        delivery_key = (int(user.id), str(share_key))
        if delivery_key in category_delivery_in_progress:
            await query.answer("⏳ 这一批资源正在发送，请稍候。", show_alert=True)
            return
        category_delivery_in_progress.add(delivery_key)
        try:
            batch = await db.list_category_files(
                category["name"], limit=20, offset=start
            )
            batch = await db.complete_album_groups(batch)
            if not batch:
                await query.answer("这个位置已经没有资源了。", show_alert=True)
                return
            await query.answer("开始重新发送…")
            sent, failed, skipped, _already, _next, _last = await send_catalog_batch(
                client, user.id, f"member:{share_key}", start, batch,
                track_progress=False, ignore_history=True,
            )
            lines = ["🔁 重新发送完成\n", f"✅ 成功发送：{sent} 个"]
            if failed:
                lines.append(f"❌ 发送失败：{failed} 个（可稍后重试）")
            if skipped:
                lines.append(f"⚠️ 多次失败已跳过：{skipped} 个")
            lines.append("\n领取进度没有变化，可以继续领取后面的资源。")
            try:
                await client.send_message(user.id, "\n".join(lines))
            except Exception:
                log.exception("Could not send resend summary: user=%s", user.id)
        except Exception:
            log.exception("Resend batch failed: user=%s share_key=%s", user.id, share_key)
            try:
                await query.answer("重新发送时出错，请稍后再试。", show_alert=True)
            except Exception:
                pass
        finally:
            category_delivery_in_progress.discard(delivery_key)
        return

    if (action[1] == "getfolder" and len(action) == 4) or (
        action[1] == "reget" and len(action) == 3
    ):
        share_key = action[2]
        restart_folder = action[1] == "reget"
        if restart_folder:
            offset = 0
        else:
            try:
                offset = max(0, int(action[3]))
            except ValueError:
                await message.reply_text("⚠️ 批次参数无效，请重新打开目录。")
                return
        category = await db.get_category_by_share_key(share_key)
        if not category:
            await message.reply_text("⚠️ 文件夹不存在或已被移除。")
            return
        delivery_key = (int(user.id), str(share_key))
        if delivery_key in category_delivery_in_progress:
            await message.reply_text("⏳ 这一批资源正在发送，请稍候，不要重复点击。")
            return
        category_delivery_in_progress.add(delivery_key)
        try:
            # The category row stores counts refreshed only when files are
            # published or scanned; do not COUNT(*) over the files table per click.
            total = int(category["file_count"] or 0)
            # Keep member delivery progress separate from legacy admin-send progress.
            member_progress_key = f"member:{share_key}"
            saved_offset = await db.get_category_delivery_progress(user.id, member_progress_key)
            # Older releases advanced this cursor even when Telegram rejected a
            # copy. Repair each existing member/folder cursor once from successful
            # delivery history, without deleting that history or resetting users.
            # Positions shift down whenever new resources are published, so a
            # stored cursor can land on files the member already received.
            # Recompute it from delivery history on every claim.
            try:
                saved_offset = await db.compute_member_cursor_from_history(
                    user.id, member_progress_key, category["name"],
                    max_attempts=DELIVERY_MAX_ATTEMPTS,
                )
            except Exception:
                log.exception(
                    "Could not recompute member cursor; using the stored progress: "
                    "user=%s share_key=%s", user.id, share_key,
                )
                saved_offset = await db.get_category_delivery_progress(
                    user.id, member_progress_key
                )

            # A completed member/folder cursor restarts only after the member
            # taps 「✅ 重新领取」, so one stray tap cannot resend the whole
            # folder. The confirmation itself carries no state change.
            repeat_delivery = bool(total > 0 and saved_offset >= total)
            if restart_folder:
                await db.reset_category_delivery_progress(user.id, member_progress_key)
                saved_offset = 0
            elif repeat_delivery:
                await query.answer("这个目录已经领完了")
                await message.edit_text(
                    "🔄 这个目录已经领完了\n\n"
                    f"📁 目录：{category['name']}\n"
                    f"📦 资源：{total} 个\n\n"
                    "重新领取会从第一个资源开始，整本再发一遍（每批 20 个）。",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(
                            "✅ 重新领取", callback_data=f"uc:reget:{share_key}"
                        )],
                        [InlineKeyboardButton(
                            "↩️ 返回目录", callback_data=f"uc:folderlink:{share_key}"
                        )],
                    ]),
                )
                return

            # The database cursor is authoritative; stale Telegram buttons must
            # not jump over resources that were never delivered.
            offset = saved_offset
            remaining = max(0, total - offset)
            batch = await db.list_category_files(
                category["name"], limit=min(20, remaining), offset=offset
            )
            batch = await db.complete_album_groups(batch)
            if not batch:
                await message.reply_text("📭 这个目录的资源已经领取完毕。")
                return
            # Do not post a status message before the files: the final
            # delivery summary is sent only after every resource message.
            last_sent_message_id = None
            next_offset = offset
            has_more = False
            try:
                pace = None if await is_admin(user.id) else delivery_pace_hint(user.id)
                if pace:
                    await message.reply_text(pace)
                    return
                sent, failed, skipped, already, next_offset, last_sent_message_id = await send_catalog_batch(
                    client, user.id, member_progress_key, offset, batch,
                    ignore_history=repeat_delivery,
                )
                # The stored folder count is refreshed lazily, so it can lag a
                # fresh upload. Probe past the cursor before declaring "done".
                has_more = next_offset < total
                if not has_more and next_offset > 0:
                    try:
                        has_more = bool(await db.list_category_files(
                            category["name"], limit=1, offset=next_offset
                        ))
                    except Exception:
                        log.exception("Could not probe folder tail: share_key=%s", share_key)
                # Never claim success when nothing was actually delivered.
                if sent:
                    header = "📦 本批资源发送完成！"
                elif already and not failed:
                    header = "ℹ️ 本批没有重复发送"
                else:
                    header = "⚠️ 本批没有文件成功发出"
                lines = [
                    header + "\n",
                    f"📁 目录：{category['name']}",
                    f"📥 本批处理：{len(batch)} 个",
                    f"✅ 成功发送：{sent} 个",
                ]
                if failed:
                    lines.append(f"❌ 发送失败：{failed} 个（已保留，下次继续尝试）")
                if already:
                    lines.append(f"⏭ 之前已领过、未重复发送：{already} 个")
                if skipped:
                    lines.append(f"⚠️ 多次失败已跳过：{skipped} 个")
                lines.append(f"📊 领取进度：{next_offset}/{max(total, next_offset)}")
                if already and not sent and not failed:
                    lines.append(
                        "\n上面这些资源你之前已经领取过，机器人不会重复发送，"
                        "所以这一批没有文件。确实要再拿一次，点下方「🔁 重新发送本批」。"
                    )
                else:
                    lines.append(
                        "\n还有资源，点击下方按钮继续领取（单文件每批20个，相册整组）。"
                        if has_more else "\n🎉 该目录资源已全部领取完毕。"
                    )
                text_result = "\n".join(lines) + "\n"
            except Exception as exc:
                log.exception(
                    "Folder resource delivery aborted: user=%s share_key=%s",
                    user.id, share_key,
                )
                try:
                    next_offset = await db.get_category_delivery_progress(
                        user.id, member_progress_key
                    )
                except Exception:
                    log.exception("Could not reload folder delivery progress")
                text_result = (
                    "⚠️ 本批资源发送过程中出现异常，发送已中止。\n"
                    "请稍后继续领取，已成功处理的进度会保留。"
                )

            button_rows = []
            if already and not sent:
                button_rows.append([InlineKeyboardButton(
                    "🔁 重新发送本批",
                    callback_data=f"uc:resend:{share_key}:{offset}",
                )])
            if has_more:
                button_rows.append([InlineKeyboardButton(
                    "📥 继续领取（单文件20个，相册整组）",
                    callback_data=f"uc:getfolder:{share_key}:{next_offset}",
                )])
            button_rows.extend([
                [InlineKeyboardButton("📁 返回文件夹", callback_data=f"uc:folderlink:{share_key}")],
                [InlineKeyboardButton("🏠 返回主菜单", callback_data="uc:home")],
            ])
            buttons = InlineKeyboardMarkup(button_rows)
            # Send a standalone message only after the entire media batch has
            # finished. Do not reply to an earlier message: Telegram clients may
            # visually anchor/reposition reply previews away from the bottom.
            try:
                await client.send_message(
                    user.id,
                    text_result,
                    reply_markup=buttons,
                )
            except Exception:
                log.exception(
                    "Could not send standalone delivery summary; falling back to Bot API: user=%s",
                    user.id,
                )
                api_markup = {
                    "inline_keyboard": (
                        [[{
                            "text": "📥 继续领取（单文件20个，相册整组）",
                            "callback_data": f"uc:getfolder:{share_key}:{next_offset}",
                        }]] if has_more else []
                    ) + [
                        [{"text": "📁 返回文件夹", "callback_data": f"uc:folderlink:{share_key}"}],
                        [{"text": "🏠 返回主菜单", "callback_data": "uc:home"}],
                    ]
                }
                try:
                    await telegram_bot_api("sendMessage", {
                        "chat_id": user.id,
                        "text": text_result,
                        "reply_markup": api_markup,
                    })
                except Exception:
                    log.exception(
                        "Bot API summary fallback failed; trying reply to folder message: user=%s",
                        user.id,
                    )
                    try:
                        await message.reply_text(text_result, reply_markup=buttons)
                    except Exception:
                        log.exception(
                            "All delivery summary methods failed: user=%s share_key=%s",
                            user.id, share_key,
                        )
            return
        finally:
            category_delivery_in_progress.discard(delivery_key)

    if action[1] == "open" and len(action) == 3:
        guard = (int(user.id), str(action[2]))
        if guard in file_open_in_progress:
            await query.answer("⏳ 这个资源正在发送，请稍候，不要重复点击。")
            return
        file_open_in_progress.add(guard)
        try:
            if not await has_access(user.id, force_refresh=True):
                await query.answer("⛔ 请先加入管理员指定的会员群，再获取资源。", show_alert=True)
                return
            pace = None if await is_admin(user.id) else delivery_pace_hint(user.id)
            if pace:
                await query.answer(pace, show_alert=False)
                return
            ok, result = await send_catalog_file(client, user.id, action[2])
            await query.answer(result, show_alert=not ok)
        finally:
            file_open_in_progress.discard(guard)
        return

    if action[1] == "star" and len(action) >= 3:
        record = await db.get_file(action[2])
        if not record:
            await query.answer("文件不存在或已被移除", show_alert=True)
            return
        added = await db.toggle_favorite(user.id, action[2])
        await query.answer("⭐ 已加入收藏" if added else "已取消收藏")
        if len(action) > 3 and action[3] == "favorites":
            body, markup = await build_favorites_view(user.id)
            await message.edit_text(body, reply_markup=markup)
        return


async def render_backup_admin(message, notice=""):
    enabled = await db.get_setting("backup_retry_enabled") != "0"
    counts = await db.backup_retry_stats()
    _, backup_channel_id = await get_storage_channels()
    restart_pending = await db.get_setting("backup_retry_full_restart_pending") == "1"
    state = "🟢 自动运行已开启" if enabled else "⏸ 自动备份已暂停"
    worker = "运行中" if backup_worker_running else "尚未启动"
    restart_state = "⏳ 等待后台空闲后从头复制" if restart_pending else "可继续备份"
    body = (
        "📦 备份管理\n"
        "━━━━━━━━━━━━━━\n"
        f"⚙️ 自动备份：{state}\n"
        f"🖥 后台任务：{worker}\n"
        f"📍 备用频道：{backup_channel_id or '未绑定'}\n"
        f"🔄 任务状态：{restart_state}\n"
        "━━━━━━━━━━━━━━\n"
        "📊 复制统计\n"
        f"✅ 已复制：{counts['backed']} 个资源\n"
        f"⏳ 待复制单文件：{counts['candidates']} 个\n"
        f"🖼 待复制相册组：{counts['album_candidates']} 组\n"
        f"📦 队列任务：{counts['queued']} 个\n"
        f"⚠️ 失败待重试：{counts['failed']} 个"
    )
    if notice:
        body = notice + "\n\n" + body
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏮️ 从头开始", callback_data="mc:backupstart"),
         InlineKeyboardButton("⏯️ 继续上次", callback_data="mc:backupcontinue")],
        [InlineKeyboardButton("⏸ 暂停备份", callback_data="mc:backuppause"),
         InlineKeyboardButton("📊 刷新进度", callback_data="mc:backuprefresh")],
        [InlineKeyboardButton("⚡ 立即执行一轮", callback_data="mc:backuprun")],
        [InlineKeyboardButton("📋 失败清单", callback_data="mc:backupfailures"),
         InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")],
    ])
    await message.edit_text(body, reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"^mc:"))
async def admin_callback_handler(client, query):
    user = query.from_user
    if not user or not await is_admin(user.id):
        await query.answer("无管理员权限", show_alert=True)
        return
    await query.answer()
    action = query.data.split(":", 1)[1]
    message = query.message
    if action == "broadcast:confirm":
        pending = pending_broadcasts.get(int(user.id))
        if not pending:
            await message.reply_text("⚠️ 没有待发送的内容，请重新进入广播管理。")
            return
        if int(user.id) in broadcast_running:
            await message.reply_text("⚠️ 这次广播已经在进行中，请勿重复点击。")
            return
        broadcast_running.add(int(user.id))
        try:
            await message.edit_reply_markup(None)
        except Exception:
            pass
        status = await message.reply_text("⏳ 正在启动广播…")
        spawn_background(
            f"broadcast-{int(user.id)}",
            run_broadcast_guarded(client, int(user.id), status),
        )
        return
    if action == "broadcast:cancel":
        pending_broadcasts.pop(int(user.id), None)
        broadcast_input_mode.discard(int(user.id))
        for key, task in list(broadcast_album_tasks.items()):
            if key[0] == int(user.id):
                if task and not task.done():
                    task.cancel()
                broadcast_album_tasks.pop(key, None)
                broadcast_album_buffers.pop(key, None)
        await message.edit_text("❌ 已取消本次广播，没有向用户发送任何内容。", reply_markup=back_keyboard())
        return
    if action == "categorycancel":
        pending_category_actions.pop(user.id, None)
        active_batch = await db.get_upload_batch(user.id)
        await message.edit_text("已取消建立文件夹，没有创建任何目录。")
        await client.send_message(
            user.id,
            "已退出创建文件夹流程，可以继续操作。",
            reply_markup=upload_active_keyboard() if active_batch else admin_home_keyboard(),
        )
        return
    if action == "uploadcontinue":
        await message.edit_text(
            f"📥 当前已收到：{upload_received_counts.get(int(user.id), 0)} 个文件。\n"
            "继续发送的文件仍加入当前文件夹和同一批次，不会发布或清空已有计数。",
            reply_markup=None,
        )
        return
    if action == "uploadfinish":
        uid = int(user.id)
        if uid in upload_finish_in_progress:
            return

        upload_finish_in_progress.add(uid)
        timer = pending_upload_confirmation_tasks.get(uid)
        if timer and not timer.done():
            timer.cancel()

        try:
            # Give immediate feedback before waiting for Telegram's channel-copy
            # retries (FloodWait can legitimately take tens of seconds).
            try:
                await message.edit_text(
                    f"⏳ 正在结束上传……\n📥 已接收：{upload_received_counts.get(uid, 0)} 个文件\n"
                    "正在等待后台保存任务完成，请不要重复点击。"
                )
            except Exception as exc:
                if type(exc).__name__ != "MessageNotModified":
                    log.warning("Could not show upload-finish status for user %s: %s", uid, exc)

            while True:
                tasks = list(pending_upload_tasks.get(uid, set()))
                if not tasks:
                    await asyncio.sleep(0.2)
                    tasks = list(pending_upload_tasks.get(uid, set()))
                    if not tasks:
                        break
                await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

            received_count = int(upload_received_counts.get(uid, 0))
            try:
                result = await db.publish_upload_batch(uid)
            except Exception:
                log.exception("Could not publish upload batch for user %s", uid)
                try:
                    await message.edit_text(
                        "❌ 上传文件已完成后台保存，但发布入库失败。\n"
                        "本次批次和接收数量已保留，请检查数据库/部署日志后重试。"
                    )
                except Exception as edit_exc:
                    log.warning("Could not show upload-publish error for user %s: %s", uid, edit_exc)
                return

            if result.get("category") and int(result.get("published", 0)) > 0:
                await notify_folder_subscribers(client, result["category"], "new_resources")
            failure_suffix = render_failures(upload_failures.get(uid))
            upload_received_counts.pop(uid, None)
            upload_progress_state.pop(uid, None)
            pending_upload_categories.pop(uid, None)
            category_catalog_cache["expires_at"] = 0
            upload_status_messages.pop(uid, None)
            upload_confirmation_messages.pop(uid, None)
            upload_latest_messages.pop(uid, None)
            upload_failures.pop(uid, None)

            if result.get("category") is None:
                completion_text = "目前没有待发布的上传批次。"
            else:
                published = int(result.get("published", 0))
                completion_text = (
                    f"✅ 本次上传完成\n📥 收到文件：{received_count} 个\n"
                    f"📦 成功入库：{published} 个文件"
                    + (f"\n⚠️ 未成功入库：{max(0, received_count - published)} 个，请检查部署日志。" if received_count > published else "")
                )
            completion_text += failure_suffix
            try:
                await message.edit_text(completion_text, reply_markup=None)
            except Exception as exc:
                if type(exc).__name__ != "MessageNotModified":
                    log.warning("Could not edit upload-finish result for user %s: %s", uid, exc)
            # Editing an inline confirmation does not reliably restore the
            # persistent reply keyboard. Send it explicitly after completion.
            await client.send_message(user.id, "请选择下一步操作。", reply_markup=admin_home_keyboard())
        finally:
            upload_finish_in_progress.discard(uid)
        return
    if action == "backup":
        await render_backup_admin(message)
    elif action == "backupfailures":
        failures = await db.list_backup_failures(limit=20)
        if not failures:
            body = "📋 备份失败清单\n\n当前没有超过重试次数的失败文件。"
        else:
            lines = ["📋 备份失败清单（最近 20 条）", "━━━━━━━━━━━━━━"]
            for row in failures:
                name = str(row["file_name"] or "未命名资源")[:40]
                category = str(row["category"] or "未分类")
                error = str(row["last_error"] or "")[:80]
                failed_at = format_cn_datetime(row["failed_at"])
                lines.append(f"• {name}\n　📂 {category}　🕐 {failed_at}\n　⚠️ {error}")
            body = "\n\n".join(lines)
        await message.edit_text(
            body,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ 返回备份管理", callback_data="mc:backup")]
            ]),
        )
    elif action in ("backuprefresh", "backupenable", "backuppause", "backuprun", "backupstart", "backupcontinue"):
        global backup_retry_manual_requested, last_user_activity
        if action == "backupstart":
            primary_channel_id, backup_channel_id = await get_storage_channels()
            if not backup_channel_id:
                notice = "❌ 请先绑定备用频道，再开始备份。"
            elif scanner is None or not scanner.is_connected:
                notice = (
                    "❌ 暂时不能从头备份：扫描账号尚未连接。\n"
                    "请先进入「扫描账号登录」完成登录，再回来点击从头开始。\n"
                    "本次没有重置任何备份 ID，也没有改动原数据。"
                )
            elif not primary_channel_id:
                notice = "❌ 尚未绑定主存储频道，无法从头扫描。没有改动现有备份记录。"
            else:
                # Stop any older /scan task first. Otherwise it could keep
                # advancing the previous cursor while the full restart waits.
                if scan_task and not scan_task.done():
                    scan_task.cancel()
                    try:
                        await scan_task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        log.exception("Previous history scan ended with an error during full restart")
                # Reset the actual durable history cursor immediately on click,
                # not later after the worker becomes idle. Indexed rows remain intact.
                await db.reset_scan_progress_for_full_rescan(primary_channel_id)
                await db.set_setting("history_scan_direction", "newest_first_v1")
                await db.set_setting("backup_full_scan_started", "0")
                await db.set_setting("backup_full_scan_required", "1")
                await db.set_setting("backup_retry_full_restart_pending", "1")
                log.warning(
                    "Admin requested full backup restart: scan cursor reset to 0 for channel %s",
                    primary_channel_id,
                )
                await db.set_setting("backup_retry_enabled", "1")
                backup_retry_manual_requested = True
                last_user_activity = asyncio.get_running_loop().time() - 31
                notice = (
                    "⏳ 已提交全量备份任务。\n"
                    "第一阶段：只扫描主存储频道全部历史，统计并补齐遗漏资源，不向备用频道发送内容。\n"
                    "第二阶段：扫描全部完成后锁定完整资源清单，再按原始消息 ID 顺序转发；相册整组转发并保留来源。\n"
                    "扫描未完成或中断时，不会提前开始备份。\n"
                    "原始频道文件、目录和会员数据均保留。"
                )
        elif action == "backupcontinue":
            await db.set_setting("backup_retry_enabled", "1")
            backup_retry_manual_requested = True
            last_user_activity = asyncio.get_running_loop().time() - 31
            notice = "⏯️ 已继续上次备份；从未完成的任务接着处理，已成功项目不会重复复制。"
        elif action == "backupenable":
            await db.set_setting("backup_retry_enabled", "1")
            notice = "✅ 已开启自动备份。"
        elif action == "backuppause":
            await db.set_setting("backup_retry_enabled", "0")
            backup_retry_manual_requested = False
            notice = "⏸ 已暂停自动备份；已保存的任务不会删除。"
        elif action == "backuprun":
            await db.set_setting("backup_retry_enabled", "1")
            backup_retry_manual_requested = True
            last_user_activity = asyncio.get_running_loop().time() - 31
            notice = "⚡ 已提交立即执行请求；正在进行的上传、扫描和会员发送会优先完成。"
        else:
            notice = ""
        await render_backup_admin(message, notice)
    elif action == "captcha" or action.startswith("captcha:"):
        notice = ""
        if action == "captcha:toggle":
            enabled = await captcha_is_enabled()
            await db.set_setting(CAPTCHA_ENABLED_KEY, "0" if enabled else "1")
            notice = "⏸ 已关闭图片验证码。" if enabled else "✅ 已开启图片验证码。"
        await render_captcha_admin(message, notice)
    elif action == "contentguard" or action.startswith("guard:"):
        if action == "contentguard":
            pass
        else:
            parts = action.split(":")
            if len(parts) == 2 and parts[1] == "protect":
                protect, _ = await get_member_delivery_settings()
                await db.set_setting("member_content_protection", "0" if protect else "1")
            elif len(parts) == 2 and parts[1] == "newcomer":
                enabled = await db.get_setting(NEWCOMER_GUARD_KEY) == "1"
                await db.set_setting(NEWCOMER_GUARD_KEY, "0" if enabled else "1")
                newcomer_guard_cache.clear()
            elif len(parts) == 3 and parts[1] == "newcomerhours":
                try:
                    hours = int(parts[2])
                except ValueError:
                    await query.answer("无效的时长", show_alert=True)
                    return
                if hours not in (12, 24, 48):
                    await query.answer("无效的时长", show_alert=True)
                    return
                await db.set_setting(NEWCOMER_HOURS_KEY, str(hours))
                newcomer_guard_cache.clear()
            elif len(parts) == 3 and parts[1] == "delete":
                try:
                    seconds = int(parts[2])
                except ValueError:
                    seconds = -1
                if seconds not in (0, 60, 300, 600, 1800, 3600, 86400):
                    await query.answer("无效的删除时间", show_alert=True)
                    return
                await db.set_setting("member_auto_delete_seconds", str(seconds))
            else:
                return
        protect, delete_after = await get_member_delivery_settings()
        newcomer_on = await db.get_setting(NEWCOMER_GUARD_KEY) == "1"
        newcomer_hours = await newcomer_guard_hours()
        duration_labels = {0: "关闭", 60: "1 分钟", 300: "5 分钟", 600: "10 分钟", 1800: "30 分钟", 3600: "1 小时", 86400: "24 小时"}
        duration_label = duration_labels.get(delete_after, f"{delete_after // 60} 分钟")
        await message.edit_text(
            "🛡 会员资源内容保护与自动删除\n\n"
            f"内容保护：{'开启' if protect else '关闭'}\n"
            f"自动删除：{duration_label}\n"
            f"新人保护：{'开启，入群后 ' + str(int(newcomer_hours)) + ' 小时内禁止保存/转发' if newcomer_on else '关闭'}\n\n"
            "内容保护用于限制会员转发/保存机器人发出的资源。自动删除只清理会员私聊里的机器人发送消息，不会删除主存储或备用存储频道原件。\n"
            "新人保护按入群时间计算，只记录部署之后新入群的人；已经在群里的老成员不受影响。\n\n请选择要修改的项目：",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'✅' if protect else '⏸'} 内容保护开关", callback_data="mc:guard:protect"),
                 InlineKeyboardButton(f"{'✅' if newcomer_on else '⏸'} 新人保护", callback_data="mc:guard:newcomer")],
                [InlineKeyboardButton("新人 12 小时", callback_data="mc:guard:newcomerhours:12"),
                 InlineKeyboardButton("新人 24 小时", callback_data="mc:guard:newcomerhours:24")],
                [InlineKeyboardButton("新人 48 小时", callback_data="mc:guard:newcomerhours:48"),
                 InlineKeyboardButton("关闭自动删除", callback_data="mc:guard:delete:0")],
                [InlineKeyboardButton("1 分钟", callback_data="mc:guard:delete:60"),
                 InlineKeyboardButton("5 分钟", callback_data="mc:guard:delete:300")],
                [InlineKeyboardButton("10 分钟", callback_data="mc:guard:delete:600"),
                 InlineKeyboardButton("30 分钟", callback_data="mc:guard:delete:1800")],
                [InlineKeyboardButton("1 小时", callback_data="mc:guard:delete:3600"),
                 InlineKeyboardButton("24 小时", callback_data="mc:guard:delete:86400")],
                [InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")],
            ]),
        )
    elif action == "welcome" or action.startswith("welcome:"):
        parts = action.split(":")
        op = parts[1] if len(parts) > 1 else ""
        notice = ""
        if op == "toggle":
            enabled, _ = await get_welcome_settings()
            await db.set_setting(WELCOME_ENABLED_KEY, "0" if enabled else "1")
            notice = "✅ 已切换自动欢迎开关。"
        elif op == "del":
            try:
                seconds = int(parts[2]) if len(parts) > 2 else -1
            except ValueError:
                seconds = -1
            if seconds not in WELCOME_AUTO_DELETE_ALLOWED:
                await query.answer("无效的删除时间", show_alert=True)
                return
            await db.set_setting(WELCOME_AUTO_DELETE_KEY, str(seconds))
            notice = "✅ 已改为不自动删除。" if seconds == 0 else "✅ 欢迎语将按新时间自动删除。"
        elif op == "reset":
            await db.set_setting(WELCOME_MESSAGE_KEY, "")
            notice = "♻️ 已恢复默认欢迎文案。"
        elif op == "edit":
            pending_welcome_edit.add(int(user.id))
            await message.edit_text(
                "✏️ 编辑进群欢迎文案\n\n"
                "请直接发送新的欢迎文案。\n"
                "可用占位：{name}=新人称呼，{bot}=机器人入口。\n"
                "发送 /cancel 可取消。",
                reply_markup=back_keyboard(),
            )
            return
        await render_welcome_admin(message, notice)
    elif action == "announce" or action.startswith("announce:"):
        parts = action.split(":")
        op = parts[1] if len(parts) > 1 else ""
        if op == "edit":
            pending_announce_edit.add(int(user.id))
            await message.edit_text(
                "✏️ 编辑首页公告\n\n请直接发送要展示在首页的公告内容（最多 300 字）。\n"
                "发送 /cancel 可取消。",
                reply_markup=back_keyboard(),
            )
            return
        if op == "clear":
            await db.set_setting(ANNOUNCEMENT_KEY, "")
            await render_announce_admin(message, "🗑 公告已清空。")
            return
        await render_announce_admin(message)
    elif action == "more":
        await message.edit_text(
            "⚙️ 更多设置\n━━━━━━━━━━━━━━\n"
            "扫描、内容保护、编号查询、管理员与首页配置都在这里。\n"
            "返回「⚙️ 资源管理中心」可回到常用功能。",
            reply_markup=admin_more_keyboard(),
        )
    elif action == "home":
        users, files, _, banned = await db.stats()
        activity = await db.activity_stats()
        group_id = await get_member_group_id()
        primary, backup = await get_storage_channels()
        text = (
            "⚙️ 资源管理中心\n━━━━━━━━━━━━━━\n"
            f"👥 会员群：{'已绑定' if group_id else '未绑定'}\n"
            f"🗄 主存储：{'已绑定' if primary else '未绑定'}\n"
            f"📦 备用存储：{'已绑定' if backup else '未绑定'}\n\n"
            f"🟢 今日活跃：{activity['day']} 人（7 天 {activity['week']} 人）\n"
            f"👤 注册总数：{users}    📁 资源：{files}\n"
            f"🚫 封禁用户：{banned}\n\n请选择需要管理的功能。"
        )
        await message.edit_text(text, reply_markup=admin_keyboard())
    elif action == "audit":
        try:
            totals, top = await db.get_delivery_audit_summary(hours=24, limit=5)
            total = int(totals.get("total") or 0)
            success = int(totals.get("success") or 0)
            failed = int(totals.get("failed") or 0)
            pending = int(totals.get("pending") or 0)
            lines = [
                "🧾 资源发放审计",
                "━━━━━━━━━━━━━━",
                "最近24小时",
                f"发放记录：{total} 条",
                f"✅ 成功：{success}｜❌ 失败：{failed}｜⏳ 未完成：{pending}",
                "",
                "🔎 高频资源",
            ]
            if top:
                for index, row in enumerate(top, 1):
                    name = str(row.get("file_name") or row.get("file_key") or "未知资源")[:45]
                    lines.append(
                        f"{index}. {name}\n"
                        f"目录：{str(row.get('category') or '未分类')[:25]}"
                        f"｜发送 {int(row.get('sends') or 0)} 次"
                    )
            else:
                lines.append("暂无记录；部署审计版本后产生的发送才会累计。")
            lines.extend([
                "",
                "可查看每次发送的资源、接收用户和时间；封禁时间回溯请使用 /auditban。",
            ])
            await message.edit_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🧾 最近发送明细", callback_data="mc:auditdetail"),
                 InlineKeyboardButton("🔄 刷新审计", callback_data="mc:audit")],
                [InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")],
                ]),
            )
        except Exception:
            log.exception("Could not render delivery audit settings")
            await message.edit_text(
                "⚠️ 审计数据暂时无法读取，请检查数据库初始化和部署日志。",
                reply_markup=back_keyboard(),
            )
    elif action == "auditdetail":
        try:
            rows = await db.list_delivery_audit_details(limit=20)
            lines = ["🧾 最近20条资源发送明细"]
            resend_buttons = []
            if not rows:
                lines.append("暂无逐条发送记录。新版本部署后产生的发送才会记录。")
            else:
                for row in rows:
                    stamp = format_cn_datetime(row.get("created_at"))
                    name = str(row.get("file_name") or row.get("file_key") or "未知资源")[:45]
                    category = str(row.get("category") or "未分类")[:25]
                    status_key = str(row.get("status"))
                    status = {"success": "成功", "failed": "失败", "attempted": "未完成"}.get(
                        status_key, status_key or "未知"
                    )
                    lines.append(
                        f"\n⏱ {stamp}\n📄 {name}\n📁 {category}"
                        f"\n用户：{row.get('user_id')}｜{status}"
                    )
                    if status_key == "failed":
                        reason = humanize_delivery_error(row.get("error_text"))
                        lines.append(f"原因：{reason}")
                        audit_id = str(row.get("audit_id") or "")
                        resend_key = str(row.get("file_key") or "")
                        if audit_id:
                            resend_data = (
                                f"mc:resend:{audit_id}:{resend_key}"
                                if resend_key else f"mc:resend:{audit_id}"
                            )
                            resend_buttons.append([InlineKeyboardButton(
                                f"🔁 重发：{name[:24]}",
                                callback_data=resend_data,
                            )])
            body = "\n".join(lines)
            nav = [[InlineKeyboardButton("🧾 返回领取审计", callback_data="mc:audit"),
                    InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")]]
            await message.edit_text(
                body[:3900],
                reply_markup=InlineKeyboardMarkup(resend_buttons + nav),
            )
        except Exception:
            log.exception("Could not render delivery audit details")
            await message.edit_text("⚠️ 发送明细暂时无法读取。", reply_markup=back_keyboard())
    elif action == "resend" or action.startswith("resend:"):
        parts = action.split(":")
        if len(parts) < 2 or not parts[1]:
            await query.answer("记录不存在。", show_alert=True)
            return
        resend_key = parts[2] if len(parts) > 2 else ""
        audit = await db.get_delivery_audit_row(parts[1], resend_key or None)
        if not audit:
            await query.answer("该领取记录已找不到，可能已被清理。", show_alert=True)
            return
        target_user = int(audit.get("user_id") or 0)
        file_key = str(audit.get("file_key") or "")
        if not target_user or not file_key:
            await query.answer("记录缺少接收人或资源信息，无法补发。", show_alert=True)
            return
        ok, reason = await send_catalog_file(client, target_user, file_key)
        if ok:
            await query.answer("✅ 已重新发送给该用户。", show_alert=False)
        else:
            await query.answer(reason, show_alert=True)
    elif action == "wm":
        try:
            rows = await db.list_recent_watermarks(limit=10)
            lines = [
                "🆔 领取编号查询",
                "━━━━━━━━━━━━━━",
                "下面是最近 10 个发放编号，点一行看详情；",
                "也可以直接发送命令 /wm 编号 精确查询。",
                "",
            ]
            buttons = []
            if not rows:
                lines.append("暂无编号记录（部署新版后产生领取才会有）。")
            else:
                for index, row in enumerate(rows, 1):
                    code = str(row["watermark"])
                    stamp = format_cn_datetime(row["created_at"], "%m-%d %H:%M")
                    name = str(row["file_key"] or "")[:34]
                    lines.append(
                        f"{index}. {code}｜用户 {int(row['user_id'])}｜{stamp}"
                    )
                    buttons.append([
                        InlineKeyboardButton(
                            f"🔍 {code} · 用户{int(row['user_id'])}",
                            callback_data=f"mc:wm:{code}",
                        )
                    ])
            lines.append("")
            lines.append("编号写在会员收到的文件说明里，可用于定位外传/转卖的人。")
            buttons.append([InlineKeyboardButton("🔄 刷新编号列表", callback_data="mc:wm")])
            buttons.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
            await message.edit_text(
                "\n".join(lines)[:3800],
                reply_markup=InlineKeyboardMarkup(buttons),
            )
        except Exception:
            log.exception("Could not render watermark index")
            await message.edit_text(
                "⚠️ 编号列表暂时无法读取，请检查数据库状态。",
                reply_markup=back_keyboard(),
            )
    elif action.startswith("wm:"):
        code = action.split(":", 1)[1].strip().upper()
        try:
            rows = await db.find_delivery_by_watermark(code, limit=10)
            await message.edit_text(
                watermark_detail_text(code, rows),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ 返回编号列表", callback_data="mc:wm")],
                    [InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")],
                ]),
            )
        except Exception:
            log.exception("Could not render watermark detail: %s", code)
            await message.edit_text("⚠️ 编号详情暂时无法读取。", reply_markup=back_keyboard())
    elif action == "storage":
        primary, backup = await get_storage_channels()
        group_id = await get_member_group_id()
        await message.edit_text(
            "🗄 绑定设置\n\n"
            f"会员群：{group_id or '未绑定'}\n主存储频道：{primary or '未绑定'}\n备用存储（频道/群组）：{backup or '未绑定'}\n\n选择需要绑定或刷新状态：",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👥 绑定会员群", callback_data="mc:bindgroup"),
                 InlineKeyboardButton("🔗 邀请链接", callback_data="mc:setgroupurl")],
                [InlineKeyboardButton("📥 绑定主频道", callback_data="mc:bindmain"),
                 InlineKeyboardButton("📥 绑定备份频道", callback_data="mc:bindbackup")],
                [InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")],
            ]),
        )
    elif action == "setgroupurl":
        current = await get_member_group_url()
        pending_member_group_url.add(int(user.id))
        await message.edit_text(
            "🔗 会员群邀请链接\n\n"
            f"当前：{current or '未设置'}\n\n"
            "请直接发送邀请链接（以 http://、https:// 或 t.me/ 开头）。\n"
            "发送「清空」可移除；发送 /cancel 放弃本次设置。",
            reply_markup=back_keyboard(),
        )
    elif action in ("bindgroup", "bindmain", "bindbackup"):
        setting = {"bindgroup": "member_group_id", "bindmain": "storage_primary_channel_id", "bindbackup": "storage_backup_channel_id"}[action]
        pending_channel_bindings[user.id] = setting
        label = {"bindgroup": "会员群", "bindmain": "主存储私有频道", "bindbackup": "备用存储频道或群组"}[action]
        await message.edit_text(
            f"📨 请把{label}中的任意一条普通消息直接转发给机器人。\n\n"
            "会员群：推荐直接在会员群内发送 /bindgroup，无需转发。\n"
            "主存储：仅支持频道；备用存储：支持频道或群组。也可以直接发送 Chat ID（-100...）；机器人必须是目标管理员。",
            reply_markup=back_keyboard(),
        )
    elif action == "members":
        await message.edit_text(
            "👥 会员资格由指定会员群统一管理。\n\n"
            "本机器人只检测用户是否仍在已绑定的会员群内，不在这里手动开通、续期或取消会员。\n\n"
            "如需更换会员群，请使用「储存管理 → 绑定会员群」。",
            reply_markup=back_keyboard(),
        )
    elif action == "tagfolderscan":
        global directory_import_running
        if directory_import_running:
            await message.edit_text(
                "⏳ 当前已有扫描或目录导入任务正在运行，请完成后再试。",
                reply_markup=back_keyboard(),
            )
            return
        if scanner is None or not scanner.is_connected:
            await message.edit_text(
                "❌ 扫描账号尚未登录或连接已断开。请先完成扫描账号登录。",
                reply_markup=back_keyboard(),
            )
            return
        _, backup_channel_id = await get_storage_channels()
        if not backup_channel_id:
            await message.edit_text(
                "❌ 尚未绑定备用频道，请先绑定备用存储频道。",
                reply_markup=back_keyboard(),
            )
            return
        directory_import_running = True
        try:
            await message.edit_text(
                "⏳ 正在读取备用频道已备份的资源并识别相册标签……\n\n"
                "只会修改数据库中的目录归属，不会复制、删除或移动频道消息。"
            )
        except Exception:
            directory_import_running = False
            raise
        spawn_background(
            "backup-tag-scan",
            run_backup_tag_folder_scan(
                client, int(user.id), int(backup_channel_id), message
            ),
        )
        return
    elif action == "directories" or action.startswith("directories:"):
        categories = await db.list_categories()
        page = int(action.split(":", 1)[1]) if ":" in action else 0
        page = max(0, page)
        page_size = 10
        start_at = page * page_size
        selected = categories[start_at:start_at + page_size]
        rows = [
            [InlineKeyboardButton("➕ 新建目录", callback_data="mc:dirnew")],
            [InlineKeyboardButton("🏷 按标签建立文件夹（读取备用频道）", callback_data="mc:tagfolderscan")],
        ]
        for index, row in enumerate(selected, start_at):
            rows.append([InlineKeyboardButton(
                f"{index - start_at + 1}. 📁 {row['category'][:36]}（{int(row['file_count'] or 0)}）",
                callback_data=f"mc:directory:{index}",
            )])
        for chunk_start in range(0, len(selected), 5):
            rows.append([
                InlineKeyboardButton(
                    str(index + 1),
                    callback_data=f"mc:directory:{start_at + index}",
                )
                for index in range(chunk_start, min(chunk_start + 5, len(selected)))
            ])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"mc:directories:{page - 1}"))
        if start_at + page_size < len(categories):
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"mc:directories:{page + 1}"))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")])
        list_text = (
            f"📁 文件夹列表（共 {len(categories)} 个，第 {page + 1} 页）\n\n"
            "点击文件夹名称可复制名称；点击下方数字查看统计和管理。"
        )
        copy_rows = [
            [{"text": "➕ 新建目录", "callback_data": "mc:dirnew"}],
            [{"text": "🏷 按标签建立文件夹（读取备用频道）", "callback_data": "mc:tagfolderscan"}],
        ]
        for index, row in enumerate(selected):
            copy_rows.append([{
                "text": f"{index + 1}. 📁 {row['category'][:36]}（{int(row['file_count'] or 0)}）",
                "copy_text": {"text": str(row["category"])},
            }])
        for chunk_start in range(0, len(selected), 5):
            copy_rows.append([{
                "text": str(index + 1),
                "callback_data": f"mc:directory:{start_at + index}",
            } for index in range(chunk_start, min(chunk_start + 5, len(selected)))])
        admin_nav = []
        if page > 0:
            admin_nav.append({"text": "⬅️ 上一页", "callback_data": f"mc:directories:{page - 1}"})
        if start_at + page_size < len(categories):
            admin_nav.append({"text": "下一页 ➡️", "callback_data": f"mc:directories:{page + 1}"})
        if admin_nav:
            copy_rows.append(admin_nav)
        copy_rows.append([{"text": "⬅️ 返回管理面板", "callback_data": "mc:home"}])
        try:
            await telegram_bot_api("editMessageText", {
                "chat_id": message.chat.id,
                "message_id": message.id,
                "text": list_text,
                "reply_markup": {"inline_keyboard": copy_rows},
            })
        except Exception:
            log.exception("Could not render admin folder copy buttons; showing selectable folder names")
            fallback_names = "\n".join(
                f"{start_at + index + 1}. {row['category']}（{int(row['file_count'] or 0)}）"
                for index, row in enumerate(selected)
            )
            fallback_text = list_text + (
                "\n\n" + fallback_names + "\n\n可长按上方目录名称复制。"
                if fallback_names else ""
            )
            fallback_rows = rows[:1] + rows[1 + len(selected):]
            await message.edit_text(
                fallback_text,
                reply_markup=InlineKeyboardMarkup(fallback_rows),
            )
    elif action == "dirnew":
        pending_category_actions[user.id] = {"action": "create"}
        await message.edit_text(
            "➕ 新建目录\n\n请直接发送目录名称（最多 120 个字符）。\n"
            "创建后空目录会立即显示给会员。",
            reply_markup=back_keyboard(),
        )
    elif action.startswith("dirupload:"):
        try:
            index = int(action.split(":", 1)[1])
            row = (await db.list_categories())[index]
        except (ValueError, IndexError):
            await message.edit_text("⚠️ 目录列表已更新，请重新打开目录管理。", reply_markup=back_keyboard())
            return
        active_batch = await db.get_upload_batch(user.id)
        if active_batch and active_batch["category"] != str(row["category"]):
            await message.edit_text(
                f"⚠️ 当前正在上传目录：{active_batch['category']}。请先点击「✅ 结束上传」，发布本批文件后再切换目录。",
                reply_markup=back_keyboard(),
            )
            return
        pending_upload_categories[user.id] = str(row["category"])
        if not active_batch:
            # Persist the selected directory before any file arrives. This keeps
            # uploads in staged mode even if the bot restarts mid-session.
            await db.start_upload_batch(user.id, str(row["category"]))
            upload_received_counts[int(user.id)] = 0
        await message.edit_text(
            f"📤 已选择上传目录：{row['category']}\n\n"
            "现在直接向机器人发送图片、视频、音频或文件，都会保存到这个目录。\n"
            "可以连续上传多个文件。发送暂停后，机器人会在文件下方显示已接收数量，并提供内联「结束上传」或「继续上传」按钮。\n"
            "确认接收数量无误后再结束上传；上传期间会员看不到本批文件。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📁 选择其他目录", callback_data="mc:directories")],
                [InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")],
            ]),
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
        sizes = await db.get_category_storage_stats(row["category"])
        measured = int(sizes["sized_count"] or 0)
        buttons = [
            [InlineKeyboardButton("📂 查看内容", callback_data=f"mc:dircontent:{index}"),
             InlineKeyboardButton("📤 上传资源", callback_data=f"mc:dirupload:{index}")],
            [InlineKeyboardButton("✏️ 修改目录名称", callback_data=f"mc:dirrename:{index}")],
        ]
        if count == 0:
            buttons.append([InlineKeyboardButton("🗑 删除空目录", callback_data=f"mc:dirdelete:{index}")])
        buttons.append([InlineKeyboardButton("📁 文件夹列表", callback_data="mc:directories"),
                        InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")])
        detail = (
            f"📁 目录：{row['category']}\n"
            f"📦 资源总数：{count} 个\n"
            f"🎬 视频：{int(row['video_count'] or 0)} 个，约 {format_folder_size(sizes['video_size'])}\n"
            f"🖼 图片：{int(row['image_count'] or 0)} 张，约 {format_folder_size(sizes['image_size'])}\n"
            f"🎞 动图：{int(row['animation_count'] or 0)} 个\n"
            f"🎵 音频：{int(row['audio_count'] or 0)} 个\n"
            f"📄 文档及其他：{int(row['document_count'] or 0)} 个，约 {format_folder_size(sizes['other_size'])}\n"
            f"💾 已统计容量：{format_folder_size(row['total_size'])}\n"
            f"📏 大小已知：{measured}/{count} 个\n"
            f"🔗 分享链接：{folder_share_url(str(row['share_key'])) if row['share_key'] and BOT_USERNAME else '机器人启动后可用'}"
        )
        if measured < count:
            detail += "\n\n⚠️ 部分文件大小未知，容量仅为已知文件合计。"
        detail += "\n\n" + ("这是空目录，可以删除。" if count == 0 else "目录含有资源，只能修改名称，不能删除资源。")
        await message.edit_text(
            detail,
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True,
        )
    elif action.startswith("dircontent:"):
        try:
            parts = action.split(":")
            index = int(parts[1])
            offset = max(0, int(parts[2])) if len(parts) > 2 else 0
            row = (await db.list_categories())[index]
        except (ValueError, IndexError):
            await message.edit_text("⚠️ 目录列表已更新，请重新打开目录管理。", reply_markup=back_keyboard())
            return
        share_key = str(row["share_key"] or "")
        if not share_key:
            await message.reply_text("❌ 该目录暂时没有可用的领取标识。")
            return
        delivery_key = (int(user.id), share_key)
        if delivery_key in category_delivery_in_progress:
            await message.reply_text("⏳ 这个目录正在发送文件，请稍候。")
            return
        category_delivery_in_progress.add(delivery_key)
        try:
            # Count actual rows instead of trusting the cached folder-card summary.
            total = await db.count_category_files(row["category"])
            offset = min(offset, total)
            batch = await db.list_category_files(row["category"], limit=20, offset=offset)
            batch = await db.complete_album_groups(batch)
            if not batch:
                await message.reply_text(
                    f"📁「{row['category']}」当前实际资源：{total} 个。\n"
                    "这一页没有可发送的文件；重新打开目录后可从头查看。"
                )
                return
            progress = await message.reply_text(
                f"📥 正在发送「{row['category']}」资源（单文件每批20个，相册整组）…"
            )
            try:
                sent, failed, skipped, _already, next_offset, _last_sent_message_id = await send_catalog_batch(
                    client, user.id, f"admin:{share_key}", offset, batch,
                    track_progress=False,
                )
                result_text = (
                    f"📦「{row['category']}」本组发送完成\n\n"
                    f"成功：{sent} 个\n失败：{failed} 个\n"
                    f"已发送进度：{next_offset}/{total}"
                )
                buttons = []
                if next_offset < total:
                    buttons.append([InlineKeyboardButton(
                        "📥 继续发送下一组（单文件20个，相册整组）",
                        callback_data=f"mc:dircontent:{index}:{next_offset}",
                    )])
                # Remove the temporary status message, then send the completion
                # summary as a brand-new message after all media have been sent.
                buttons.append([InlineKeyboardButton("📁 返回目录", callback_data="mc:directories")])
                buttons.append([InlineKeyboardButton("🏠 返回主页", callback_data="mc:home")])
                try:
                    await progress.delete()
                except Exception:
                    log.warning("Could not delete admin delivery progress message")
                await client.send_message(
                    user.id,
                    result_text,
                    reply_markup=InlineKeyboardMarkup(buttons),
                )
            except Exception as exc:
                log.exception(
                    "Admin directory delivery failed: user=%s category=%s offset=%s",
                    user.id, row["category"], offset,
                )
                error_text = (
                    f"⚠️「{row['category']}」发送过程中出现异常。\n"
                    f"已处理进度：{offset}/{total}\n"
                    f"原因：{str(exc)[:180]}"
                )
                # A failed batch still gets its own final notification.
                # Remove the temporary status message instead of editing it.
                try:
                    await progress.delete()
                except Exception:
                    log.warning("Could not delete failed admin delivery progress message")
                try:
                    await client.send_message(
                        user.id,
                        error_text,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📁 返回目录", callback_data="mc:directories")],
                            [InlineKeyboardButton("🏠 返回主页", callback_data="mc:home")],
                        ]),
                    )
                except Exception:
                    log.exception(
                        "Could not send standalone admin delivery error summary: user=%s category=%s",
                        user.id, row["category"],
                    )
                    try:
                        await message.reply_text(error_text)
                    except Exception:
                        log.exception("All admin delivery error notification methods failed")
        finally:
            category_delivery_in_progress.discard(delivery_key)
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
    elif action == "admins":
        if not await is_superadmin(user.id):
            await message.edit_text("⛔ 只有超级管理员可以管理管理员名单。", reply_markup=back_keyboard())
            return
        delegated = await db.list_admins()
        superadmin_id = await db.get_admin_id()
        lines = [
            f"👑 超级管理员：{int(config.ADMIN_ID)}"
        ]
        if superadmin_id and int(superadmin_id) != int(config.ADMIN_ID):
            lines.append(f"👑 数据库超级管理员：{int(superadmin_id)}")
        rows = [[InlineKeyboardButton("➕ 添加管理员", callback_data="mc:adminadd"),
                 InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")]]
        if delegated:
            lines.append("")
            lines.append("🛡 其他管理员：")
            for item in delegated:
                admin_id = int(item["user_id"])
                lines.append(f"• {admin_id}")
                if rows and len(rows[-1]) == 1 and rows[-1][0].callback_data == "mc:adminadd":
                    rows[-1].append(InlineKeyboardButton(
                        f"❌ 移除 {admin_id}",
                        callback_data=f"mc:adminremove:{admin_id}",
                    ))
                else:
                    rows.append([InlineKeyboardButton(
                        f"❌ 移除 {admin_id}",
                        callback_data=f"mc:adminremove:{admin_id}",
                    )])
        else:
            lines.extend(["", "目前还没有添加其他管理员。"])
        if not any(button.callback_data == "mc:home" for row in rows for button in row):
            rows.append([InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")])
        await message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))
    elif action == "adminadd":
        if not await is_superadmin(user.id):
            await message.edit_text("⛔ 只有超级管理员可以添加管理员。", reply_markup=back_keyboard())
            return
        pending_admin_actions[user.id] = {"action": "add"}
        await message.edit_text(
            "➕ 添加管理员\n\n请发送对方的 Telegram 数字用户 ID。\n"
            "只接受数字 ID，不要发送用户名。\n\n发送 /cancel 可取消。",
            reply_markup=back_keyboard(),
        )
    elif action.startswith("adminremove:"):
        if not await is_superadmin(user.id):
            await message.edit_text("⛔ 只有超级管理员可以移除管理员。", reply_markup=back_keyboard())
            return
        try:
            target_id = int(action.split(":", 1)[1])
        except ValueError:
            await message.edit_text("❌ 管理员 ID 无效。", reply_markup=back_keyboard())
            return
        superadmin_id = await db.get_admin_id()
        if target_id in (int(config.ADMIN_ID), int(superadmin_id or 0)):
            await message.edit_text("❌ 不能移除超级管理员。", reply_markup=back_keyboard())
            return
        removed = await db.remove_admin(target_id)
        await message.edit_text(
            ("✅ 已移除管理员：" if removed else "ℹ️ 这个用户不在管理员名单中：")
            + str(target_id),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ 返回管理员列表", callback_data="mc:admins")],
                [InlineKeyboardButton("⬅️ 返回管理面板", callback_data="mc:home")],
            ]),
        )
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
    elif action == "upload":
        categories = await db.list_categories()
        rows = []
        for index, row in enumerate(categories[:20]):
            button = InlineKeyboardButton(
                f"📁 {str(row['category'])[:20]}（{int(row['file_count'] or 0)}）",
                callback_data=f"mc:dirupload:{index}",
            )
            if rows and len(rows[-1]) == 1 and rows[-1][0].callback_data.startswith("mc:dirupload:"):
                rows[-1].append(button)
            else:
                rows.append([button])
        if len(categories) > 20:
            rows.append([InlineKeyboardButton("📂 全部文件夹", callback_data="mc:directories")])
        if not categories:
            rows.append([InlineKeyboardButton("➕ 新建文件夹", callback_data="mc:dirnew")])
        rows.append([InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")])
        await message.edit_text(
            f"📤 上传资源\n\n当前共有 {len(categories)} 个文件夹。请选择上传位置；"
            "选择后直接发送文件或相册，结束上传前资源不会发布到目录。",
            reply_markup=InlineKeyboardMarkup(rows),
        )
    elif action == "files":
        rows = await db.list_files(15)
        lines = [f"{row['file_key']} | {row['category']} | {row['file_name'][:40]}" for row in rows]
        body = "📂 最近文件（最多 15 个）\n" + ("\n".join(lines) if lines else "暂无文件记录") + "\n\n修改分类：/category 文件码 分类\n批量分享：/share 文件码1 文件码2"
        await message.edit_text(body, reply_markup=back_keyboard())
    elif action == "broadcast":
        broadcast_input_mode.add(int(user.id))
        pending_broadcasts.pop(int(user.id), None)
        await message.edit_text(
            "📢 广播管理\n\n"
            "请直接发送一条要广播的消息：文字、图片、视频、文件、音频、GIF 或相册均可。\n"
            "文字格式和媒体说明会尽量按原消息保留。\n\n"
            "收到后先显示预览，只有点击“确认群发”才会发送；发送失败不会删除用户记录。\n"
            "发送 /cancel 可退出。",
            reply_markup=back_keyboard(),
        )
    elif action == "stats":
        users, files, _, banned = await db.stats()
        activity = await db.activity_stats()
        reach = await db.reach_stats()
        await message.edit_text(
            "📊 数据统计\n"
            "━━━━━━━━━━━━━━\n"
            "🟢 当前活跃（不是累计注册数）\n"
            f"今日 24 小时：{activity['day']} 人\n"
            f"最近 7 天：{activity['week']} 人\n"
            f"最近 30 天：{activity['month']} 人\n"
            f"今日新注册：{activity['new_today']} 人\n\n"
            "📡 可达性（能不能收到机器人消息）\n"
            f"✅ 可达 {reach['ok']} ｜🚫 已屏蔽 {reach['blocked']}"
            f" ｜💀 注销 {reach['deactivated']}\n"
            f"⏳ 尚未检测 {reach['unchecked']} 人\n\n"
            f"👤 注册总数（累计）：{users}\n"
            f"📦 资源总数：{files}\n"
            f"🚫 封禁用户：{banned}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📡 检测用户能否接收", callback_data="mc:reachcheck")],
                [InlineKeyboardButton("🏠 返回管理首页", callback_data="mc:home")],
            ]),
        )
    elif action == "reachcheck":
        running = reach_check_state["task"]
        if running is not None and not running.done():
            await message.edit_text(
                "⏳ 正在检测中，请等这一轮结束再重新点。\n"
                "检测期间机器人可正常使用，不会给任何用户发消息。",
                reply_markup=back_keyboard(),
            )
            return
        await message.edit_text(
            "📡 开始检测用户可达性\n"
            "━━━━━━━━━━━━━━\n"
            "方式：给对方发一个「正在输入」状态，能发出去就是可达。\n"
            "对方不会收到任何消息，最多看到一瞬间的输入提示。\n\n"
            "检测期间领取、上传、备份都正常，进度会自动刷新。",
        )
        reach_check_state["task"] = spawn_background(
            "reach-check", run_reach_check(message)
        )
    elif action == "scannerlogin":
        await message.edit_text(
            "🔐 扫描账号设置\n\n"
            "使用普通 Telegram 用户账号读取主、备份频道历史。建议使用专用账号，并先将它加入两个私有频道。\n\n"
            "点击下方按钮后，机器人会引导你输入手机号、登录验证码及（如有）两步验证密码。敏感输入会尝试立即删除。\n"
            "扫描进度和扫描账号登录会话均保存在 PostgreSQL 数据库中。重新部署后会自动恢复，无需重复登录。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔑 开始登录", callback_data="mc:scannerloginstart"),
                 InlineKeyboardButton("🏠 管理首页", callback_data="mc:home")],
            ]),
        )
    elif action == "scannerloginstart":
        try:
            await _start_scanner_qr_login(message)
        except Exception:
            log.exception("Could not start scanner QR login from admin panel")
            await message.edit_text("❌ 无法启动登录流程，请稍后重试。", reply_markup=back_keyboard())
    elif action == "commands":
        await message.edit_text(
            "📖 管理命令速查\n\n管理员名单：打开 /admin → 管理员管理（仅超级管理员）\n\n"
            "/bindgroup — 绑定会员群\n/membercheck [用户ID] — 检查会员群和成员权限\n/bindmain [Chat ID] — 绑定主存储频道\n/bindbackup [Chat ID] — 绑定备用频道或群组\n/storage — 查看绑定状态\n"
            "/ban 用户ID 原因 — 封禁用户\n/unban 用户ID — 解封用户\n/users — 用户列表\n/files — 文件列表\n/import — 批量转发导入旧文件\n/scanlogin — 在机器人内登录扫描账号\n/scancancel — 取消扫描账号登录\n/scanlogout — 退出/更换扫描账号\n/scancheck — 检查频道访问和历史读取权限\n/catalogreset — 预览清理旧资源索引\n/catalogresetconfirm — 确认清理资源索引\n/scan — 扫描频道历史\n/scanstatus — 查看扫描进度\n/scanstop — 暂停扫描\n/category 文件码 分类 — 修改分类\n/share 文件码1 文件码2 — 批量分享\n/wm 领取编号 — 按编号查领取人\n/welcome — 新人进群欢迎设置\n"
            "广播请打开 /admin → 群发广播，直接发送内容并确认。",
            reply_markup=back_keyboard(),
        )


@app.on_message(filters.private & filters.command("welcome"))
async def welcome_admin_handler(client, message):
    user = message.from_user
    if not user or not await is_admin(user.id):
        await reply_admin_only(message)
        return

    pending_welcome_edit.discard(int(user.id))
    enabled, template = await get_welcome_settings()
    parts = message.command[1:] if len(message.command) > 1 else []
    bot_url = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else ""

    if not parts:
        status = "开启" if enabled else "关闭"
        await message.reply_text(
            "👋 新人进群欢迎设置\n\n"
            f"当前状态：{status}\n"
            f"当前文案：\n{template}\n\n"
            "可用命令：\n"
            "/welcome on — 开启自动欢迎\n"
            "/welcome off — 关闭自动欢迎\n"
            "/welcome 文案 — 自定义文案（{name} 替换为新人称呼，{bot} 替换为机器人入口）\n"
            "/welcome reset — 恢复默认文案",
            reply_markup=back_keyboard(),
        )
        return

    action = parts[0].lower()
    if action in ("on", "开", "开启", "1"):
        await db.set_setting(WELCOME_ENABLED_KEY, "1")
        await message.reply_text("✅ 已开启新人进群自动欢迎。", reply_markup=back_keyboard())
        return
    if action in ("off", "关", "关闭", "0"):
        await db.set_setting(WELCOME_ENABLED_KEY, "0")
        await message.reply_text("✅ 已关闭新人进群自动欢迎。", reply_markup=back_keyboard())
        return
    if action == "reset":
        await db.set_setting(WELCOME_MESSAGE_KEY, "")
        await message.reply_text("✅ 已恢复默认欢迎文案。", reply_markup=back_keyboard())
        return

    custom = " ".join(parts).strip()
    if not custom:
        await message.reply_text("❌ 文案不能为空。", reply_markup=back_keyboard())
        return

    await db.set_setting(WELCOME_MESSAGE_KEY, custom)
    preview = render_welcome_message(custom, ["新成员"], bot_url)
    await message.reply_text(
        f"✅ 欢迎文案已保存。\n\n预览：\n{preview}",
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
            dt = parse_cn_datetime(expiry)
            if dt is None:
                label = "日期异常"
            else:
                # Display only: Beijing time for the admin, expiry check untouched.
                label = "已过期" if dt <= now else dt.strftime("%Y-%m-%d %H:%M")
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
        category_catalog_cache["expires_at"] = 0
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
    await message.reply_text(
        "📢 旧版命令仅支持文字。请打开 /admin → 群发广播，"
        "直接发送文字或媒体，预览后确认发送。"
    )


async def main():
    global BOT_USERNAME, BOT_ID, telegram_http_session, scanner, last_user_activity, backup_worker_running
    scanner_started = False
    app_started = False
    runner = None
    backup_retry_task = None
    scanner_caption_task = None
    retention_task = None
    try:
        # Keep database initialization inside the cleanup scope. A migration
        # error must not leave AsyncConnectionPool worker tasks behind.
        boot_t0 = time.monotonic()
        await db.initialize()
        log.info(
            "Database backend: %s (%.1fs)",
            "PostgreSQL" if db.uses_postgres else "SQLite (local fallback)",
            time.monotonic() - boot_t0,
        )

        # Restore the scanner from a session string. An environment variable
        # takes precedence; otherwise fall back to the database-backed setting.
        stored_scanner_session = os.getenv("SCANNER_SESSION_STRING", "").strip()
        if not stored_scanner_session:
            stored_scanner_session = await db.get_setting(SCANNER_SESSION_SETTING_KEY)
        if stored_scanner_session:
            scanner = Client(
                "member_cloud_scanner",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=stored_scanner_session,
            )
            log.info("Persistent scanner session loaded from database")
        elif legacy_scanner_session_is_authorized(SCANNER_SESSION_PATH):
            # One-time migration for deployments that still have the old
            # file-based Pyrogram session available.
            legacy_scanner = Client(
                "member_cloud_scanner",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                workdir=SCANNER_WORKDIR,
            )
            try:
                await legacy_scanner.start()
                legacy_me = await legacy_scanner.get_me()
                if legacy_me.is_bot:
                    raise RuntimeError("History scanner must be a normal Telegram user account")
                migrated_session = await legacy_scanner.export_session_string()
                await db.set_setting(SCANNER_SESSION_SETTING_KEY, migrated_session)
                await legacy_scanner.stop()
                scanner = Client(
                    "member_cloud_scanner",
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    session_string=migrated_session,
                )
                log.info("Legacy file-based scanner session migrated to database")
            except Exception:
                log.exception("Could not migrate legacy scanner session")
                try:
                    if legacy_scanner.is_connected:
                        await legacy_scanner.stop()
                except Exception:
                    log.exception("Could not stop legacy scanner during migration")
                scanner = None
        else:
            scanner = None
            if os.path.exists(SCANNER_SESSION_PATH):
                log.warning(
                    "Legacy scanner session file exists but is not an authorized session; "
                    "skipping interactive migration. Admin can use /scanlogin."
                )
            else:
                log.warning("No persistent scanner session found; admin must use /scanlogin")

        # Cold-start handshake: a first-hit 500/network blip must not exit the
        # process and trigger a platform restart loop. Retry a bounded number
        # of times with a short backoff before giving up.
        handshake_attempts = 3
        last_start_error = None
        for attempt in range(1, handshake_attempts + 1):
            attempt_t0 = time.monotonic()
            try:
                await app.start()
                last_start_error = None
                break
            except Exception as start_exc:
                last_start_error = start_exc
                log.warning(
                    "Telegram handshake attempt %s/%s failed (%.1fs): %s",
                    attempt, handshake_attempts,
                    time.monotonic() - attempt_t0, type(start_exc).__name__,
                )
                if app.is_connected:
                    try:
                        await app.disconnect()
                    except Exception:
                        log.exception("Could not disconnect after failed handshake")
                if attempt < handshake_attempts:
                    await asyncio.sleep(3)
        if last_start_error is not None:
            raise last_start_error
        app_started = True
        log.info("Telegram handshake completed (%.1fs)", time.monotonic() - attempt_t0)
        # Publish the Telegram chat command menu for all users.
        # /admin remains protected by the existing is_admin() check.
        await app.set_bot_commands([
            BotCommand("start", "开始使用机器人"),
            BotCommand("admin", "打开管理后台"),
        ])
        telegram_http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=12, connect=5, sock_read=10)
        )
        await resume_pending_deletions(app)

        async def _connect_scanner():
            global scanner
            nonlocal scanner_started
            if scanner is None:
                return
            try:
                await scanner.start()
                scanner_started = True
                scanner_me = await scanner.get_me()
                if scanner_me.is_bot:
                    raise RuntimeError("History scanner must be a normal Telegram user account")
                log.info("History scanner ready: user %s", scanner_me.id)
            except Exception:
                log.exception(
                    "History scanner is unavailable; bot will continue without history scanning"
                )
                if scanner_started:
                    try:
                        if scanner.is_connected:
                            await scanner.stop()
                    except Exception:
                        log.exception("Could not stop failed history scanner session")
                    scanner_started = False
                scanner = None

        # Channel-permission self-check and scanner login are not required
        # before the bot can answer messages. Run them in the background so
        # the first /start is not blocked on extra Telegram round trips.
        spawn_background("startup-self-check", startup_self_check(app))
        spawn_background("scanner-connect", _connect_scanner())

        last_user_activity = asyncio.get_running_loop().time()
        me = await app.get_me()
        BOT_USERNAME = me.username
        BOT_ID = me.id
        log.info("Bot started: @%s", me.username)

        backup_retry_task = spawn_background("backup-worker", backup_retry_worker(app))
        backup_worker_running = True
        log.info("Durable backup retry worker started")
        scanner_caption_task = spawn_background("caption-worker", scanner_caption_worker())
        log.info("Durable scanner caption worker started")
        retention_task = spawn_background("settings-retention", settings_retention_worker())
        log.info("Settings retention worker started")

        runner = web.AppRunner(create_catalog_app(db, lambda: BOT_USERNAME))
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", config.WEB_PORT)
        await site.start()
        log.info("Public catalog listening on port %s", config.WEB_PORT)

        await asyncio.Event().wait()
    finally:
        # Each resource is closed independently so one cleanup failure cannot
        # prevent the PostgreSQL pool from being closed.
        backup_worker_running = False
        if scanner_caption_task is not None:
            scanner_caption_task.cancel()
            try:
                await scanner_caption_task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Scanner caption worker stopped with an error")
        if backup_retry_task is not None:
            backup_retry_task.cancel()
            try:
                await backup_retry_task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Backup retry worker stopped with an error")

        if retention_task is not None:
            retention_task.cancel()
            try:
                await retention_task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Settings retention worker stopped with an error")

        if runner is not None:
            try:
                await runner.cleanup()
            except Exception:
                log.exception("Could not clean up catalog web server")

        if scanner_started and scanner is not None:
            try:
                if scanner.is_connected:
                    await scanner.stop()
            except Exception:
                log.exception("Could not stop history scanner")

        if app_started:
            try:
                await app.stop()
            except Exception:
                log.exception("Could not stop Telegram bot client")

        if telegram_http_session is not None and not telegram_http_session.closed:
            try:
                await telegram_http_session.close()
            except Exception:
                log.exception("Could not close Telegram HTTP session")
            finally:
                telegram_http_session = None

        try:
            await db.close()
        except Exception:
            log.exception("Could not close database connection pool")


if __name__ == "__main__":
    app.run(main())
