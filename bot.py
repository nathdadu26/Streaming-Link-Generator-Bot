"""
R2 Renamer + MEGA Downloader + Telegram Channel Poster
Koyeb Deployment — MongoDB progress, live message edits
"""

import os
import re
import json
import time
import uuid
import random
import logging
import asyncio
import threading
import requests
from datetime import datetime, timezone, date
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from pymongo import MongoClient
from telegram import Update, Message
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from health_check import run_health_server
import mega_downloader

# ─────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────

load_dotenv(dotenv_path=Path(__file__).parent / ".env")


def require_env(key):
    val = os.getenv(key, "").strip()
    if not val:
        raise SystemExit(f"ERROR: '{key}' not set in environment")
    return val


def optional_env(key, default=""):
    return os.getenv(key, default).strip() or default


# ─────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────

CF_ACCOUNT_ID  = require_env("CF_ACCOUNT_ID")
R2_ACCESS_KEY  = require_env("R2_ACCESS_KEY")
R2_SECRET_KEY  = require_env("R2_SECRET_KEY")
R2_BUCKET_NAME = require_env("R2_BUCKET_NAME")
D1_DATABASE_ID = require_env("D1_DATABASE_ID")
CF_API_TOKEN   = require_env("CF_API_TOKEN")

BOT_TOKEN      = require_env("TELEGRAM_BOT_TOKEN")
CHANNEL_ID     = require_env("TELEGRAM_CHANNEL_ID")
POST_URL       = require_env("POST_URL").rstrip("/")
ADMIN_IDS      = {int(x.strip()) for x in require_env("ADMIN_TELEGRAM_ID").split(",") if x.strip()}

MONGO_URI      = require_env("MONGO_URI")
MONGO_DB       = optional_env("MONGO_DB",  "bot_db")
MONGO_COL      = optional_env("MONGO_COL", "rename_progress")

TARGET_FOLDER  = optional_env("TARGET_FOLDER", "new-videos/")
POST_INTERVAL  = int(optional_env("POST_INTERVAL_SECONDS", "350"))  # 350s ≈ 247 posts/day

if not TARGET_FOLDER.endswith("/"):
    TARGET_FOLDER += "/"

VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".wmv",
    ".flv", ".webm", ".m4v", ".3gp", ".ts",
    ".mts", ".m2ts", ".mpeg", ".mpg", ".vob"
}

# ─────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────
# MONGODB — rename progress
# ─────────────────────────────────────────────────────

_mongo = MongoClient(MONGO_URI)
_col   = _mongo[MONGO_DB][MONGO_COL]


def mongo_get_processed(folder_key: str) -> set:
    doc = _col.find_one({"_id": folder_key}, {"done": 1})
    return set(doc.get("done", [])) if doc else set()


def mongo_mark_done(folder_key: str, old_key: str):
    _col.update_one(
        {"_id": folder_key},
        {"$addToSet": {"done": old_key}},
        upsert=True,
    )


def mongo_clear(folder_key: str):
    _col.delete_one({"_id": folder_key})

# ─────────────────────────────────────────────────────
# R2 CLIENT
# ─────────────────────────────────────────────────────

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

# ─────────────────────────────────────────────────────
# CLOUDFLARE D1
# ─────────────────────────────────────────────────────

D1_URL = (
    f"https://api.cloudflare.com/client/v4"
    f"/accounts/{CF_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}/query"
)
D1_HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type":  "application/json",
}


def d1_query(sql, params=None):
    payload = {"sql": sql}
    if params:
        payload["params"] = params
    resp = requests.post(D1_URL, headers=D1_HEADERS, json=payload, timeout=15)
    data = resp.json()
    if not data.get("success"):
        raise Exception(f"D1 error: {data.get('errors')}")
    return data["result"][0].get("results", [])


def d1_insert(video_id, slug, filename, file_size, r2_key, created_at):
    sql = (
        "INSERT OR IGNORE INTO videos "
        "(id, slug, filename, file_size, r2_key, created_at, posted, posted_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, NULL)"
    )
    try:
        resp = requests.post(
            D1_URL, headers=D1_HEADERS,
            json={"sql": sql, "params": [video_id, slug, filename, file_size, r2_key, created_at]},
            timeout=15,
        )
        data = resp.json()
        if not data.get("success"):
            logger.error(f"D1 INSERT FAILED: {data.get('errors', [])}")
            return False
        return True
    except Exception as e:
        logger.error(f"D1 REQUEST ERROR: {e}")
        return False


def d1_ensure_columns():
    for sql in [
        "ALTER TABLE videos ADD COLUMN posted    INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE videos ADD COLUMN posted_at TEXT",
    ]:
        try:
            d1_query(sql)
        except Exception:
            pass


def d1_get_next_unposted():
    rows = d1_query(
        "SELECT id, slug FROM videos WHERE posted=0 ORDER BY created_at ASC LIMIT 1"
    )
    return rows[0] if rows else None


def d1_mark_posted(video_id):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    d1_query("UPDATE videos SET posted=1, posted_at=? WHERE id=?", [now, video_id])


def d1_stats():
    rows = d1_query(
        "SELECT COUNT(*) AS total,"
        " SUM(CASE WHEN posted=1 THEN 1 ELSE 0 END) AS done,"
        " SUM(CASE WHEN posted=0 THEN 1 ELSE 0 END) AS pending"
        " FROM videos"
    )
    return rows[0] if rows else {"total": 0, "done": 0, "pending": 0}


def d1_today_posted() -> int:
    today = date.today().strftime("%Y-%m-%d")
    rows  = d1_query(
        "SELECT COUNT(*) AS cnt FROM videos WHERE posted=1 AND posted_at LIKE ?",
        [f"{today}%"]
    )
    return rows[0].get("cnt", 0) if rows else 0

# ─────────────────────────────────────────────────────
# R2 HELPERS
# ─────────────────────────────────────────────────────

def get_r2_files(prefix):
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=R2_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):
                files.append({"key": obj["Key"], "size": obj.get("Size", 0)})
    return files


def get_file_size(key):
    try:
        return s3.head_object(Bucket=R2_BUCKET_NAME, Key=key).get("ContentLength", 0)
    except Exception:
        return 0

# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────

def make_slug():
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8))


def generate_name(ext):
    return f"[TG - @atoz_links]VID_{datetime.now().strftime('%H%M%S%d%m%Y%f')}{ext}"


def progress_bar(pct: int, width: int = 10) -> str:
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled)

# ─────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────

bot_state = {
    "running"       : False,
    "photo_file_id" : None,
    "posted_today"  : 0,
    "last_post_time": None,
    "failed_count"  : 0,
}

rename_state = {"active": False, "cancel": False, "thread": None}
mega_state   = {"active": False, "thread": None}

# ─────────────────────────────────────────────────────
# ADMIN CHECK
# ─────────────────────────────────────────────────────

def is_admin(update: Update) -> bool:
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        logger.warning(f"Unauthorized: user_id={uid}")
        return False
    return True

# ─────────────────────────────────────────────────────
# LIVE MESSAGE EDITOR — thread-safe
# One message is sent, then edited in-place every update.
# ─────────────────────────────────────────────────────

class LiveMessage:
    """
    Sends one message, then edits it in-place.
    Thread-safe: background threads call update_sync().
    """
    def __init__(self, loop, bot, chat_id, initial_text):
        self._loop    = loop
        self._bot     = bot
        self._chat_id = chat_id
        self._msg_id  = None
        self._last    = ""
        self._lock    = threading.Lock()

        # Send initial message (from main async context)
        fut = asyncio.run_coroutine_threadsafe(
            self._send(initial_text), loop
        )
        fut.result(timeout=10)

    async def _send(self, text):
        msg = await self._bot.send_message(
            chat_id=self._chat_id, text=text, parse_mode="Markdown"
        )
        self._msg_id = msg.message_id
        self._last   = text

    def update_sync(self, text: str):
        """Call from background thread — edits the live message."""
        if text == self._last:
            return
        with self._lock:
            self._last = text
        asyncio.run_coroutine_threadsafe(self._edit(text), self._loop)

    async def _edit(self, text: str):
        try:
            await self._bot.edit_message_text(
                chat_id    = self._chat_id,
                message_id = self._msg_id,
                text       = text,
                parse_mode = "Markdown",
            )
        except Exception as e:
            logger.warning(f"LiveMessage edit failed: {e}")

    def send_final_sync(self, text: str):
        """Send a NEW message for final result (so it doesn't disappear on scroll)."""
        asyncio.run_coroutine_threadsafe(
            self._bot.send_message(
                chat_id=self._chat_id, text=text, parse_mode="Markdown"
            ),
            self._loop,
        )

# ─────────────────────────────────────────────────────
# RENAMER WORKER
# ─────────────────────────────────────────────────────

def rename_worker(folder: str, live: LiveMessage):
    folder_key = f"rename:{folder}"
    processed  = mongo_get_processed(folder_key)

    try:
        live.update_sync(
            f"🔍 *Renaming & Moving*\n"
            f"`{folder}` → `{TARGET_FOLDER}`\n\n"
            f"Scanning folder..."
        )

        all_files   = get_r2_files(folder)
        video_files = [
            f for f in all_files
            if os.path.splitext(f["key"])[1].lower() in VIDEO_EXTENSIONS
        ]
        total_v     = len(video_files)
        skipped_img = len(all_files) - total_v

        if not video_files:
            live.update_sync(
                f"🔍 *Renaming & Moving*\n"
                f"`{folder}` → `{TARGET_FOLDER}`\n\n"
                f"⚠️ Koi video file nahi mili."
            )
            return

        success = failed = duplicate = 0
        last_milestone = -1

        for index, file_info in enumerate(video_files, start=1):

            if rename_state["cancel"]:
                pct = int((index / total_v) * 100)
                live.send_final_sync(
                    f"🛑 *Rename Cancelled*\n\n"
                    f"Done      : {success}\n"
                    f"Failed    : {failed}\n"
                    f"Remaining : {total_v - index + 1}"
                )
                return

            old_key   = file_info["key"]
            file_size = file_info["size"] or get_file_size(old_key)

            if old_key in processed:
                duplicate += 1
                continue

            try:
                ext          = os.path.splitext(old_key)[1].lower()
                new_filename = generate_name(ext)
                new_key      = TARGET_FOLDER + new_filename

                s3.copy_object(
                    Bucket=R2_BUCKET_NAME,
                    CopySource={"Bucket": R2_BUCKET_NAME, "Key": old_key},
                    Key=new_key,
                )
                s3.delete_object(Bucket=R2_BUCKET_NAME, Key=old_key)

                video_id   = str(uuid.uuid4())
                slug       = make_slug()
                created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                d1_insert(video_id, slug, new_filename, file_size, new_key, created_at)

                mongo_mark_done(folder_key, old_key)
                processed.add(old_key)
                success += 1

            except Exception as e:
                failed += 1
                logger.error(f"Rename error [{old_key}]: {e}")
                time.sleep(1)

            # Live progress update (har iteration — but edit throttle handles it)
            pct       = int((index / total_v) * 100)
            bar       = progress_bar(pct)
            milestone = (pct // 25) * 25

            if milestone > last_milestone:
                last_milestone = milestone
                live.update_sync(
                    f"🔄 *Renaming & Moving*\n"
                    f"`{folder}` → `{TARGET_FOLDER}`\n\n"
                    f"`{bar}` {pct}%\n"
                    f"[{index}/{total_v}]\n\n"
                    f"✅ Done   : {success}\n"
                    f"❌ Failed : {failed}\n"
                    f"⏭ Skip   : {duplicate}"
                )

        live.send_final_sync(
            f"✅ *Rename Complete!*\n\n"
            f"`{folder}` → `{TARGET_FOLDER}`\n\n"
            f"Success   : {success}\n"
            f"Failed    : {failed}\n"
            f"Duplicates: {duplicate}\n"
            f"Img skip  : {skipped_img}"
        )

    except Exception as e:
        logger.error(f"Rename worker fatal: {e}")
        live.send_final_sync(f"💥 *Rename Fatal Error*\n\n{e}")

    finally:
        rename_state["active"] = False
        rename_state["cancel"] = False

# ─────────────────────────────────────────────────────
# MEGA WORKER
# ─────────────────────────────────────────────────────

def mega_worker_fn(mega_link: str, live: LiveMessage):
    try:
        mega_downloader.run_mega_download(mega_link, live)
    except Exception as e:
        logger.error(f"MEGA worker fatal: {e}")
        live.send_final_sync(f"💥 *MEGA Fatal Error*\n\n{e}")
    finally:
        mega_state["active"] = False

# ─────────────────────────────────────────────────────
# POSTING JOB
# ─────────────────────────────────────────────────────

async def posting_job(context: ContextTypes.DEFAULT_TYPE):
    if not bot_state["running"] or not bot_state["photo_file_id"]:
        return

    try:
        video = d1_get_next_unposted()
    except Exception as e:
        logger.error(f"D1 fetch error: {e}")
        return

    if not video:
        bot_state["running"] = False
        for job in context.job_queue.get_jobs_by_name("poster"):
            job.schedule_removal()
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text="✅ Saare videos post ho gaye! Posting band ho gayi.",
                )
            except Exception as e:
                logger.error(f"Notify admin {admin_id} failed: {e}")
        return

    url = f"{POST_URL}/{video['slug']}"
    try:
        await context.bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=bot_state["photo_file_id"],
            caption=url,
        )
        d1_mark_posted(video["id"])
        bot_state["posted_today"]   += 1
        bot_state["last_post_time"]  = datetime.now().strftime("%H:%M:%S")
        logger.info(f"Posted: {url}")
    except Exception as e:
        bot_state["failed_count"] += 1
        logger.error(f"Post failed: {e}")

# ─────────────────────────────────────────────────────
# COMMANDS
# ─────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    # Always show full menu on /start
    await update.message.reply_text(
        "👋 *Welcome! Yeh hai tumhara bot.*\n\n"

        "━━━━━━━━━━━━━━━━━━━━\n"
        "📤 *Channel Posting*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/start\\_posting — Channel pe posting shuru karo\n"
        "_(Pehle /image se thumbnail set karo)_\n\n"
        "/cancel\\_posting — Posting band karo\n\n"
        "/image — Naya thumbnail image set karo\n"
        "_(Jo image bhejoge woh har post mein use hogi)_\n\n"

        "━━━━━━━━━━━━━━━━━━━━\n"
        "🔄 *R2 Renamer*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/rename `<folder>` — R2 folder ki videos rename karke\n"
        f"`{TARGET_FOLDER}` mein move karo + D1 mein save karo\n\n"
        "/cancel\\_renaming — Rename rok do\n\n"

        "━━━━━━━━━━━━━━━━━━━━\n"
        "⬇️ *MEGA Downloader*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/mega `<link>` — MEGA folder/file download karke R2 pe upload karo\n"
        "_(3GB daily limit hit hone pe 6 ghante baad auto-retry hoga)_\n\n"
        "/cancel\\_mega — MEGA download rok do\n\n"

        "━━━━━━━━━━━━━━━━━━━━\n"
        "📊 *Info*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/status — Sab processes ka live status dekho\n"
        "/start — Yeh menu dobara dekho",

        parse_mode="Markdown",
    )

    # Agar image set hai to posting bhi start karo
    if not bot_state["photo_file_id"]:
        await update.message.reply_text(
            "📸 *Posting start karne ke liye pehle thumbnail bhejo.*\n"
            "Ek photo send karo is chat mein.",
            parse_mode="Markdown",
        )
        return

    if bot_state["running"]:
        await update.message.reply_text("⚡ Posting pehle se chal rahi hai.")
        return


async def cmd_start_posting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if bot_state["running"]:
        await update.message.reply_text("⚡ Posting pehle se chal rahi hai!")
        return

    if not bot_state["photo_file_id"]:
        await update.message.reply_text(
            "📸 Pehle thumbnail bhejo, phir /start\\_posting karo.",
            parse_mode="Markdown",
        )
        return

    bot_state["running"]      = True
    bot_state["posted_today"] = d1_today_posted()

    for job in context.job_queue.get_jobs_by_name("poster"):
        job.schedule_removal()

    context.job_queue.run_repeating(posting_job, interval=POST_INTERVAL, first=1, name="poster")

    stats = d1_stats()
    await update.message.reply_text(
        f"▶️ *Posting Shuru!*\n\n"
        f"📢 Channel  : `{CHANNEL_ID}`\n"
        f"⏱ Interval : {POST_INTERVAL}s (~{86400 // POST_INTERVAL} posts/day)\n"
        f"📦 Pending  : {stats.get('pending', 0)} videos",
        parse_mode="Markdown",
    )


async def cmd_cancel_posting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if not bot_state["running"]:
        await update.message.reply_text("⏹ Posting pehle se band hai.")
        return

    bot_state["running"] = False
    for job in context.job_queue.get_jobs_by_name("poster"):
        job.schedule_removal()

    await update.message.reply_text(
        f"⏹ *Posting Band*\n\nAaj post hue: {bot_state['posted_today']}",
        parse_mode="Markdown",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    try:
        stats = d1_stats()
    except Exception as e:
        await update.message.reply_text(f"D1 error: {e}")
        return

    today_cnt  = bot_state["posted_today"]
    total_slug = stats.get("total", 0)
    pending    = stats.get("pending", 0)
    done       = stats.get("done",    0)

    post_bar   = progress_bar(int(done / total_slug * 100) if total_slug else 0)

    posting_st = "▶️ Chal rahi hai" if bot_state["running"]   else "⏹ Band"
    rename_st  = "🔄 Chal rahi hai" if rename_state["active"] else "💤 Idle"
    mega_st    = "⬇️ Chal rahi hai" if mega_state["active"]   else "💤 Idle"
    img_st     = "✅ Set"            if bot_state["photo_file_id"] else "❌ Set nahi"

    await update.message.reply_text(
        f"📊 *Live Status*\n\n"

        f"━━━ 📤 Posting ━━━\n"
        f"Status   : {posting_st}\n"
        f"Thumbnail: {img_st}\n"
        f"Interval : {POST_INTERVAL}s\n"
        f"Last post: {bot_state['last_post_time'] or 'N/A'}\n\n"

        f"📢 *Channel*: `{CHANNEL_ID}`\n"
        f"Today posts: *{today_cnt}* / {total_slug} total slugs\n"
        f"`{post_bar}` {int(done/total_slug*100) if total_slug else 0}% done\n\n"

        f"Pending  : {pending}\n"
        f"Posted   : {done}\n\n"

        f"━━━ 🔄 Renamer ━━━\n"
        f"{rename_st}\n\n"

        f"━━━ ⬇️ MEGA ━━━\n"
        f"{mega_st}",
        parse_mode="Markdown",
    )


async def cmd_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    bot_state["photo_file_id"] = None
    await update.message.reply_text("📸 Naya thumbnail bhejo.")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    bot_state["photo_file_id"] = update.message.photo[-1].file_id
    msg = "✅ Thumbnail save ho gayi!\nAb /start\\_posting se posting shuru karo."
    if bot_state["running"]:
        msg = "✅ Thumbnail update! Agli post se naya image use hoga."
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_rename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ Folder name do.\n\nUsage: `/rename all-collections`",
            parse_mode="Markdown",
        )
        return

    if rename_state["active"]:
        await update.message.reply_text("⚠️ Rename pehle se chal rahi hai! /cancel\\_renaming karo.", parse_mode="Markdown")
        return

    folder = context.args[0].strip().rstrip("/") + "/"
    rename_state["active"] = True
    rename_state["cancel"] = False

    loop = asyncio.get_event_loop()
    live = LiveMessage(
        loop=loop,
        bot=context.bot,
        chat_id=update.effective_chat.id,
        initial_text=f"🔄 *Renaming & Moving*\n`{folder}` → `{TARGET_FOLDER}`\n\nStarting...",
    )

    t = threading.Thread(target=rename_worker, args=(folder, live), daemon=True)
    rename_state["thread"] = t
    t.start()


async def cmd_cancel_renaming(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not rename_state["active"]:
        await update.message.reply_text("⚠️ Koi rename chal nahi rahi.")
        return
    rename_state["cancel"] = True
    await update.message.reply_text("🛑 Cancel request bhej di...")


async def cmd_mega(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ MEGA link do.\n\nUsage: `/mega https://mega.nz/folder/xxx#yyy`",
            parse_mode="Markdown",
        )
        return

    if mega_state["active"]:
        await update.message.reply_text("⚠️ MEGA download pehle se chal rahi hai! /cancel\\_mega karo.", parse_mode="Markdown")
        return

    mega_link = context.args[0].strip()
    mega_state["active"] = True
    mega_downloader.control["cancel"] = False

    loop = asyncio.get_event_loop()
    live = LiveMessage(
        loop=loop,
        bot=context.bot,
        chat_id=update.effective_chat.id,
        initial_text=(
            f"⬇️ *Downloading MEGA*\n"
            f"`{mega_link[:60]}...`\n\n"
            f"Starting..."
        ),
    )

    # mega_downloader expects update_sync + send_final_sync interface → pass live
    t = threading.Thread(target=mega_worker_fn, args=(mega_link, live), daemon=True)
    mega_state["thread"] = t
    t.start()


async def cmd_cancel_mega(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not mega_state["active"]:
        await update.message.reply_text("⚠️ Koi MEGA download chal nahi rahi.")
        return
    mega_downloader.control["cancel"] = True
    await update.message.reply_text("🛑 Cancel request bhej di...")

# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────

def main():
    d1_ensure_columns()
    run_health_server()

    logger.info("=" * 50)
    logger.info(f"  Channel  : {CHANNEL_ID}")
    logger.info(f"  Interval : {POST_INTERVAL}s")
    logger.info(f"  Admins   : {ADMIN_IDS}")
    logger.info("=" * 50)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",            cmd_start))
    app.add_handler(CommandHandler("start_posting",    cmd_start_posting))
    app.add_handler(CommandHandler("cancel_posting",   cmd_cancel_posting))
    app.add_handler(CommandHandler("status",           cmd_status))
    app.add_handler(CommandHandler("image",            cmd_image))
    app.add_handler(CommandHandler("rename",           cmd_rename))
    app.add_handler(CommandHandler("cancel_renaming",  cmd_cancel_renaming))
    app.add_handler(CommandHandler("mega",             cmd_mega))
    app.add_handler(CommandHandler("cancel_mega",      cmd_cancel_mega))
    app.add_handler(MessageHandler(filters.PHOTO,      handle_photo))

    logger.info("Bot online. /start bhejo.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
