"""
R2 Renamer + MEGA Downloader + Telegram Bot — Koyeb Deployment
================================================================
Bot commands:
  /start            — Posting shuru karo (image pehle chahiye)
  /stop             — Posting band karo
  /status           — Stats dekho
  /image            — Naya thumbnail image set karo
  /rename <folder>  — R2 folder ko rename karke D1 mein save karo
  /cancelrename     — Chal rahi rename process rok do
  /mega <link>      — MEGA link se download + R2 upload
  /cancelmega       — Chal rahi MEGA download rok do
  /help             — Commands list
"""

import os
import sys
import json
import time
import uuid
import random
import logging
import asyncio
import threading
import requests
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from health_check import run_health_server
import mega_downloader

# =====================================================
# ENV
# =====================================================

load_dotenv(dotenv_path=Path(__file__).parent / ".env")


def require_env(key):
    val = os.getenv(key, "").strip()
    if not val:
        raise SystemExit(f"ERROR: '{key}' not set in environment")
    return val


def optional_env(key, default=""):
    return os.getenv(key, default).strip() or default


# =====================================================
# CONFIG
# =====================================================

CF_ACCOUNT_ID  = require_env("CF_ACCOUNT_ID")
R2_ACCESS_KEY  = require_env("R2_ACCESS_KEY")
R2_SECRET_KEY  = require_env("R2_SECRET_KEY")
R2_BUCKET_NAME = require_env("R2_BUCKET_NAME")
D1_DATABASE_ID = require_env("D1_DATABASE_ID")
CF_API_TOKEN   = require_env("CF_API_TOKEN")

BOT_TOKEN      = require_env("TELEGRAM_BOT_TOKEN")
CHANNEL_ID     = require_env("TELEGRAM_CHANNEL_ID")
POST_URL       = require_env("POST_URL").rstrip("/")
ADMIN_IDS_RAW  = require_env("ADMIN_TELEGRAM_ID")
ADMIN_IDS      = {
    int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip()
}

TARGET_FOLDER  = optional_env("TARGET_FOLDER", "new-videos/")
PROGRESS_FILE  = optional_env("PROGRESS_FILE", "progress.json")
POST_INTERVAL  = int(optional_env("POST_INTERVAL_SECONDS", "350"))  # 350s ≈ 247 posts/day

if not TARGET_FOLDER.endswith("/"):
    TARGET_FOLDER += "/"

VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".wmv",
    ".flv", ".webm", ".m4v", ".3gp", ".ts",
    ".mts", ".m2ts", ".mpeg", ".mpg", ".vob"
}

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =====================================================
# R2 CLIENT
# =====================================================

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

# =====================================================
# D1 HELPERS
# =====================================================

D1_URL = (
    f"https://api.cloudflare.com/client/v4"
    f"/accounts/{CF_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}/query"
)

D1_HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type" : "application/json",
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
            D1_URL,
            headers=D1_HEADERS,
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
        "SELECT id, slug FROM videos "
        "WHERE posted = 0 ORDER BY created_at ASC LIMIT 1"
    )
    return rows[0] if rows else None


def d1_mark_posted(video_id):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    d1_query(
        "UPDATE videos SET posted = 1, posted_at = ? WHERE id = ?",
        [now, video_id],
    )


def d1_stats():
    rows = d1_query(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN posted=1 THEN 1 ELSE 0 END) AS done, "
        "SUM(CASE WHEN posted=0 THEN 1 ELSE 0 END) AS pending "
        "FROM videos"
    )
    return rows[0] if rows else {"total": 0, "done": 0, "pending": 0}

# =====================================================
# R2 HELPERS
# =====================================================

def get_r2_files(prefix):
    files     = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            files.append({"key": key, "size": obj.get("Size", 0)})
    return files


def get_file_size(key):
    try:
        resp = s3.head_object(Bucket=R2_BUCKET_NAME, Key=key)
        return resp.get("ContentLength", 0)
    except Exception:
        return 0

# =====================================================
# RENAMER HELPERS
# =====================================================

def make_slug():
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choices(chars, k=8))


def generate_name(ext):
    now = datetime.now()
    return (
        f"[TG - @atoz_links]VID_"
        f"{now.strftime('%H%M%S%d%m%Y%f')}"
        f"{ext}"
    )


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"processed": []}


def save_progress(data):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

# =====================================================
# GLOBAL STATE
# =====================================================

bot_state = {
    "running"        : False,   # poster on/off
    "photo_file_id"  : None,    # thumbnail file_id
    "posted_count"   : 0,
    "failed_count"   : 0,
    "last_post_time" : None,
}

rename_state = {
    "active"    : False,        # rename chal rahi hai?
    "cancel"    : False,        # cancel flag
    "thread"    : None,         # background thread
}

mega_state = {
    "active"    : False,        # mega download chal rahi hai?
    "thread"    : None,
}

# =====================================================
# ADMIN CHECK
# =====================================================

def is_admin(update: Update) -> bool:
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        logger.warning(f"Unauthorized: user_id={uid}")
        return False
    return True

# =====================================================
# RENAMER — background thread mein chalega
# =====================================================

def rename_worker(folder: str, send_msg_sync):
    """
    Yeh function ek alag thread mein chalta hai.
    send_msg_sync(text) — Telegram pe message bhejne ke liye callback.
    """
    progress  = load_progress()
    processed = set(progress["processed"])

    try:
        send_msg_sync(f"🔍 Scanning: `{folder}`", parse_mode="Markdown")

        all_files   = get_r2_files(folder)
        total       = len(all_files)
        video_files = [
            f for f in all_files
            if os.path.splitext(f["key"])[1].lower() in VIDEO_EXTENSIONS
        ]
        skipped_img = total - len(video_files)

        send_msg_sync(
            f"📂 *Scan Complete*\n\n"
            f"Total files : {total}\n"
            f"Videos      : {len(video_files)}\n"
            f"Images skip : {skipped_img}\n\n"
            f"Starting rename...",
            parse_mode="Markdown"
        )

        if not video_files:
            send_msg_sync("⚠️ Koi video file nahi mili. Rename band.")
            rename_state["active"] = False
            return

        success = failed = duplicate = 0
        total_v = len(video_files)

        # Progress update har 25% complete hone par
        PROGRESS_MILESTONE_PCT = 25
        last_milestone = -1

        for index, file_info in enumerate(video_files, start=1):

            # Cancel check
            if rename_state["cancel"]:
                save_progress(progress)
                send_msg_sync(
                    f"🛑 *Rename Cancelled*\n\n"
                    f"Done    : {success}\n"
                    f"Failed  : {failed}\n"
                    f"Remaining: {total_v - index + 1}",
                    parse_mode="Markdown"
                )
                rename_state["active"] = False
                rename_state["cancel"] = False
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

                processed.add(old_key)
                progress["processed"] = list(processed)
                success += 1

            except Exception as e:
                failed += 1
                logger.error(f"Rename error [{old_key}]: {e}")
                time.sleep(1)

            # 25% milestone pe progress save + Telegram update
            pct = int((index / total_v) * 100)
            milestone = (pct // PROGRESS_MILESTONE_PCT) * PROGRESS_MILESTONE_PCT
            if milestone > last_milestone and milestone > 0:
                last_milestone = milestone
                save_progress(progress)
                send_msg_sync(
                    f"⏳ *Rename Progress* — {milestone}%\n\n"
                    f"[{index}/{total_v}]\n"
                    f"✅ Success : {success}\n"
                    f"❌ Failed  : {failed}\n"
                    f"⏭ Skip    : {duplicate}",
                    parse_mode="Markdown"
                )

        save_progress(progress)

        send_msg_sync(
            f"✅ *Rename Complete!*\n\n"
            f"Success : {success}\n"
            f"Failed  : {failed}\n"
            f"Skipped : {duplicate}\n"
            f"Img skip: {skipped_img}\n\n"
            f"_(Source folder delete nahi ki gayi)_",
            parse_mode="Markdown"
        )

    except Exception as e:
        save_progress(progress)
        send_msg_sync(f"💥 Fatal Error: {e}")
        logger.error(f"Rename worker fatal: {e}")

    finally:
        rename_state["active"] = False
        rename_state["cancel"] = False

# =====================================================
# BOT HANDLERS
# =====================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if bot_state["running"]:
        await update.message.reply_text("⚡ Posting pehle se chal rahi hai!")
        return

    if not bot_state["photo_file_id"]:
        await update.message.reply_text(
            "📸 Pehle ek thumbnail image bhejo.\n"
            "Woh image har channel post mein use hogi."
        )
        return

    bot_state["running"] = True

    for job in context.job_queue.get_jobs_by_name("poster"):
        job.schedule_removal()

    context.job_queue.run_repeating(
        posting_job,
        interval=POST_INTERVAL,
        first=1,
        name="poster",
    )

    await update.message.reply_text(
        f"▶️ Posting shuru!\n"
        f"Interval: har {POST_INTERVAL}s mein ek post."
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if not bot_state["running"]:
        await update.message.reply_text("⏹ Posting pehle se band hai.")
        return

    bot_state["running"] = False
    for job in context.job_queue.get_jobs_by_name("poster"):
        job.schedule_removal()

    await update.message.reply_text("⏹ Posting band kar di gayi.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    try:
        stats = d1_stats()
    except Exception as e:
        await update.message.reply_text(f"D1 error: {e}")
        return

    image_set    = "✅ Set hai"       if bot_state["photo_file_id"] else "❌ Set nahi"
    posting_stat = "▶️ Chal rahi hai" if bot_state["running"]       else "⏹ Band hai"
    rename_stat  = "🔄 Chal rahi hai" if rename_state["active"]     else "💤 Idle"

    await update.message.reply_text(
        f"📊 *Status*\n\n"
        f"Posting  : {posting_stat}\n"
        f"Renamer  : {rename_stat}\n"
        f"Image    : {image_set}\n"
        f"Interval : {POST_INTERVAL}s\n\n"
        f"*D1 Stats*\n"
        f"Total    : {stats['total']}\n"
        f"Posted   : {stats['done']}\n"
        f"Pending  : {stats['pending']}\n\n"
        f"*Session*\n"
        f"✅ {bot_state['posted_count']} posted\n"
        f"❌ {bot_state['failed_count']} failed\n"
        f"🕐 Last  : {bot_state['last_post_time'] or 'N/A'}",
        parse_mode="Markdown",
    )


async def cmd_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    bot_state["photo_file_id"] = None
    await update.message.reply_text("📸 Naya thumbnail image bhejo.")


async def cmd_rename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    # Folder name argument check
    if not context.args:
        await update.message.reply_text(
            "⚠️ Folder name do.\n\n"
            "Usage: `/rename all-collections`\n"
            "Ya trailing slash ke saath: `/rename all-collections/`",
            parse_mode="Markdown"
        )
        return

    if rename_state["active"]:
        await update.message.reply_text(
            "⚠️ Rename pehle se chal rahi hai!\n"
            "Rokne ke liye /cancelrename karo."
        )
        return

    folder = context.args[0].strip()
    if not folder.endswith("/"):
        folder += "/"

    rename_state["active"] = True
    rename_state["cancel"] = False

    # Telegram async se sync bridge
    loop = asyncio.get_event_loop()

    def send_msg_sync(text, parse_mode=None):
        """Thread-safe Telegram message sender."""
        coro = update.message.reply_text(text, parse_mode=parse_mode)
        asyncio.run_coroutine_threadsafe(coro, loop)

    # Background thread mein rename chalao
    t = threading.Thread(
        target=rename_worker,
        args=(folder, send_msg_sync),
        daemon=True,
    )
    rename_state["thread"] = t
    t.start()

    await update.message.reply_text(
        f"🚀 Rename shuru ho gayi!\n\n"
        f"Folder: `{folder}`\n"
        f"Har 50 files pe update milega.\n\n"
        f"Rokne ke liye: /cancelrename",
        parse_mode="Markdown"
    )


async def cmd_cancelrename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if not rename_state["active"]:
        await update.message.reply_text("⚠️ Koi rename chal nahi rahi.")
        return

    rename_state["cancel"] = True
    await update.message.reply_text("🛑 Cancel request bhej di... ruk rahi hai.")


async def cmd_mega(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ MEGA link do.\n\n"
            "Usage: `/mega https://mega.nz/folder/xxxx#yyyy`",
            parse_mode="Markdown"
        )
        return

    if mega_state["active"]:
        await update.message.reply_text(
            "⚠️ MEGA download pehle se chal rahi hai!\n"
            "Rokne ke liye /cancelmega karo."
        )
        return

    mega_link = context.args[0].strip()

    mega_state["active"] = True
    mega_downloader.control["cancel"] = False

    loop = asyncio.get_event_loop()

    def send_msg_sync(text, parse_mode=None):
        """Thread-safe Telegram message sender — bot khud se message bhejta hai."""
        coro = update.message.reply_text(text, parse_mode=parse_mode)
        asyncio.run_coroutine_threadsafe(coro, loop)

    def mega_worker():
        try:
            mega_downloader.run_mega_download(mega_link, send_msg_sync)
        except Exception as e:
            logger.error(f"MEGA worker fatal: {e}")
            send_msg_sync(f"💥 Fatal Error: {e}")
        finally:
            mega_state["active"] = False

    t = threading.Thread(target=mega_worker, daemon=True)
    mega_state["thread"] = t
    t.start()

    await update.message.reply_text(
        f"🚀 MEGA download shuru ho gayi!\n\n"
        f"Har 25% progress pe update milega.\n"
        f"Bandwidth limit lagne par 6 ghante baad auto-retry hoga.\n\n"
        f"Rokne ke liye: /cancelmega",
        parse_mode="Markdown"
    )


async def cmd_cancelmega(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    if not mega_state["active"]:
        await update.message.reply_text("⚠️ Koi MEGA download chal nahi rahi.")
        return

    mega_downloader.control["cancel"] = True
    await update.message.reply_text("🛑 Cancel request bhej di... ruk rahi hai.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await update.message.reply_text(
        "🤖 *Commands*\n\n"
        "*Posting*\n"
        "/start              — Channel posting shuru karo\n"
        "/stop               — Posting band karo\n"
        "/image              — Naya thumbnail set karo\n\n"
        "*Renamer*\n"
        "/rename `<folder>`  — R2 folder rename + D1 save\n"
        "/cancelrename       — Chal rahi rename rok do\n\n"
        "*Info*\n"
        "/status             — Full stats dekho\n"
        "/help               — Yeh message",
        parse_mode="Markdown",
    )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    bot_state["photo_file_id"] = update.message.photo[-1].file_id

    msg = "✅ Thumbnail save ho gayi!\n\nAb /start se posting shuru karo."
    if bot_state["running"]:
        msg = "✅ Thumbnail update ho gayi! Agli post se naya image use hoga."

    await update.message.reply_text(msg)


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
        bot_state["posted_count"]  += 1
        bot_state["last_post_time"] = datetime.now().strftime("%H:%M:%S")
        logger.info(f"Posted: {url}")
    except Exception as e:
        bot_state["failed_count"] += 1
        logger.error(f"Post failed: {e}")

# =====================================================
# MAIN
# =====================================================

def main():
    d1_ensure_columns()

    # Health check server start karo (Koyeb keep-alive ke liye)
    run_health_server()

    logger.info("=" * 50)
    logger.info("  Bot + Renamer Starting")
    logger.info("=" * 50)
    logger.info(f"  Channel  : {CHANNEL_ID}")
    logger.info(f"  POST_URL : {POST_URL}")
    logger.info(f"  Interval : {POST_INTERVAL}s")
    logger.info(f"  Admin IDs: {ADMIN_IDS}")
    logger.info("=" * 50)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("stop",         cmd_stop))
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("image",        cmd_image))
    app.add_handler(CommandHandler("rename",       cmd_rename))
    app.add_handler(CommandHandler("cancelrename", cmd_cancelrename))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(MessageHandler(filters.PHOTO,  handle_photo))

    logger.info("Bot online. /help bhejo.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
