import os
import time
import logging
import asyncio
from pathlib import Path

from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified

# =========================
# LOAD ENV
# =========================
load_dotenv()

API_ID    = int(os.getenv("API_ID", "0"))
API_HASH  = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Cloudflare R2 config
R2_ACCOUNT_ID    = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY    = os.getenv("R2_SECRET_KEY", "")
R2_BUCKET        = os.getenv("R2_BUCKET", "")
R2_PUBLIC_URL    = os.getenv("R2_PUBLIC_URL", "")    # e.g. https://pub-xxx.r2.dev
WORKER_BASE_URL  = os.getenv("WORKER_BASE_URL", "")  # e.g. https://your-worker.workers.dev

DOWNLOAD_DIR      = "downloads"
PROGRESS_INTERVAL = 5
MAX_RETRIES       = 3

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =========================
# BOT CLIENT
# =========================
bot = Client(
    "SimpleR2Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# =========================
# HELPERS
# =========================

def human_size(num: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"


def progress_bar(current: int, total: int, length: int = 15) -> str:
    filled = int(length * current / total) if total else 0
    bar    = "█" * filled + "░" * (length - filled)
    pct    = (current / total * 100) if total else 0
    return f"[{bar}] {pct:.1f}%"


async def safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try:
            await msg.edit_text(text)
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"safe_edit failed: {e}")


def make_progress_callback(message: Message, action: str):
    state = {"last_update": 0.0}

    async def callback(current: int, total: int):
        now = time.time()
        if now - state["last_update"] < PROGRESS_INTERVAL and current < total:
            return
        state["last_update"] = now
        icon = "⬇️ Downloading" if action == "download" else "⬆️ Uploading"
        text = (
            f"{icon}...\n\n"
            f"{progress_bar(current, total)}\n"
            f"{human_size(current)} / {human_size(total)}"
        )
        await safe_edit(message, text)

    return callback


def _guess_content_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return {
        ".mp4":  "video/mp4",
        ".mkv":  "video/x-matroska",
        ".webm": "video/webm",
        ".avi":  "video/x-msvideo",
        ".mov":  "video/quicktime",
        ".mp3":  "audio/mpeg",
        ".m4a":  "audio/mp4",
        ".ogg":  "audio/ogg",
        ".pdf":  "application/pdf",
        ".zip":  "application/zip",
    }.get(ext, "application/octet-stream")


async def upload_to_r2(file_path: str, original_name: str) -> tuple[str, str]:
    """Upload to R2 with UUID key. Returns (watch_url, r2_public_url)."""
    import boto3
    import uuid
    from botocore.config import Config
    from urllib.parse import quote

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )

    uid        = uuid.uuid4().hex
    safe_name  = original_name.replace("/", "_").replace("\\", "_")
    object_key = f"{uid}_{safe_name}"   # e.g. "a3f8c2d1_Movie.mkv"

    with open(file_path, "rb") as f:
        s3.upload_fileobj(
            f, R2_BUCKET, object_key,
            ExtraArgs={"ContentType": _guess_content_type(original_name)}
        )

    r2_url    = f"{R2_PUBLIC_URL.rstrip('/')}/{quote(object_key)}"
    watch_url = f"{WORKER_BASE_URL.rstrip('/')}/watch/{quote(object_key, safe='')}"
    return watch_url, r2_url


# =========================
# MEDIA HANDLER
# =========================

@bot.on_message(filters.private & (
    filters.video | filters.document | filters.audio |
    filters.voice | filters.video_note | filters.photo |
    filters.animation | filters.sticker
))
async def media_handler(client: Client, message: Message):
    # ── Identify media ───────────────────────────────────────────────────────
    if message.photo:
        media         = message.photo[-1]
        original_name = f"photo_{message.id}.jpg"
    elif message.video:
        media         = message.video
        original_name = media.file_name or f"video_{message.id}.mp4"
    elif message.audio:
        media         = message.audio
        original_name = media.file_name or f"audio_{message.id}.mp3"
    elif message.voice:
        media         = message.voice
        original_name = f"voice_{message.id}.ogg"
    elif message.video_note:
        media         = message.video_note
        original_name = f"videonote_{message.id}.mp4"
    elif message.animation:
        media         = message.animation
        original_name = media.file_name or f"animation_{message.id}.gif"
    elif message.sticker:
        media         = message.sticker
        original_name = f"sticker_{message.id}.webp"
    elif message.document:
        media         = message.document
        original_name = media.file_name or f"document_{message.id}"
    else:
        await message.reply_text("❌ Unsupported media type.")
        return

    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
    progress_msg = await message.reply_text("⬇️ Downloading...")

    # ── Download ─────────────────────────────────────────────────────────────
    downloaded_path = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            downloaded_path = await client.download_media(
                message,
                file_name=os.path.join(DOWNLOAD_DIR, original_name),
                progress=make_progress_callback(progress_msg, "download")
            )
            break
        except FloodWait as e:
            await safe_edit(progress_msg, f"⏳ Flood wait {e.value}s...")
            await asyncio.sleep(e.value)
        except Exception as e:
            if attempt == MAX_RETRIES:
                await safe_edit(progress_msg, f"❌ Download failed:\n{e}")
                return
            await asyncio.sleep(2 * attempt)

    if not downloaded_path:
        await safe_edit(progress_msg, "❌ Download failed.")
        return

    file_size = human_size(os.path.getsize(downloaded_path))
    await safe_edit(progress_msg, f"⬆️ Uploading to R2...\n{original_name} ({file_size})")

    # ── Upload to R2 ─────────────────────────────────────────────────────────
    try:
        watch_url, r2_url = await upload_to_r2(downloaded_path, original_name)
    except Exception as e:
        logger.error(f"R2 upload failed: {e}")
        await safe_edit(progress_msg, f"❌ Upload to R2 failed:\n{e}")
        return
    finally:
        try:
            os.remove(downloaded_path)
        except Exception:
            pass

    try:
        await progress_msg.delete()
    except Exception:
        pass

    await message.reply_text(
        f"✅ Done!\n\n"
        f"📄 {original_name}\n"
        f"📦 {file_size}\n\n"
        f"▶️ Watch / Stream:\n{watch_url}"
    )


# =========================
# /start
# =========================

@bot.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    await message.reply_text(
        "👋 Send me any file and I'll upload it to R2 and give you a watch link!"
    )


# =========================
# RUN
# =========================
logger.info("Starting Bot...")
bot.run()
