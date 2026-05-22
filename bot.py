import os
import time
import asyncio
import logging
import uuid
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from telethon import TelegramClient, events
from FastTelethon import download_file
from dotenv import load_dotenv

from health_check import start_health_server

# ================== ENV ==================
load_dotenv()

API_ID    = int(os.getenv("API_ID"))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

R2_ACCOUNT_ID    = os.getenv("R2_ACCOUNT_ID")       # Cloudflare Account ID
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")     # R2 Access Key ID
R2_SECRET_KEY    = os.getenv("R2_SECRET_KEY")         # R2 Secret Access Key
R2_BUCKET_NAME   = os.getenv("R2_BUCKET_NAME")        # e.g. "my-videos"
R2_PUBLIC_DOMAIN = os.getenv("R2_PUBLIC_DOMAIN")      # e.g. "cdn.example.com" (custom domain or r2.dev subdomain)

R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ================== LOG ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ================== DIR ==================
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ================== CLIENT ==================
bot = TelegramClient("bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ─────────────────────────────────────────────
#  R2 CLIENT (boto3 S3-compatible)
# ─────────────────────────────────────────────

def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
        region_name="auto",
    )

# ─────────────────────────────────────────────
#  UTILS
# ─────────────────────────────────────────────

def human(size: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def progress_bar(current: int, total: int, start: float, label: str, filename: str) -> str:
    percent = current / total if total else 0
    filled  = int(20 * percent)
    bar     = "█" * filled + "░" * (20 - filled)
    elapsed = time.time() - start
    speed   = current / elapsed if elapsed > 0 else 0
    eta     = int((total - current) / speed) if speed > 0 else 0
    return (
        f"{label}\n\n"
        f"📄 File  : {filename}\n"
        f"📦 Size  : {human(total)}\n"
        f"⚡ Speed : {human(speed)}/s\n"
        f"⏳ ETA   : {eta}s\n\n"
        f"[{bar}] {percent * 100:.1f}%"
    )


# ─────────────────────────────────────────────
#  HTML STREAMING PAGE GENERATOR
# ─────────────────────────────────────────────

def make_streaming_page(video_url: str, title: str, filesize: str) -> str:
    safe_title = title.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{safe_title}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: #0f0f0f;
    color: #e0e0e0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 24px 16px;
  }}
  .card {{
    width: 100%;
    max-width: 860px;
    background: #1a1a1a;
    border-radius: 16px;
    overflow: hidden;
    box-shadow: 0 8px 40px rgba(0,0,0,0.6);
  }}
  video {{
    width: 100%;
    display: block;
    background: #000;
    max-height: 70vh;
  }}
  .info {{
    padding: 16px 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
    border-top: 1px solid #2a2a2a;
  }}
  .title {{ font-size: 15px; font-weight: 500; color: #fff; word-break: break-all; }}
  .meta  {{ font-size: 13px; color: #888; white-space: nowrap; }}
  a.dl {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 7px 16px;
    background: #2563eb;
    color: #fff;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 500;
    text-decoration: none;
    white-space: nowrap;
    transition: background .2s;
  }}
  a.dl:hover {{ background: #1d4ed8; }}
</style>
</head>
<body>
<div class="card">
  <video controls autoplay preload="metadata" playsinline>
    <source src="{video_url}" type="video/mp4">
    Your browser does not support HTML5 video.
  </video>
  <div class="info">
    <span class="title">{safe_title}</span>
    <span class="meta">{filesize}</span>
    <a class="dl" href="{video_url}" download="{safe_title}">&#8595; Download</a>
  </div>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────
#  R2 UPLOAD HELPERS
# ─────────────────────────────────────────────

class R2Error(Exception):
    pass


def upload_to_r2_multipart(
    s3,
    file_path: str,
    object_key: str,
    content_type: str = "video/mp4",
    progress_cb=None,
) -> None:
    """
    Uploads file_path to R2 using multipart upload.
    Calls progress_cb(bytes_sent, total_bytes, start_time) periodically.
    """
    file_size = os.path.getsize(file_path)
    CHUNK     = 8 * 1024 * 1024  # 8 MB per part (R2 min is 5 MB)

    mpu = s3.create_multipart_upload(
        Bucket=R2_BUCKET_NAME,
        Key=object_key,
        ContentType=content_type,
    )
    upload_id = mpu["UploadId"]
    parts      = []
    sent       = 0
    start      = time.time()

    try:
        part_number = 1
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    break
                resp = s3.upload_part(
                    Bucket=R2_BUCKET_NAME,
                    Key=object_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
                sent += len(chunk)
                if progress_cb:
                    progress_cb(sent, file_size, start)
                part_number += 1

        s3.complete_multipart_upload(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        log.info("R2 multipart upload complete: %s", object_key)

    except Exception as e:
        s3.abort_multipart_upload(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id,
        )
        raise R2Error(f"Multipart upload failed: {e}") from e


def upload_string_to_r2(s3, content: str, object_key: str, content_type: str) -> None:
    s3.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=object_key,
        Body=content.encode("utf-8"),
        ContentType=content_type,
    )
    log.info("R2 string upload: %s", object_key)


# ─────────────────────────────────────────────
#  BOT HANDLER
# ─────────────────────────────────────────────

@bot.on(events.NewMessage)
async def handler(event):
    if not event.video:
        return await event.reply("❌ Pehle ek video bhejo.")

    msg       = await event.reply("🚀 Starting...")
    filename  = event.file.name or f"video_{event.id}.mp4"
    total_sz  = event.file.size
    file_path = os.path.join(DOWNLOAD_DIR, f"{event.id}_{filename}")

    # Unique folder per upload so filenames don't clash
    folder_id = str(uuid.uuid4())[:8]
    video_key = f"{folder_id}/{filename}"
    page_key  = f"{folder_id}/index.html"

    # ──────────── 1. DOWNLOAD FROM TELEGRAM (FastTelethon) ────────────
    dl_start = time.time()
    dl_last  = 0.0

    def dl_progress(current, total):
        nonlocal dl_last
        if time.time() - dl_last < 10:
            return
        asyncio.get_event_loop().call_soon_threadsafe(
            lambda c=current, t=total: asyncio.ensure_future(
                msg.edit(progress_bar(c, t, dl_start, "📥 Downloading from Telegram...", filename))
            )
        )
        dl_last = time.time()

    log.info("Downloading '%s' (%.1f MB) via FastTelethon", filename, total_sz / 1_048_576)

    with open(file_path, "wb") as f:
        await download_file(
            client=bot,
            location=event.message.media,
            out=f,
            progress_callback=dl_progress,
        )

    log.info("Download complete: %s", file_path)
    await msg.edit(
        f"✅ Download done — {human(total_sz)}\n\n"
        f"☁️ Uploading to Cloudflare R2..."
    )

    # ──────────── 2. UPLOAD VIDEO TO R2 ────────────
    s3       = get_r2_client()
    up_last  = [0.0]
    up_start = time.time()

    def up_progress(sent, total, start):
        now = time.time()
        if now - up_last[0] < 10:
            return
        up_last[0] = now
        asyncio.get_event_loop().call_soon_threadsafe(
            lambda s=sent, t=total, st=start: asyncio.ensure_future(
                msg.edit(progress_bar(s, t, st, "☁️ Uploading to Cloudflare R2...", filename))
            )
        )

    try:
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: upload_to_r2_multipart(
                s3, file_path, video_key,
                content_type="video/mp4",
                progress_cb=up_progress,
            )
        )
    finally:
        # ──────────── DELETE LOCAL FILE IMMEDIATELY ────────────
        try:
            os.remove(file_path)
            log.info("Local file deleted: %s", file_path)
        except OSError:
            pass

    # ──────────── 3. BUILD PUBLIC URLs ────────────
    video_url = f"https://{R2_PUBLIC_DOMAIN}/{video_key}"
    page_url  = f"https://{R2_PUBLIC_DOMAIN}/{page_key}"

    # ──────────── 4. UPLOAD STREAMING HTML PAGE ────────────
    await msg.edit("🎬 Generating streaming page...")

    html_content = make_streaming_page(
        video_url=video_url,
        title=filename,
        filesize=human(total_sz),
    )

    await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: upload_string_to_r2(
            s3, html_content, page_key, content_type="text/html; charset=utf-8"
        )
    )

    log.info("Streaming page uploaded: %s", page_url)

    # ──────────── 5. DONE ────────────
    await msg.edit(
        f"✅ Done!\n\n"
        f"📄 {filename}\n"
        f"📦 {human(total_sz)}\n\n"
        f"🎬 Streaming Page:\n{page_url}\n\n"
        f"🔗 Direct Video Link:\n`{video_url}`"
    )
    log.info("Job complete: video=%s page=%s", video_key, page_key)


# ─────────────────────────────────────────────
#  RUN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    start_health_server()
    print("🤖 Bot Running (Cloudflare R2 + FastTelethon mode)...")
    bot.run_until_disconnected()
