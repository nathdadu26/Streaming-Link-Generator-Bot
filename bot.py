import os
import time
import asyncio
import logging
import uuid
import boto3
from botocore.config import Config
from telethon import TelegramClient, events
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation
from telethon.crypto import AuthKey
from dotenv import load_dotenv
from health_check import start_health_server

# ================== ENV ==================
load_dotenv()

API_ID    = int(os.getenv("API_ID"))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
R2_ACCOUNT_ID    = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY    = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME   = os.getenv("R2_BUCKET_NAME")
R2_PUBLIC_DOMAIN = os.getenv("R2_PUBLIC_DOMAIN")
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ================== DOWNLOAD CONFIG ==================
WORKERS        = 6           # parallel connections
PART_SIZE      = 512 * 1024  # 512 KB — must stay as-is (Telegram requirement)
PROGRESS_EVERY = 5           # seconds between progress edits

# ================== LOG ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ================== DIR ==================
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ================== MAIN CLIENT ==================
bot = TelegramClient("bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ─────────────────────────────────────────────
# R2
# ─────────────────────────────────────────────
def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "adaptive"}),
        region_name="auto",
    )

# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────
def human(size: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def progress_bar(current, total, start, label, filename):
    pct    = current / total if total else 0
    filled = int(20 * pct)
    bar    = "█" * filled + "░" * (20 - filled)
    elapsed = time.time() - start
    speed  = current / elapsed if elapsed > 0 else 0
    eta    = int((total - current) / speed) if speed > 0 else 0
    return (
        f"{label}\n\n"
        f"📄 File  : {filename}\n"
        f"📦 Size  : {human(total)}\n"
        f"⚡ Speed : {human(speed)}/s\n"
        f"⏳ ETA   : {eta}s\n\n"
        f"[{bar}] {pct*100:.1f}%"
    )

# ─────────────────────────────────────────────
# PARALLEL DOWNLOAD — uses bot's borrowed DC senders
# ─────────────────────────────────────────────
async def parallel_download(media, file_path: str, total_size: int, msg, filename: str):
    """
    Uses bot._borrow_exported_sender(dc_id) to open multiple connections
    to the correct DC where the file is stored, then fires GetFileRequest
    with different offsets concurrently.
    """
    doc = media.document
    dc_id = doc.dc_id  # actual DC where file lives (was 4 in your logs)

    location = InputDocumentFileLocation(
        id=doc.id,
        access_hash=doc.access_hash,
        file_reference=doc.file_reference,
        thumb_size="",
    )

    num_parts  = (total_size + PART_SIZE - 1) // PART_SIZE
    part_queue = asyncio.Queue()
    for i in range(num_parts):
        await part_queue.put(i)

    # Pre-allocate file
    with open(file_path, "wb") as f:
        f.seek(total_size - 1)
        f.write(b"\0")

    downloaded_bytes = [0]
    dl_start  = time.time()
    last_edit = [0.0]
    write_lock = asyncio.Lock()

    log.info("Parallel download: %d parts, DC %d, %d workers", num_parts, dc_id, WORKERS)

    async def worker(worker_id: int):
        # Borrow a sender for the correct DC — Telethon handles auth export internally
        sender = await bot._borrow_exported_sender(dc_id)
        try:
            while True:
                try:
                    part_no = part_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                offset = part_no * PART_SIZE
                limit  = min(PART_SIZE, total_size - offset)
                retries = 0

                while True:
                    try:
                        result = await sender.send(GetFileRequest(
                            location=location,
                            offset=offset,
                            limit=limit,
                            precise=True,
                            cdn_supported=False,
                        ))
                        data = result.bytes
                        break
                    except Exception as e:
                        retries += 1
                        if retries >= 5:
                            raise RuntimeError(f"Worker {worker_id} part {part_no} failed: {e}") from e
                        log.warning("Worker %d part %d retry %d: %s", worker_id, part_no, retries, e)
                        await asyncio.sleep(1)

                async with write_lock:
                    with open(file_path, "r+b") as f:
                        f.seek(offset)
                        f.write(data)
                    downloaded_bytes[0] += len(data)
                    now = time.time()
                    if now - last_edit[0] >= PROGRESS_EVERY:
                        last_edit[0] = now
                        try:
                            await msg.edit(
                                progress_bar(downloaded_bytes[0], total_size, dl_start,
                                             "📥 Downloading from Telegram...", filename)
                            )
                        except Exception:
                            pass

                part_queue.task_done()
        finally:
            await bot._return_exported_sender(sender)

    workers = [asyncio.create_task(worker(i)) for i in range(WORKERS)]
    await asyncio.gather(*workers)
    log.info("Parallel download complete — %s (%.1f MB/s avg)",
             file_path, total_size / (time.time() - dl_start) / 1_048_576)

# ─────────────────────────────────────────────
# HTML STREAMING PAGE
# ─────────────────────────────────────────────
def make_streaming_page(video_url, title, filesize):
    s = title.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{s}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0f0f0f;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
     min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px 16px}}
.card{{width:100%;max-width:860px;background:#1a1a1a;border-radius:16px;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,.6)}}
video{{width:100%;display:block;background:#000;max-height:70vh}}
.info{{padding:16px 20px;display:flex;align-items:center;justify-content:space-between;gap:12px;
       flex-wrap:wrap;border-top:1px solid #2a2a2a}}
.title{{font-size:15px;font-weight:500;color:#fff;word-break:break-all}}
.meta{{font-size:13px;color:#888;white-space:nowrap}}
a.dl{{display:inline-flex;align-items:center;gap:6px;padding:7px 16px;background:#2563eb;color:#fff;
      border-radius:8px;font-size:13px;font-weight:500;text-decoration:none;transition:background .2s}}
a.dl:hover{{background:#1d4ed8}}
</style></head><body>
<div class="card">
<video controls autoplay preload="metadata" playsinline>
  <source src="{video_url}" type="video/mp4">
</video>
<div class="info">
  <span class="title">{s}</span>
  <span class="meta">{filesize}</span>
  <a class="dl" href="{video_url}" download="{s}">&#8595; Download</a>
</div></div></body></html>"""

# ─────────────────────────────────────────────
# R2 MULTIPART UPLOAD
# ─────────────────────────────────────────────
class R2Error(Exception): pass

def r2_multipart_upload(s3, file_path, key, content_type="video/mp4", progress_cb=None):
    size  = os.path.getsize(file_path)
    CHUNK = 8 * 1024 * 1024
    mpu   = s3.create_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key, ContentType=content_type)
    uid   = mpu["UploadId"]
    parts = []
    sent  = 0
    start = time.time()
    try:
        pn = 1
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    break
                r = s3.upload_part(Bucket=R2_BUCKET_NAME, Key=key,
                                   UploadId=uid, PartNumber=pn, Body=chunk)
                parts.append({"PartNumber": pn, "ETag": r["ETag"]})
                sent += len(chunk)
                if progress_cb:
                    progress_cb(sent, size, start)
                pn += 1
        s3.complete_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key, UploadId=uid,
                                     MultipartUpload={"Parts": parts})
    except Exception as e:
        s3.abort_multipart_upload(Bucket=R2_BUCKET_NAME, Key=key, UploadId=uid)
        raise R2Error(e) from e

def r2_put(s3, content, key, content_type):
    s3.put_object(Bucket=R2_BUCKET_NAME, Key=key,
                  Body=content.encode(), ContentType=content_type)

# ─────────────────────────────────────────────
# BOT HANDLER
# ─────────────────────────────────────────────
@bot.on(events.NewMessage)
async def handler(event):
    if not event.video:
        return await event.reply("❌ Pehle ek video bhejo.")

    msg       = await event.reply("🚀 Starting...")
    filename  = event.file.name or f"video_{event.id}.mp4"
    total_sz  = event.file.size
    file_path = os.path.join(DOWNLOAD_DIR, f"{event.id}_{filename}")
    folder_id = str(uuid.uuid4())[:8]
    video_key = f"{folder_id}/{filename}"
    page_key  = f"{folder_id}/index.html"

    # ── 1. PARALLEL DOWNLOAD ──
    log.info("Downloading '%s' (%.1f MB) DC=%d workers=%d",
             filename, total_sz/1_048_576, event.message.media.document.dc_id, WORKERS)
    try:
        await parallel_download(event.message.media, file_path, total_sz, msg, filename)
    except Exception as e:
        log.warning("Parallel download failed: %s — falling back to sequential", e)
        dl_start = time.time()
        dl_last  = 0.0
        done     = 0
        with open(file_path, "wb") as f:
            async for chunk in bot.iter_download(event.message.media, chunk_size=512*1024):
                f.write(chunk)
                done += len(chunk)
                now = time.time()
                if now - dl_last >= PROGRESS_EVERY:
                    try:
                        await msg.edit(progress_bar(done, total_sz, dl_start,
                                                    "📥 Downloading (sequential)...", filename))
                    except Exception:
                        pass
                    dl_last = now

    await msg.edit(f"✅ Download done — {human(total_sz)}\n\n☁️ Uploading to Cloudflare R2...")

    # ── 2. UPLOAD TO R2 ──
    s3      = get_r2()
    up_last = [0.0]
    loop    = asyncio.get_event_loop()

    def up_cb(sent, total, start):
        now = time.time()
        if now - up_last[0] < PROGRESS_EVERY:
            return
        up_last[0] = now
        loop.call_soon_threadsafe(
            lambda s=sent, t=total, st=start: asyncio.ensure_future(
                msg.edit(progress_bar(s, t, st, "☁️ Uploading to Cloudflare R2...", filename))
            )
        )

    try:
        await loop.run_in_executor(
            None, lambda: r2_multipart_upload(s3, file_path, video_key, progress_cb=up_cb)
        )
    finally:
        try:
            os.remove(file_path)
        except OSError:
            pass

    # ── 3. STREAMING PAGE ──
    video_url = f"https://{R2_PUBLIC_DOMAIN}/{video_key}"
    page_url  = f"https://{R2_PUBLIC_DOMAIN}/{page_key}"
    await msg.edit("🎬 Generating streaming page...")
    html = make_streaming_page(video_url, filename, human(total_sz))
    await loop.run_in_executor(
        None, lambda: r2_put(s3, html, page_key, "text/html; charset=utf-8")
    )

    # ── 4. DONE ──
    await msg.edit(
        f"✅ Done!\n\n"
        f"📄 {filename}\n"
        f"📦 {human(total_sz)}\n\n"
        f"🎬 Streaming Page:\n{page_url}\n\n"
        f"🔗 Direct Link:\n`{video_url}`"
    )
    log.info("Done: %s", video_key)

# ─────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    start_health_server()
    print("🤖 Bot Running — Parallel DC-aware Download + R2 Upload")
    bot.run_until_disconnected()
