import os
import time
import asyncio
import logging
import aiohttp

from telethon import TelegramClient, events
from FastTelethon import download_file
from dotenv import load_dotenv

# Health check server (Koyeb free tier ke liye)
from health_check import start_health_server

# ================== ENV ==================
load_dotenv()

API_ID    = int(os.getenv("API_ID"))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

BUNNY_LIBRARY_ID   = os.getenv("BUNNY_LIBRARY_ID")
BUNNY_API_KEY      = os.getenv("BUNNY_API_KEY")
BUNNY_CDN_HOSTNAME = os.getenv("BUNNY_CDN_HOSTNAME")

BUNNY_BASE = "https://video.bunnycdn.com"

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
#  Bunny Stream video status codes
# ─────────────────────────────────────────────
BUNNY_STATUS = {
    0: "Created",
    1: "Uploaded",
    2: "Processing",
    3: "Transcoding",
    4: "Finished",
    5: "Error",
    6: "Upload Failed",
}

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


def _headers(extra: dict = None) -> dict:
    h = {"AccessKey": BUNNY_API_KEY, "accept": "application/json"}
    if extra:
        h.update(extra)
    return h


# ─────────────────────────────────────────────
#  BUNNY STREAM HELPERS
# ─────────────────────────────────────────────

class BunnyError(Exception):
    pass


async def bunny_create_video(session: aiohttp.ClientSession, title: str) -> str:
    url = f"{BUNNY_BASE}/library/{BUNNY_LIBRARY_ID}/videos"
    async with session.post(
        url,
        headers=_headers({"content-type": "application/json"}),
        json={"title": title},
    ) as r:
        data = await r.json(content_type=None)

    if r.status not in (200, 201) or "guid" not in data:
        raise BunnyError(f"create_video failed [{r.status}]: {data}")

    log.info("Bunny video created: guid=%s", data["guid"])
    return data["guid"]


async def bunny_upload_video(
    session: aiohttp.ClientSession,
    guid: str,
    file_path: str,
    progress_cb=None,
) -> None:
    file_size = os.path.getsize(file_path)
    url = f"{BUNNY_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{guid}"
    CHUNK = 1024 * 1024  # 1 MB
    sent  = 0
    start = time.time()

    async def chunked_reader():
        nonlocal sent
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    break
                sent += len(chunk)
                if progress_cb:
                    progress_cb(sent, file_size, start)
                yield chunk

    async with session.put(
        url,
        headers=_headers({"content-type": "application/octet-stream"}),
        data=chunked_reader(),
        chunked=True,
    ) as r:
        resp_text = await r.text()

    if r.status not in (200, 201):
        raise BunnyError(f"upload_video failed [{r.status}]: {resp_text}")

    log.info("Bunny upload complete: guid=%s", guid)


async def bunny_get_video(session: aiohttp.ClientSession, guid: str) -> dict:
    url = f"{BUNNY_BASE}/library/{BUNNY_LIBRARY_ID}/videos/{guid}"
    async with session.get(url, headers=_headers()) as r:
        data = await r.json(content_type=None)
    if r.status != 200:
        raise BunnyError(f"get_video failed [{r.status}]: {data}")
    return data


async def bunny_wait_for_encoding(
    session: aiohttp.ClientSession,
    guid: str,
    progress_cb=None,
    poll_interval: float = 10.0,
) -> dict:
    while True:
        data    = await bunny_get_video(session, guid)
        status  = data.get("status", 0)
        percent = data.get("encodeProgress", 0)
        label   = BUNNY_STATUS.get(status, "Unknown")

        log.info("Bunny status=%s (%s) percent=%d%%", status, label, percent)

        if progress_cb:
            await progress_cb(percent, label)

        if status == 4:
            return data
        elif status in (5, 6):
            raise BunnyError(f"Bunny encoding failed: status={label}")

        await asyncio.sleep(poll_interval)


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

    async with aiohttp.ClientSession() as session:

        # ──────────── 1. DOWNLOAD FROM TELEGRAM (FastTelethon) ────────────
        dl_start = time.time()
        dl_last  = 0.0

        def dl_progress(current, total):
            nonlocal dl_last
            if time.time() - dl_last < 10:
                return
            asyncio.get_event_loop().call_soon_threadsafe(
                lambda c=current, t=total: asyncio.ensure_future(
                    msg.edit(progress_bar(c, t, dl_start, "📥 Downloading...", filename))
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

        await msg.edit(f"✅ Download done — {human(total_sz)}\n\n⏳ Creating video on Bunny Stream...")

        try:
            # ──────────── 2. CREATE VIDEO OBJECT ────────────
            guid = await bunny_create_video(session, filename)
            await msg.edit(
                f"📋 Video object created\n"
                f"🆔 GUID: `{guid}`\n\n"
                f"☁️ Uploading to Bunny Stream..."
            )

            # ──────────── 3. UPLOAD RAW BINARY ────────────
            up_last = [0.0]

            def up_progress(sent, total, start):
                now = time.time()
                if now - up_last[0] < 10:
                    return
                up_last[0] = now
                asyncio.get_event_loop().call_soon_threadsafe(
                    lambda s=sent, t=total, st=start: asyncio.ensure_future(
                        msg.edit(progress_bar(s, t, st, "☁️ Uploading to Bunny Stream...", filename))
                    )
                )

            await bunny_upload_video(session, guid, file_path, up_progress)

        finally:
            # ──────────── DELETE LOCAL FILE IMMEDIATELY ────────────
            try:
                os.remove(file_path)
                log.info("Local file deleted: %s", file_path)
            except OSError:
                pass

        await msg.edit(
            f"🎬 Transcoding on Bunny Stream...\n\n"
            f"📄 {filename}\n"
            f"🆔 `{guid}`\n\n"
            f"[░░░░░░░░░░░░░░░░░░░░] 0%"
        )

        # ──────────── 4. POLL UNTIL ENCODING DONE ────────────
        tc_start = time.time()
        tc_last  = 0.0

        async def tc_progress(percent: int, status_label: str):
            nonlocal tc_last
            if time.time() - tc_last < 10:
                return
            tc_last = time.time()
            filled  = int(20 * percent / 100)
            bar     = "█" * filled + "░" * (20 - filled)
            elapsed = int(time.time() - tc_start)
            try:
                await msg.edit(
                    f"🎬 Transcoding on Bunny Stream...\n\n"
                    f"📄 File    : {filename}\n"
                    f"📦 Size    : {human(total_sz)}\n"
                    f"⏱ Elapsed : {elapsed}s\n"
                    f"🔄 Status  : {status_label}\n\n"
                    f"[{bar}] {percent}%"
                )
            except Exception:
                pass

        await bunny_wait_for_encoding(session, guid, tc_progress, poll_interval=10.0)

    # ──────────── 5. DONE ────────────
    hls_url   = f"https://{BUNNY_CDN_HOSTNAME}/{guid}/playlist.m3u8"
    embed_url = f"https://iframe.mediadelivery.net/embed/{BUNNY_LIBRARY_ID}/{guid}"

    await msg.edit(
        f"✅ Done!\n\n"
        f"📄 {filename}\n"
        f"📦 {human(total_sz)}\n"
        f"🆔 `{guid}`\n\n"
        f"🔗 HLS Stream:\n`{hls_url}`\n\n"
        f"🖥 Embed Player:\n{embed_url}"
    )
    log.info("Job complete: guid=%s", guid)


# ─────────────────────────────────────────────
#  RUN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    # Koyeb health-check server start karo (background thread mein)
    start_health_server()
    print("🤖 Bot Running (Bunny Stream + FastTelethon mode)...")
    bot.run_until_disconnected()
