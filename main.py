import os
import asyncio
import subprocess
import json
import shutil
from telethon import TelegramClient, events
import boto3
from dotenv import load_dotenv

# ================== LOAD ENV ==================
load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")

PUBLIC_DOMAIN = os.getenv("PUBLIC_DOMAIN")

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
HLS_DIR = os.getenv("HLS_DIR", "hls")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(HLS_DIR, exist_ok=True)

# ================== TELEGRAM ==================
client = TelegramClient("bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ================== R2 CLIENT ==================
s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

# ================== DOWNLOAD ==================
async def fast_download(event, file_path):
    msg = event.message

    file = await msg.download_media(
        file=file_path,
        progress_callback=lambda d, t: print(f"Downloading: {d*100/t:.2f}%")
    )
    return file

# ================== RESOLUTION ==================
def get_resolution(file):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=height",
        "-of", "json", file
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    return data["streams"][0]["height"]

# ================== HLS GENERATION ==================
def generate_hls(input_file, output_dir, height):
    os.makedirs(output_dir, exist_ok=True)

    if height >= 1080:
        renditions = [1080, 720, 480]
    elif height >= 720:
        renditions = [720, 480]
    else:
        renditions = [480]

    playlist_entries = []

    for res in renditions:
        out_m3u8 = f"{output_dir}/{res}p.m3u8"

        cmd = [
            "ffmpeg", "-y",
            "-i", input_file,
            "-vf", f"scale=-2:{res}",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "22",
            "-c:a", "aac",
            "-ar", "48000",
            "-b:a", "128k",
            "-g", "48",
            "-keyint_min", "48",
            "-sc_threshold", "0",
            "-hls_time", "6",
            "-hls_playlist_type", "vod",
            "-hls_segment_filename", f"{output_dir}/{res}p_%03d.ts",
            out_m3u8
        ]

        subprocess.run(cmd)
        playlist_entries.append((res, f"{res}p.m3u8"))

    # MASTER PLAYLIST
    master_path = f"{output_dir}/master.m3u8"
    with open(master_path, "w") as f:
        f.write("#EXTM3U\n")
        for res, path in playlist_entries:
            bandwidth = res * 1000000
            f.write(f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION=1280x{res}\n")
            f.write(f"{path}\n")

# ================== UPLOAD ==================
def upload_folder(folder, prefix):
    for root, dirs, files in os.walk(folder):
        for file in files:
            full_path = os.path.join(root, file)
            key = f"{prefix}/{file}"

            content_type = (
                "application/vnd.apple.mpegurl"
                if file.endswith(".m3u8")
                else "video/mp2t"
            )

            s3.upload_file(
                full_path,
                R2_BUCKET,
                key,
                ExtraArgs={"ContentType": content_type}
            )

# ================== CLEANUP ==================
def cleanup(file_path, folder):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(folder):
            shutil.rmtree(folder)
    except Exception as e:
        print("Cleanup error:", e)

# ================== HANDLER ==================
@client.on(events.NewMessage(incoming=True))
async def handler(event):
    if not event.video:
        return

    status = await event.reply("📥 Downloading...")

    try:
        file_path = os.path.join(DOWNLOAD_DIR, f"{event.id}.mp4")
        file = await fast_download(event, file_path)

        await status.edit("⚙️ Processing video...")

        height = get_resolution(file)
        output_dir = os.path.join(HLS_DIR, str(event.id))

        generate_hls(file, output_dir, height)

        await status.edit("☁️ Uploading to Cloud...")

        upload_folder(output_dir, str(event.id))

        link = f"{PUBLIC_DOMAIN}/{event.id}/master.m3u8"

        await status.edit(f"✅ Stream Ready:\n\n🔗 {link}")

    except Exception as e:
        await status.edit(f"❌ Error:\n{str(e)}")

    finally:
        cleanup(file_path, output_dir)

# ================== START ==================
print("🚀 Bot Started with ENV support...")
client.run_until_disconnected()
