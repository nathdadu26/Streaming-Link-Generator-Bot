import os
import asyncio
import subprocess
import shutil

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
import boto3
from dotenv import load_dotenv

# 🔑 Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

DOWNLOAD_DIR = "downloads"
HLS_DIR = "hls"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(HLS_DIR, exist_ok=True)

# 📥 FAST TELEGRAM DOWNLOAD (supports up to 4GB safely)
async def download_file(file_path, dest):
    file = await bot.get_file(file_path)
    await bot.download_file(file.file_path, dest, chunk_size=64 * 1024)  # stream in chunks

# 🎥 GET VIDEO RESOLUTION
def get_resolution(input_file):
    cmd = f'ffprobe -v error -select_streams v:0 -show_entries stream=height -of csv=p=0 "{input_file}"'
    result = subprocess.getoutput(cmd)
    return int(result.strip())

# 🎬 HLS TRANSCODE
def transcode_hls(input_file, output_dir, height):
    os.makedirs(output_dir, exist_ok=True)

    renditions = []
    if height >= 1080:
        renditions = [1080, 720, 480]
    elif height >= 720:
        renditions = [720, 480]
    else:
        renditions = [480]

    master_playlist = os.path.join(output_dir, "master.m3u8")
    streams = ""

    for r in renditions:
        out = f"{output_dir}/{r}p.m3u8"
        cmd = f"""
        ffmpeg -y -i "{input_file}" \
        -vf scale=-2:{r} -c:a aac -ar 48000 -c:v h264 \
        -preset veryfast -crf 23 \
        -hls_time 6 -hls_playlist_type vod \
        -hls_segment_filename "{output_dir}/{r}p_%03d.ts" \
        "{out}"
        """
        subprocess.run(cmd, shell=True)
        streams += f'#EXT-X-STREAM-INF:BANDWIDTH={r*1000},RESOLUTION=1280x{r}\n{r}p.m3u8\n'

    with open(master_playlist, "w") as f:
        f.write("#EXTM3U\n" + streams)

    return master_playlist

# ☁️ UPLOAD TO R2 (async for speed)
async def upload_folder(folder, prefix):
    urls = []
    tasks = []
    for root, _, files in os.walk(folder):
        for file in files:
            path = os.path.join(root, file)
            key = f"{prefix}/{file}"
            tasks.append(asyncio.to_thread(s3.upload_file, path, R2_BUCKET, key))
            urls.append(f"{R2_PUBLIC_URL}/{key}")
    await asyncio.gather(*tasks)
    return urls

# 🤖 HANDLE VIDEO
@dp.message(lambda message: message.video)
async def handle_video(message: Message):
    file_id = message.video.file_id
    file = await bot.get_file(file_id)
    file_path = os.path.join(DOWNLOAD_DIR, f"{file_id}.mp4")

    await message.reply("📥 Downloading...")
    await download_file(file.file_path, file_path)

    await message.reply("🔍 Detecting quality...")
    height = get_resolution(file_path)

    output_dir = os.path.join(HLS_DIR, file_id)
    await message.reply("🎬 Transcoding to HLS...")
    master = transcode_hls(file_path, output_dir, height)

    await message.reply("☁️ Uploading to R2...")
    await upload_folder(output_dir, file_id)

    link = f"{R2_PUBLIC_URL}/{file_id}/master.m3u8"
    await message.reply(f"✅ Done!\n🎥 Stream Link:\n{link}")

    # cleanup
    os.remove(file_path)
    shutil.rmtree(output_dir, ignore_errors=True)

# 🚀 START
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
