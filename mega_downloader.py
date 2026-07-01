"""
MEGA → R2 Downloader Module
============================
bot.py isko import karta hai. Yeh module khud Telegram calls nahi karta —
sirf ek run_mega_download() function expose karta hai jisko bot.py
background thread mein chalata hai, progress callback ke through updates deta hai.

IMPORTANT — MEGA Bandwidth Limit:
  MEGA free account ko daily ~3GB bandwidth milta hai. Jab limit hit hoti hai,
  MEGA HTTP 509 deta hai. Is module mein 509 aate hi download turant ruk jata hai
  aur 6 ghante (BANDWIDTH_COOLDOWN_SECONDS) wait karke khud-ba-khud retry karta hai.

Progress updates sirf 25% milestones pe bhejta hai (callback ke through) —
taaki Telegram ko baar baar request na bheji jaye.
"""

import os
import re
import time
import json
import base64
import struct
import logging
import tempfile
from pathlib import Path
from threading import Lock

import requests
import boto3
from boto3.s3.transfer import TransferConfig

def progress_bar(pct: int, width: int = 10) -> str:
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


from Crypto.Cipher import AES
from Crypto.Util import Counter

logger = logging.getLogger("mega_downloader")

# =====================================================
# CONFIG (env se aayega, bot.py inject karega ya yahan load karega)
# =====================================================

def _env(key, default=None, required=False):
    val = os.getenv(key, "").strip()
    if not val:
        if required:
            raise SystemExit(f"ERROR: '{key}' not set in environment")
        return default
    return val


CF_ACCOUNT_ID  = _env("CF_ACCOUNT_ID", required=True)
R2_ACCESS_KEY  = _env("R2_ACCESS_KEY", required=True)
R2_SECRET_KEY  = _env("R2_SECRET_KEY", required=True)
R2_BUCKET_NAME = _env("R2_BUCKET_NAME", required=True)

MEGA_TARGET_PREFIX = _env("MEGA_TARGET_PREFIX", "mega-uploads/")
if not MEGA_TARGET_PREFIX.endswith("/"):
    MEGA_TARGET_PREFIX += "/"

# Retry / bandwidth config
API_RETRY_MAX            = int(_env("MEGA_API_RETRY_MAX", "6"))
API_RETRY_WAIT           = int(_env("MEGA_API_RETRY_WAIT", "10"))
API_RETRY_FACTOR         = 2

BANDWIDTH_COOLDOWN_SECONDS = int(_env("MEGA_BANDWIDTH_COOLDOWN_SECONDS", str(6 * 3600)))  # 6 hours
INTER_FILE_DELAY         = int(_env("MEGA_INTER_FILE_DELAY", "2"))

PROGRESS_MILESTONE_PCT   = 25  # har 25% pe update

# =====================================================
# R2 CLIENT
# =====================================================

_s3 = boto3.client(
    service_name="s3",
    endpoint_url=f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
)

_transfer_config = TransferConfig(
    multipart_threshold=25 * 1024 * 1024,
    multipart_chunksize=25 * 1024 * 1024,
    max_concurrency=10,
    use_threads=True,
)

# =====================================================
# CONTROL FLAGS (bot.py state se set honge)
# =====================================================

control = {
    "cancel": False,   # True karne se download safely ruk jayega
}

# =====================================================
# MEGA CRYPTO HELPERS
# =====================================================

def _b64_decode(s):
    s = s.replace('-', '+').replace('_', '/')
    pad = len(s) % 4
    if pad:
        s += '=' * (4 - pad)
    return base64.b64decode(s)


def _b64_to_a32(s):
    b = _b64_decode(s)
    rem = len(b) % 4
    if rem:
        b += b'\x00' * (4 - rem)
    return struct.unpack(f'>{len(b) // 4}I', b)


def _a32_to_bytes(a):
    return struct.pack(f'>{len(a)}I', *a)


def _decrypt_key(enc_a32, master_a32):
    cipher = AES.new(_a32_to_bytes(master_a32), AES.MODE_ECB)
    enc_b = _a32_to_bytes(enc_a32)
    dec = b''.join(cipher.decrypt(enc_b[i:i + 16]) for i in range(0, len(enc_b), 16))
    return struct.unpack(f'>{len(dec) // 4}I', dec)


def _decrypt_attrs(attrs_b64, key_a32):
    key_b = _a32_to_bytes(key_a32[:4])
    enc = _b64_decode(attrs_b64)
    rem = len(enc) % 16
    if rem:
        enc += b'\x00' * (16 - rem)
    cipher = AES.new(key_b, AES.MODE_CBC, iv=b'\x00' * 16)
    dec = cipher.decrypt(enc).decode('utf-8', errors='ignore').rstrip('\x00')
    try:
        start = dec.find('{')
        end = dec.rfind('}') + 1
        if start >= 0 and end > start:
            return json.loads(dec[start:end])
    except Exception:
        pass
    return {}


def _file_key_and_iv(node_key_a32):
    k = (
        node_key_a32[0] ^ node_key_a32[4],
        node_key_a32[1] ^ node_key_a32[5],
        node_key_a32[2] ^ node_key_a32[6],
        node_key_a32[3] ^ node_key_a32[7],
    )
    iv = (node_key_a32[4], node_key_a32[5], 0, 0)
    return k, iv


def _try_decrypt_node_key(k_field, folder_key_a32, node_type, attrs_b64):
    for entry in k_field.split("/"):
        if ":" not in entry:
            continue
        _, enc_b64 = entry.split(":", 1)
        try:
            enc_a32 = _b64_to_a32(enc_b64)
            node_key = _decrypt_key(enc_a32, folder_key_a32)
            attr_key = _file_key_and_iv(node_key)[0] if node_type == 0 else node_key[:4]
            attrs = _decrypt_attrs(attrs_b64, attr_key)
            if attrs.get("n"):
                return node_key, attrs
        except Exception:
            continue
    return None, {}

# =====================================================
# MEGA API
# =====================================================

MEGA_API = "https://g.api.mega.co.nz/cs"
_seq = 0
_RETRYABLE_HTTP = {500, 502, 503, 504}


class BandwidthLimitError(Exception):
    """MEGA ne 509 diya — daily bandwidth khatam."""
    pass


def _api(payload, folder_id=None):
    global _seq
    params = {"id": _seq}
    _seq += 1
    if folder_id:
        params["n"] = folder_id
    if not isinstance(payload, list):
        payload = [payload]

    wait = API_RETRY_WAIT
    for attempt in range(1, API_RETRY_MAX + 1):
        try:
            resp = requests.post(MEGA_API, params=params, data=json.dumps(payload), timeout=30)
            resp.raise_for_status()
            result = resp.json()
            return result[0] if isinstance(result, list) else result

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code == 509:
                raise BandwidthLimitError("MEGA bandwidth limit hit (509)")
            if code in _RETRYABLE_HTTP and attempt < API_RETRY_MAX:
                logger.warning(f"MEGA API {code} — waiting {wait}s...")
                time.sleep(wait)
                wait *= API_RETRY_FACTOR
            else:
                raise

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt < API_RETRY_MAX:
                logger.warning(f"MEGA API connection error — waiting {wait}s...")
                time.sleep(wait)
                wait *= API_RETRY_FACTOR
            else:
                raise

    raise RuntimeError(f"MEGA API failed after {API_RETRY_MAX} attempts")


def _get_download_url(node_id, folder_id):
    result = _api({"a": "g", "g": 1, "n": node_id}, folder_id=folder_id)
    if isinstance(result, int) or "g" not in result:
        raise RuntimeError(f"MEGA API error for node {node_id}: {result}")
    return result["g"], result.get("s", 0)

# =====================================================
# DOWNLOAD + DECRYPT
# =====================================================

def _stream_download(dl_url, filepath, file_key_a32, iv_a32, filesize):
    key_b = _a32_to_bytes(file_key_a32)
    iv_b = _a32_to_bytes(iv_a32)
    ctr = Counter.new(128, initial_value=int.from_bytes(iv_b, 'big'))
    cipher = AES.new(key_b, AES.MODE_CTR, counter=ctr)

    resp = requests.get(dl_url, stream=True, timeout=120)

    if resp.status_code == 509:
        raise BandwidthLimitError("MEGA bandwidth limit hit (509) during download")

    resp.raise_for_status()

    with open(filepath, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(cipher.decrypt(chunk))


def _download_decrypt(node_id, folder_id, file_key_a32, iv_a32, dest_path, filename):
    safe_name = re.sub(r'[\\/*?:"<>|]', '_', filename)
    filepath = os.path.join(dest_path, safe_name)

    dl_url, filesize = _get_download_url(node_id, folder_id)
    _stream_download(dl_url, filepath, file_key_a32, iv_a32, filesize)

    size_mb = round(os.path.getsize(filepath) / 1024 / 1024, 2)
    logger.info(f"Downloaded: {safe_name} ({size_mb} MB)")
    return filepath

# =====================================================
# UPLOAD TO R2
# =====================================================

def _upload_to_r2(local_file, r2_key):
    logger.info(f"Uploading -> {r2_key}")
    _s3.upload_file(local_file, R2_BUCKET_NAME, r2_key, Config=_transfer_config)
    logger.info(f"Uploaded: {r2_key}")

# =====================================================
# LINK PARSING
# =====================================================

def _parse_link(link):
    m = re.search(r'/folder/([^#\s]+)#([^\s/]+)', link)
    if m:
        return 'folder', m.group(1), m.group(2)
    m = re.search(r'/file/([^#\s]+)#([^\s/]+)', link)
    if m:
        return 'file', m.group(1), m.group(2)
    raise ValueError(f"Cannot parse MEGA link: {link}")

# =====================================================
# FOLDER TREE
# =====================================================

def _fetch_folder_nodes(folder_id):
    result = _api({"a": "f", "c": 1, "r": 1}, folder_id=folder_id)
    if isinstance(result, int):
        raise RuntimeError(f"MEGA folder API error: {result}")
    return result.get("f", [])


def _build_tree(raw_nodes, folder_key_a32):
    tree = {}
    for node in raw_nodes:
        t = node.get("t")
        if t not in (0, 1):
            continue
        k_field = node.get("k", "")
        attrs_b64 = node.get("a", "")
        if not k_field or not attrs_b64:
            continue
        node_key, attrs = _try_decrypt_node_key(k_field, folder_key_a32, t, attrs_b64)
        if node_key is None:
            continue
        tree[node["h"]] = {
            "h": node["h"], "p": node.get("p"), "t": t,
            "name": attrs.get("n", node["h"]), "key": node_key,
        }
    return tree


def _collect_files(tree, parent_id, prefix=""):
    results = []
    for h, node in tree.items():
        if node["p"] != parent_id:
            continue
        if node["t"] == 0:
            results.append((h, prefix + node["name"]))
        elif node["t"] == 1:
            results.extend(_collect_files(tree, h, prefix + node["name"] + "/"))
    return results

# =====================================================
# PROGRESS HELPER (R2 mein already uploaded files check)
# =====================================================

def _r2_object_exists(r2_key):
    try:
        _s3.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        return True
    except Exception:
        return False

# =====================================================
# MAIN DOWNLOAD RUNNER
# =====================================================

def run_mega_download(mega_link: str, live):
    """
    mega_link : MEGA folder/file link
    live      : LiveMessage object from bot.py (has update_sync + send_final_sync)
    """
    control["cancel"] = False

    def _upd(text, **kw):
        live.update_sync(text)

    try:
        link_type, link_id, link_key_str = _parse_link(mega_link)
    except ValueError as e:
        live.send_final_sync(f"❌ Invalid MEGA link: {e}")
        return {"success": 0, "failed": 0, "error": str(e)}

    temp_dir = tempfile.mkdtemp()

    if link_type == "file":
        return _run_single_file(link_id, link_key_str, temp_dir, live)

    return _run_folder(link_id, link_key_str, temp_dir, live)


def _wait_for_bandwidth_reset(live):
    hours = BANDWIDTH_COOLDOWN_SECONDS // 3600
    live.update_sync(
        f"⛔ *MEGA Bandwidth Limit Hit*\n\n"
        f"Daily 3GB limit khatam ho gaya.\n"
        f"⏳ {hours} ghante baad automatically retry hoga.\n\n"
        f"Cancel: /cancel\\_mega"
    )

    waited = 0
    check_every = 30  # har 30s mein cancel check karo
    while waited < BANDWIDTH_COOLDOWN_SECONDS:
        if control["cancel"]:
            live.send_final_sync("🛑 Bandwidth wait cancel ho gaya.")
            return False
        time.sleep(check_every)
        waited += check_every

    live.update_sync("🔄 Cooldown khatam — download retry kar rahe hain...")
    return True


def _run_folder(folder_id, folder_key_str, temp_dir, live):
    live.update_sync("⬇️ *MEGA Download*\n\nFolder scan ho raha hai...")

    while True:
        try:
            raw_nodes = _fetch_folder_nodes(folder_id)
            break
        except BandwidthLimitError:
            if not _wait_for_bandwidth_reset(live):
                return {"success": 0, "failed": 0, "cancelled": True}
        except Exception as e:
            live.send_final_sync(f"❌ Folder fetch error: {e}")
            return {"success": 0, "failed": 1, "error": str(e)}

    if not raw_nodes:
        live.send_final_sync("❌ Koi files nahi mili — link invalid ya private hai.")
        return {"success": 0, "failed": 1}

    folder_key = _b64_to_a32(folder_key_str)
    tree = _build_tree(raw_nodes, folder_key)

    root_id = None
    for h, node in tree.items():
        if node["t"] == 1 and node["p"] not in tree:
            root_id = h
            break

    if not root_id:
        live.send_final_sync("❌ Root folder node nahi mila.")
        return {"success": 0, "failed": 1}

    all_files = _collect_files(tree, root_id)
    total = len(all_files)

    if total == 0:
        live.send_final_sync("⚠️ Folder mein koi file nahi hai.")
        return {"success": 0, "failed": 0}

    live.update_sync(
        f"⬇️ *MEGA Download*\n\n"
        f"Folder : `{tree[root_id]['name']}`\n"
        f"Files  : {total}\n\n"
        f"Shuru ho raha hai..."
    )

    success = failed = skipped = 0
    last_milestone = -1  # taaki 0% pe bhi trigger na ho duplicate

    for idx, (node_handle, rel_path) in enumerate(all_files, 1):

        if control["cancel"]:
            live.send_final_sync(
                f"🛑 *MEGA Download Cancelled*\n\n"
                f"Done     : {success}\n"
                f"Failed   : {failed}\n"
                f"Remaining: {total - idx + 1}"
            )
            return {"success": success, "failed": failed, "cancelled": True}

        node = tree[node_handle]
        r2_key = MEGA_TARGET_PREFIX + rel_path

        # Already uploaded? skip (resume support)
        if _r2_object_exists(r2_key):
            skipped += 1
            continue

        retried_this_file = False

        while True:
            try:
                file_key, iv = _file_key_and_iv(node["key"])
                local_path = _download_decrypt(
                    node_handle, folder_id, file_key, iv, temp_dir, node["name"]
                )
                _upload_to_r2(local_path, r2_key)
                os.remove(local_path)
                success += 1

                if INTER_FILE_DELAY > 0:
                    time.sleep(INTER_FILE_DELAY)
                break

            except BandwidthLimitError:
                if retried_this_file:
                    # double safety — agar wait ke baad bhi turant fir 509 aaye
                    time.sleep(5)
                retried_this_file = True
                if not _wait_for_bandwidth_reset(live):
                    return {"success": success, "failed": failed, "cancelled": True}
                # retry same file (loop continues)

            except Exception as e:
                failed += 1
                logger.error(f"MEGA file failed [{rel_path}]: {e}")
                break

        # 25% milestone progress update
        pct = int((idx / total) * 100)
        milestone = (pct // PROGRESS_MILESTONE_PCT) * PROGRESS_MILESTONE_PCT
        if milestone > last_milestone and milestone > 0:
            last_milestone = milestone
            live.update_sync(
                f"⬇️ *Downloading MEGA*\n\n"
                f"`{progress_bar(milestone)}` {milestone}%\n"
                f"[{idx}/{total}]\n\n"
                f"✅ Done    : {success}\n"
                f"❌ Failed  : {failed}\n"
                f"⏭ Skipped : {skipped}"
            )

    live.send_final_sync(
        f"✅ *MEGA Download Complete!*\n\n"
        f"Success : {success}\n"
        f"Failed  : {failed}\n"
        f"Skipped : {skipped} (already in R2)\n\n"
        f"R2 path: `{MEGA_TARGET_PREFIX}`"
    )
    return {"success": success, "failed": failed, "skipped": skipped}


def _run_single_file(file_id, file_key_str, temp_dir, live):
    live.update_sync("⬇️ *MEGA Download*\n\nFile info fetch ho raha hai...")

    while True:
        try:
            result = _api({"a": "g", "g": 1, "p": file_id})
            if isinstance(result, int) or "g" not in result:
                raise RuntimeError(f"MEGA API error: {result}")
            break
        except BandwidthLimitError:
            if not _wait_for_bandwidth_reset(live):
                return {"success": 0, "failed": 0, "cancelled": True}
        except Exception as e:
            live.send_final_sync(f"❌ File fetch error: {e}")
            return {"success": 0, "failed": 1, "error": str(e)}

    dl_url = result["g"]
    filesize = result.get("s", 0)
    attrs_b64 = result.get("at", "")

    node_key = _b64_to_a32(file_key_str)
    file_key, iv = _file_key_and_iv(node_key)
    attrs = _decrypt_attrs(attrs_b64, file_key) if attrs_b64 else {}
    filename = attrs.get("n", file_id)
    safe_name = re.sub(r'[\\/*?:"<>|]', '_', filename)
    filepath = os.path.join(temp_dir, safe_name)
    r2_key = MEGA_TARGET_PREFIX + filename

    if _r2_object_exists(r2_key):
        live.send_final_sync("⏭ File already R2 mein maujood hai. Skip.")
        return {"success": 0, "failed": 0, "skipped": 1}

    live.update_sync(f"⬇️ *MEGA Download*\n\n`{filename}`\n\nDownloading...")

    while True:
        try:
            _stream_download(dl_url, filepath, file_key, iv, filesize)
            break
        except BandwidthLimitError:
            if not _wait_for_bandwidth_reset(live):
                return {"success": 0, "failed": 0, "cancelled": True}
            # dl_url shayad expire ho gaya ho, dobara fetch karo
            try:
                result = _api({"a": "g", "g": 1, "p": file_id})
                dl_url = result["g"]
            except BandwidthLimitError:
                continue
        except Exception as e:
            live.send_final_sync(f"❌ Download failed: {e}")
            return {"success": 0, "failed": 1, "error": str(e)}

    try:
        _upload_to_r2(filepath, r2_key)
        os.remove(filepath)
    except Exception as e:
        live.send_final_sync(f"❌ Upload failed: {e}")
        return {"success": 0, "failed": 1, "error": str(e)}

    live.send_final_sync(
        f"✅ *MEGA File Complete!*\n\n"
        f"File  : `{filename}`\n"
        f"R2 key: `{r2_key}`"
    )
    return {"success": 1, "failed": 0}
