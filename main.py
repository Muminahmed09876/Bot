#!/usr/bin/env python3
import os
import re
import aiohttp
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message, BotCommand, InlineKeyboardMarkup, InlineKeyboardButton
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
import subprocess
import traceback
from flask import Flask
import time
import math
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "5000"))

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# state
USER_THUMBS = {}
LAST_FILE = {}
TASKS = {}
SET_THUMB_REQUEST = set()
SUBSCRIBERS = set()
USER_CAPTION_TEMPLATES = {}  # New: to store user-specific templates
USER_COUNTERS = {}  # New: to store user-specific counters for dynamic captions

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
MAX_SIZE = 2 * 1024 * 1024 * 2048

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

# ---- utilities ----
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url or "docs.google.com" in url

def extract_drive_id(url: str) -> str:
    patterns = [
        r"/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"open\?id=([a-zA-Z0-9_-]+)",
        r"https://drive.google.com/file/d/([a-zA-Z0-9_-]+)/"
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

def get_video_duration(file_path: Path) -> int:
    try:
        parser = createParser(str(file_path))
        if not parser:
            return 0
        with parser:
            metadata = extractMetadata(parser)
        if metadata and metadata.has("duration"):
            return int(metadata.get("duration").total_seconds())
    except Exception:
        return 0
    return 0

def progress_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel_task")]])

# ---- progress callback helpers (removed live progress) ----
async def progress_callback(current, total, message: Message, start_time, task="Progress"):
    pass

def pyrogram_progress_wrapper(current, total, message_obj, start_time_obj, task_str="Progress"):
    pass

# ---- robust download stream with retries ----
async def download_stream(resp, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    total = 0
    try:
        size = int(resp.headers.get("Content-Length", 0))
    except:
        size = 0
    chunk_size = 1024 * 1024
    try:
        with out_path.open("wb") as f:
            async for chunk in resp.content.iter_chunked(chunk_size):
                if cancel_event and cancel_event.is_set():
                    return False, "অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।"
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_SIZE:
                    return False, "ফাইলের সাইজ 2GB এর বেশি হতে পারে না।"
                f.write(chunk)
    except Exception as e:
        return False, str(e)
    return True, None

async def fetch_with_retries(session, url, method="GET", max_tries=3, **kwargs):
    backoff = 1
    for attempt in range(1, max_tries + 1):
        try:
            resp = await session.request(method, url, **kwargs)
            return resp
        except Exception as e:
            if attempt == max_tries:
                raise
            await asyncio.sleep(backoff)
            backoff *= 2
    raise RuntimeError("unreachable")

async def download_url_generic(url: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                return await download_stream(resp, out_path, message, cancel_event=cancel_event)
        except Exception as e:
            return False, str(e)

async def download_drive_file(file_id: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    base = f"https://drive.google.com/uc?export=download&id={file_id}"
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(base, allow_redirects=True) as resp:
                if resp.status == 200 and "content-disposition" in (k.lower() for k in resp.headers.keys()):
                    return await download_stream(resp, out_path, message, cancel_event=cancel_event)
                text = await resp.text(errors="ignore")
                m = re.search(r"confirm=([0-9A-Za-z-_]+)", text)
                if m:
                    token = m.group(1)
                    download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                    async with sess.get(download_url, allow_redirects=True) as resp2:
                        if resp2.status != 200:
                            return False, f"HTTP {resp2.status}"
                        return await download_stream(resp2, out_path, message, cancel_event=cancel_event)
                for k, v in resp.cookies.items():
                    if k.startswith("download_warning"):
                        token = v.value
                        download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                        async with sess.get(download_url, allow_redirects=True) as resp2:
                            if resp2.status != 200:
                                return False, f"HTTP {resp2.status}"
                            return await download_stream(resp2, out_path, message, cancel_event=cancel_event)
                return False, "ডাউনলোডের জন্য Google Drive থেকে অনুমতি প্রয়োজন বা লিংক পাবলিক নয়।"
        except Exception as e:
            return False, str(e)

async def set_bot_commands():
    cmds = [
        BotCommand("start", "বট চালু/হেল্প"),
        BotCommand("upload_url", "URL থেকে ফাইল ডাউনলোড ও আপলোড (admin only)"),
        BotCommand("setthumb", "কাস্টম থাম্বনেইল সেট করুন (admin only)"),
        BotCommand("view_thumb", "আপনার থাম্বনেইল দেখুন (admin only)"),
        BotCommand("del_thumb", "আপনার থাম্বনেইল মুছে ফেলুন (admin only)"),
        BotCommand("rename", "reply করা ভিডিও রিনেম করুন (admin only)"),
        BotCommand("set_caption_template", "ডাইনামিক ক্যাপশন টেমপ্লেট সেট করুন (admin only)"),
        BotCommand("view_caption", "বর্তমান ক্যাপশন টেমপ্লেট দেখুন (admin only)"),
        BotCommand("clear_caption_template", "ক্যাপশন টেমপ্লেট মুছে ফেলুন (admin only)"),
        BotCommand("broadcast", "ব্রডকাস্ট (কেবল অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
    except Exception as e:
        logger.warning("Set commands error: %s", e)

# ---- New: Dynamic caption generation utility ----
def generate_dynamic_caption(uid, original_caption):
    if uid not in USER_CAPTION_TEMPLATES:
        return original_caption

    template = USER_CAPTION_TEMPLATES[uid]
    counters = USER_COUNTERS.setdefault(uid, {"+1": 0, "repite": -1})
    
    final_caption = template
    
    # Process the {+1} logic
    re_plus1 = re.compile(r"\{ *\+1 *\( *(\d+) *up\) *\}")
    match_plus1 = re_plus1.search(final_caption)
    if match_plus1:
        up_count = int(match_plus1.group(1))
        
        # Check if the counter needs to be incremented based on the up_count
        if counters["+1"] % up_count == 0:
            if "last_episode" not in counters:
                counters["last_episode"] = 1
            else:
                counters["last_episode"] += 1
        
        episode_number = counters.get("last_episode", 1)
        final_caption = final_caption.replace(match_plus1.group(0), str(episode_number).zfill(2))
        
    # Process the {repite} logic
    re_repite = re.compile(r"\{ *repite *\(([^)]+)\) *\}")
    match_repite = re_repite.search(final_caption)
    if match_repite:
        options = [opt.strip() for opt in match_repite.group(1).split(',')]
        counters["repite"] = (counters["repite"] + 1) % len(options)
        index = counters["repite"]
        final_caption = final_caption.replace(match_repite.group(0), options[index])
    
    # After all replacements, increment the main counter for the next video
    counters["+1"] += 1 
    
    return final_caption

# ---- handlers ----
@app.on_message(filters.command("start") & filters.private)
async def start_handler(c, m: Message):
    await set_bot_commands()
    SUBSCRIBERS.add(m.chat.id)
    text = (
        "Hi! আমি URL uploader bot.\n\n"
        "নোট: বটের অনেক কমান্ড শুধু অ্যাডমিন (owner) চালাতে পারবে।\n\n"
        "Commands:\n"
        "/upload_url <url> - URL থেকে ডাউনলোড ও Telegram-এ আপলোড (admin only)\n"
        "/setthumb - একটি ছবি পাঠান, সেট হবে আপনার থাম্বনেইল (admin only)\n"
        "/view_thumb - আপনার থাম্বনেইল দেখুন (admin only)\n"
        "/del_thumb - আপনার থাম্বনেইল মুছে ফেলুন (admin only)\n"
        "/rename <newname.ext> - reply করা ভিডিও রিনেম করুন (admin only)\n"
        "/set_caption_template - ডাইনামিক ক্যাপশন টেমপ্লেট সেট করুন (admin only)\n"
        "/view_caption - বর্তমান ক্যাপশন টেমপ্লেট দেখুন (admin only)\n"
        "/clear_caption_template - ক্যাপশন টেমপ্লেট মুছে ফেলুন (admin only)\n"
        "/broadcast <text> - ব্রডকাস্ট (কেবল অ্যাডমিন)\n"
        "/help - সাহায্য"
    )
    await m.reply_text(text)

@app.on_message(filters.command("help") & filters.private)
async def help_handler(c, m):
    await start_handler(c, m)

@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_prompt(c, m):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_THUMB_REQUEST.add(m.from_user.id)
    await m.reply_text("একটি ছবি পাঠান (photo) — সেট হবে আপনার থাম্বনেইল।")

@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and Path(thumb_path).exists():
        await c.send_photo(chat_id=m.chat.id, photo=thumb_path, caption="এটা আপনার সেভ করা থাম্বনেইল।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই। /setthumb দিয়ে সেট করুন।")

@app.on_message(filters.command("del_thumb") & filters.private)
async def del_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and Path(thumb_path).exists():
        try:
            Path(thumb_path).unlink()
        except Exception:
            pass
        USER_THUMBS.pop(uid, None)
        await m.reply_text("আপনার থাম্বনেইল মুছে ফেলা হয়েছে।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই।")

@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id
    out = TMP / f"thumb_{uid}.jpg"
    try:
        await m.download(file_name=str(out))
        img = Image.open(out)
        img.thumbnail((320, 320))
        img = img.convert("RGB")
        img.save(out, "JPEG")
        USER_THUMBS[uid] = str(out)
        if uid in SET_THUMB_REQUEST:
            SET_THUMB_REQUEST.discard(uid)
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        else:
            await m.reply_text("অটো থাম্বনেইল সেভ হয়েছে।")
    except Exception as e:
        await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")

@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not m.command or len(m.command) < 2:
        await m.reply_text("ব্যবহার: /upload_url <url> [ক্যাপশন]\nউদাহরণ: /upload_url https://example.com/file.mp4 নতুন ভিডিও ক্যাপশন")
        return
    
    parts = m.text.split(None, 2)
    url = parts[1].strip()
    caption = parts[2] if len(parts) > 2 else None
    
    asyncio.create_task(handle_url_download_and_upload(c, m, url, caption_text=caption))

@app.on_message(filters.text & filters.private)
async def auto_url_upload(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    text = m.text.strip()
    if text.startswith("http://") or text.startswith("https://"):
        url = text.split(" ")[0]
        caption = text.split(" ", 1)[1] if len(text.split(" ")) > 1 else None
        asyncio.create_task(handle_url_download_and_upload(c, m, url, caption_text=caption))

async def handle_url_download_and_upload(c: Client, m: Message, url: str, caption_text: str = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)

    status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    try:
        fname = url.split("/")[-1].split("?")[0] or f"download_{int(datetime.now().timestamp())}"
        safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)

        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        if not any(safe_name.lower().endswith(ext) for ext in video_exts):
            safe_name += ".mp4"

        tmp_in = TMP / f"dl_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
        ok, err = False, None

        if is_drive_url(url):
            fid = extract_drive_id(url)
            if not fid:
                await status_msg.edit("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি। সঠিক লিংক দিন।", reply_markup=None)
                TASKS[uid].remove(cancel_event)
                return
            ok, err = await download_drive_file(fid, tmp_in, status_msg, cancel_event=cancel_event)
        else:
            ok, err = await download_url_generic(url, tmp_in, status_msg, cancel_event=cancel_event)

        if not ok:
            await status_msg.edit(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            try:
                if tmp_in.exists():
                    tmp_in.unlink()
            except:
                pass
            TASKS[uid].remove(cancel_event)
            return

        await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_in, original_name=safe_name, messages_to_delete=[status_msg.id], caption_text=caption_text)
    except Exception as e:
        traceback.print_exc()
        await status_msg.edit(f"অপস! কিছু ভুল হয়েছে: {e}", reply_markup=None)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_message(filters.private & filters.forwarded & (filters.video | filters.document))
async def forwarded_file_rename(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    file_info = m.video or m.document
    
    if not file_info or not file_info.file_name:
        original_name = f"new_file_{int(datetime.now().timestamp())}.mp4"
    else:
        original_name = file_info.file_name

    status_msg = await m.reply_text("ফরওয়ার্ড করা ফাইল ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    tmp_path = TMP / f"forwarded_{uid}_{int(datetime.now().timestamp())}_{original_name}"
    try:
        await m.download(file_name=str(tmp_path))
        await status_msg.edit("ডাউনলোড সম্পন্ন, এখন Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_path, original_name=original_name, messages_to_delete=[status_msg.id])
    except Exception as e:
        await m.reply_text(f"ফাইল প্রসেসিংয়ে সমস্যা: {e}")
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        await m.reply_text("ভিডিও/ডকুমেন্ট ফাইলের reply দিয়ে এই কমান্ড দিন।\nUsage: /rename <new_name.ext> [ক্যাপশন]")
        return
    if len(m.command) < 2:
        await m.reply_text("নতুন ফাইল নাম দিন। উদাহরণ: /rename new_video.mp4")
        return
    
    parts = m.text.split(None, 2)
    new_name = parts[1].strip()
    caption = parts[2] if len(parts) > 2 else None
    
    new_name = re.sub(r"[\\/*?\"<>|:]", "_", new_name)
    await m.reply_text(f"ভিডিও রিনেম করা হবে: {new_name}\n(রিনেম করতে reply করা ফাইলটি পুনরায় ডাউনলোড করে আপলোড করা হবে)")

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
    tmp_out = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{new_name}"
    try:
        await m.reply_to_message.download(file_name=str(tmp_out))
        await status_msg.edit("ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_out, original_name=new_name, messages_to_delete=[status_msg.id], caption_text=caption)
    except Exception as e:
        await m.reply_text(f"রিনেম ত্রুটি: {e}")
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    if uid in TASKS and TASKS[uid]:
        for ev in list(TASKS[uid]):
            try:
                ev.set()
            except:
                pass
        await cb.answer("অপারেশন বাতিল করা হয়েছে।", show_alert=True)
        try:
            await cb.message.delete()
        except Exception:
            pass
    else:
        await cb.answer("কোনো অপারেশন চলছে না।", show_alert=True)

# ---- New Caption Handlers (unchanged) ----
@app.on_message(filters.command("set_caption_template") & filters.private)
async def set_caption_template_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    if len(m.command) < 2:
        await m.reply_text("ক্যাপশন টেমপ্লেট সেট করার জন্য টেক্সট দিন। উদাহরণ:\n`/set_caption_template **Season - 01**, **Episode - {+1 (3 up)}**, **Quality - {repite (480p), (720p), (1080p), (4k)}**`")
        return
    
    template = m.text.split(None, 1)[1].strip()
    USER_CAPTION_TEMPLATES[m.from_user.id] = template
    USER_COUNTERS[m.from_user.id] = {"+1": 0, "repite": -1}
    await m.reply_text("ক্যাপশন টেমপ্লেট সফলভাবে সেভ হয়েছে।")

@app.on_message(filters.command("clear_caption_template") & filters.private)
async def clear_caption_template_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
        
    USER_CAPTION_TEMPLATES.pop(m.from_user.id, None)
    USER_COUNTERS.pop(m.from_user.id, None)
    await m.reply_text("ক্যাপশন টেমপ্লেট মুছে ফেলা হয়েছে।")

@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ডটি ব্যবহার করার।")
        return
    
    uid = m.from_user.id
    if uid in USER_CAPTION_TEMPLATES:
        template = USER_CAPTION_TEMPLATES[uid]
        await m.reply_text(f"আপনার বর্তমান সেভ করা ক্যাপশন টেমপ্লেটটি হলো:\n\n`{template}`")
    else:
        await m.reply_text("আপনার কোনো ক্যাপশন টেমপ্লেট সেভ করা নেই। `/set_caption_template` দিয়ে একটি টেমপ্লেট সেভ করুন।")


# ---- main processing and upload ----
async def generate_video_thumbnail(video_path: Path, thumb_path: Path):
    try:
        duration = get_video_duration(video_path)
        timestamp = 1 if duration > 1 else 0
        cmd = [
            "ffmpeg",
            "-y",
            "-i", str(video_path),
            "-ss", str(timestamp),
            "-vframes", "1",
            "-vf", "scale=320:-1",
            str(thumb_path)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except Exception as e:
        logger.warning("Thumbnail generate error: %s", e)
        return False

async def convert_to_mp4(in_path: Path, out_path: Path, status_msg: Message):
    try:
        await status_msg.edit("ভিডিওটি MP4 ফরম্যাটে কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
        cmd = [
            "ffmpeg",
            "-i", str(in_path),
            "-codec", "copy",
            str(out_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=1200)
        
        if result.returncode != 0:
            logger.warning("Container conversion failed, attempting full re-encoding: %s", result.stderr)
            await status_msg.edit("ভিডিওটি MP4 ফরম্যাটে পুনরায় এনকোড করা হচ্ছে...", reply_markup=progress_keyboard())
            cmd_full = [
                "ffmpeg",
                "-i", str(in_path),
                "-c:v", "libx264",
                "-preset", "fast",
                "-crf", "23",
                "-c:a", "copy",
                str(out_path)
            ]
            result_full = subprocess.run(cmd_full, capture_output=True, text=True, check=False, timeout=3600)
            if result_full.returncode != 0:
                raise Exception(f"Full re-encoding failed: {result_full.stderr}")

        if not out_path.exists() or out_path.stat().st_size == 0:
            raise Exception("Converted file not found or is empty.")
        
        return True, None
    except Exception as e:
        logger.error("Video conversion error: %s", e)
        return False, str(e)


async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None, messages_to_delete: list = None, caption_text: str = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    upload_path = in_path
    
    temp_thumb_path = None

    try:
        final_name = original_name or in_path.name
        
        # Check if a specific caption text was provided
        if caption_text:
            final_caption = caption_text
        else:
            caption_template = USER_CAPTION_TEMPLATES.get(uid, f"**{final_name}**")
            final_caption = generate_dynamic_caption(uid, caption_template)
        
        thumb_path = USER_THUMBS.get(uid)

        is_video = in_path.suffix.lower() in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        
        if is_video and in_path.suffix.lower() != ".mp4":
            mp4_path = TMP / f"{in_path.stem}.mp4"
            status_msg = await m.reply_text(f"ভিডিওটি {in_path.suffix} ফরম্যাটে আছে। MP4 এ কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
            if messages_to_delete:
                messages_to_delete.append(status_msg.id)
            ok, err = await convert_to_mp4(in_path, mp4_path, status_msg)
            if not ok:
                await status_msg.edit(f"কনভার্সন ব্যর্থ: {err}\nমূল ফাইলটি আপলোড করা হচ্ছে...", reply_markup=None)
            else:
                upload_path = mp4_path
                final_name = f"{Path(final_name).stem}.mp4"
                
        if is_video and not thumb_path:
            temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            ok = await generate_video_thumbnail(upload_path, temp_thumb_path)
            if ok:
                thumb_path = str(temp_thumb_path)

        status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        if messages_to_delete:
            messages_to_delete.append(status_msg.id)

        if cancel_event.is_set():
            await status_msg.edit("অপারেশন বাতিল করা হয়েছে, আপলোড শুরু করা হয়নি।", reply_markup=None)
            TASKS[uid].remove(cancel_event)
            return
        
        duration_sec = get_video_duration(upload_path) if upload_path.exists() else 0

        upload_attempts = 3
        last_exc = None
        for attempt in range(1, upload_attempts + 1):
            try:
                if is_video:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=str(upload_path),
                        caption=final_caption,
                        thumb=thumb_path,
                        duration=duration_sec,
                        supports_streaming=True
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=str(upload_path),
                        file_name=final_name,
                        caption=final_caption
                    )
                
                if messages_to_delete:
                    try:
                        await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                    except Exception:
                        pass
                
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                logger.warning("Upload attempt %s failed: %s", attempt, e)
                await asyncio.sleep(2 * attempt)
                if cancel_event.is_set():
                    if messages_to_delete:
                        try:
                            await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                        except Exception:
                            pass
                    break

        if last_exc:
            await m.reply_text(f"আপলোড ব্যর্থ: {last_exc}", reply_markup=None)
    except Exception as e:
        await m.reply_text(f"আপলোডে ত্রুটি: {e}")
    finally:
        try:
            if upload_path != in_path and upload_path.exists():
                upload_path.unlink()
            if in_path.exists():
                in_path.unlink()
            if temp_thumb_path and Path(temp_thumb_path).exists():
                Path(temp_thumb_path).unlink()
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

# *** সংশোধিত: ব্রডকাস্ট কমান্ড ***
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd_no_reply(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message:
        await m.reply_text("ব্রডকাস্ট করতে যেকোনো মেসেজে (ছবি, ভিডিও বা টেক্সট) **রিপ্লাই করে** এই কমান্ড দিন।")
        return

@app.on_message(filters.command("broadcast") & filters.private & filters.reply)
async def broadcast_cmd_reply(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    
    source_message = m.reply_to_message
    if not source_message:
        await m.reply_text("ব্রডকাস্ট করার জন্য একটি মেসেজে রিপ্লাই করে এই কমান্ড দিন।")
        return

    await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে {len(SUBSCRIBERS)} সাবস্ক্রাইবারে...", quote=True)
    failed = 0
    sent = 0
    for chat_id in list(SUBSCRIBERS):
        if chat_id == m.chat.id:
            continue
        try:
            await c.forward_messages(chat_id=chat_id, from_chat_id=source_message.chat.id, message_ids=source_message.id)
            sent += 1
            await asyncio.sleep(0.08)
        except Exception as e:
            failed += 1
            logger.warning("Broadcast to %s failed: %s", chat_id, e)

    await m.reply_text(f"ব্রডকাস্ট শেষ। পাঠানো: {sent}, ব্যর্থ: {failed}")


# Flask route to keep web service port open for Render
@flask_app.route("/")
def home():
    return "Bot is running (Flask alive)."

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)

async def periodic_cleanup():
    while True:
        try:
            now = datetime.now()
            for p in TMP.iterdir():
                try:
                    if p.is_file():
                        if now - datetime.fromtimestamp(p.stat().st_mtime) > timedelta(days=3):
                            p.unlink()
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(3600)

if __name__ == "__main__":
    print("Bot চালু হচ্ছে... Flask thread start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
    except RuntimeError:
        pass
    app.run()
