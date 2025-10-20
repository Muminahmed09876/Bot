#!/usr/bin/env python3
import os
import re
import aiohttp
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message, BotCommand, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
import subprocess
import traceback
import json 
from flask import Flask, render_template_string
import requests
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
# New env var from previous code
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# state
USER_THUMBS = {}
TASKS = {}
SET_THUMB_REQUEST = set()
SUBSCRIBERS = set()
SET_CAPTION_REQUEST = set()
USER_CAPTIONS = {}
# New state for dynamic captions
USER_COUNTERS = {}
# New state for edit caption mode
EDIT_CAPTION_MODE = set()
USER_THUMB_TIME = {}

# --- STATE FOR AUDIO CHANGE ---
MKV_AUDIO_CHANGE_MODE = set()
# Stores the path of the downloaded file waiting for audio order
AUDIO_CHANGE_FILE = {} 
# ------------------------------

# --- NEW STATE FOR CREATE POST MODE ---
CREATE_POST_MODE = set()
# Stores the data for the current post creation session
POST_CREATION_DATA = {}
# --------------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
MAX_SIZE = 4 * 1024 * 1024 * 1024

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

# Helper function for consistent renaming
def generate_new_filename(original_name: str) -> str:
    """Generates the new standardized filename while preserving the original extension."""
    BASE_NEW_NAME = "[@TA_HD_Anime] Telegram Channel"
    file_path = Path(original_name)
    file_ext = file_path.suffix.lower()
    
    # Clean up the extension and ensure it starts with a dot
    file_ext = "." + file_ext.lstrip('.')
    
    # If a file like 'video_id' or 'file_id' comes without a proper extension, default to .mp4
    if not file_ext or file_ext == '.':
        return BASE_NEW_NAME + ".mp4"
        
    return BASE_NEW_NAME + file_ext

# --- NEW UTILITY: Post Filename Generator ---
def generate_post_filename(original_name: str) -> str:
    """Generates the new standardized filename for post images."""
    BASE_NEW_NAME = "[@TA_HD_Anime] Telegram Channel"
    file_path = Path(original_name)
    file_ext = file_path.suffix.lower()
    file_ext = "." + file_ext.lstrip('.')
    # Ensure it's a common image format, default to .jpg if not clear.
    if file_ext not in {'.jpg', '.jpeg', '.png', '.webp', '.gif'}:
        return BASE_NEW_NAME + ".jpg" 
    return BASE_NEW_NAME + file_ext
# --------------------------------------------

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

def parse_time(time_str: str) -> int:
    """Parses a time string like '5s', '1m', '1h 30s' into seconds."""
    total_seconds = 0
    parts = time_str.lower().split()
    for part in parts:
        if part.endswith('s'):
            total_seconds += int(part[:-1])
        elif part.endswith('m'):
            total_seconds += int(part[:-1]) * 60
        elif part.endswith('h'):
            total_seconds += int(part[:-1]) * 3600
    return total_seconds

def progress_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel_task")]])

def delete_caption_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Delete Caption 🗑️", callback_data="delete_caption")]])

# --- NEW UTILITY: Keyboard for Mode Check (Updated with Create Post Mode) ---
def mode_check_keyboard(uid: int) -> InlineKeyboardMarkup:
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    post_status = "✅ ON" if uid in CREATE_POST_MODE else "❌ OFF" # NEW

    # Check if a file is waiting for track order input
    waiting_status = " (অর্ডার বাকি)" if uid in AUDIO_CHANGE_FILE else ""
    post_waiting_status = " (পোস্ট চলছে)" if uid in CREATE_POST_MODE else "" # NEW
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")],
        [InlineKeyboardButton(f"Create Post Mode {post_status}{post_waiting_status}", callback_data="toggle_post_mode")] # NEW BUTTON
    ]
    return InlineKeyboardMarkup(keyboard)
# ---------------------------------------------


# --- NEW UTILITY: FFprobe to get audio tracks ---
def get_audio_tracks_ffprobe(file_path: Path) -> list:
    """Uses ffprobe to get a list of audio streams with their index and title."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            str(file_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        
        audio_tracks = []
        for stream in metadata.get('streams', []):
            if stream.get('codec_type') == 'audio':
                stream_index = stream.get('index') 
                title = stream.get('tags', {}).get('title', 'N/A')
                language = stream.get('tags', {}).get('language', 'und') # 'und' is undefined
                audio_tracks.append({
                    'stream_index': stream_index,
                    'title': title,
                    'language': language
                })
        return audio_tracks
    except Exception as e:
        logger.error(f"FFprobe error: {e}")
        return []
# ---------------------------------------------


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
                if total > MAX_SIZE:
                    return False, "ফাইলের সাইজ 4GB এর বেশি হতে পারে না।"
                total += len(chunk)
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
        BotCommand("set_caption", "কাস্টম ক্যাপশন সেট করুন (admin only)"),
        BotCommand("view_caption", "আপনার ক্যাপশন দেখুন (admin only)"),
        BotCommand("edit_caption_mode", "শুধু ক্যাপশন এডিট করুন (admin only)"),
        BotCommand("rename", "reply করা ভিডিও রিনেম করুন (admin only)"),
        BotCommand("mkv_video_audio_change", "MKV ভিডিওর অডিও ট্র্যাক পরিবর্তন (admin only)"),
        BotCommand("mode_check", "বর্তমান মোড স্ট্যাটাস চেক করুন (admin only)"),
        BotCommand("create_post", "নতুন পোস্ট তৈরি মোড টগল করুন (admin only)"), # NEW COMMAND
        BotCommand("broadcast", "ব্রডকাস্ট (কেবল অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
    except Exception as e:
        logger.warning("Set commands error: %s", e)

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
        "/set_caption - একটি ক্যাপশন সেট করুন (admin only)\n"
        "/view_caption - আপনার ক্যাপশন দেখুন (admin only)\n"
        "/edit_caption_mode - শুধু ক্যাপশন এডিট করার মোড টগল করুন (admin only)\n"
        "/rename <newname.ext> - reply করা ভিডিও রিনেম করুন (admin only)\n"
        "/mkv_video_audio_change - MKV ভিডিওর অডিও ট্র্যাক পরিবর্তন মোড টগল করুন (admin only)\n"
        "/mode_check - বর্তমান মোড স্ট্যাটাস চেক করুন এবং পরিবর্তন করুন (admin only)\n"
        "/create_post - নতুন পোস্ট তৈরি মোড টগল করুন (admin only)\n" # NEW COMMAND in help
        "/broadcast <text> - ব্রডকাস্ট (শুধুমাত্র অ্যাডমিন)\n"
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
    
    uid = m.from_user.id
    if len(m.command) > 1:
        time_str = " ".join(m.command[1:])
        seconds = parse_time(time_str)
        if seconds > 0:
            USER_THUMB_TIME[uid] = seconds
            await m.reply_text(f"থাম্বনেইল তৈরির সময় সেট হয়েছে: {seconds} সেকেন্ড।")
        else:
            await m.reply_text("সঠিক ফরম্যাটে সময় দিন। উদাহরণ: `/setthumb 5s`, `/setthumb 1m`, `/setthumb 1m 30s`")
    else:
        SET_THUMB_REQUEST.add(uid)
        await m.reply_text("একটি ছবি পাঠান (photo) — সেট হবে আপনার থাম্বনেইল।")


@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    thumb_time = USER_THUMB_TIME.get(uid)
    
    if thumb_path and Path(thumb_path).exists():
        await c.send_photo(chat_id=m.chat.id, photo=thumb_path, caption="এটা আপনার সেভ করা থাম্বনেইল।")
    elif thumb_time:
        await m.reply_text(f"আপনার থাম্বনেইল তৈরির সময় সেট করা আছে: {thumb_time} সেকেন্ড।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল বা থাম্বনেইল তৈরির সময় সেভ করা নেই। /setthumb দিয়ে সেট করুন।")

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
    
    if uid in USER_THUMB_TIME:
        USER_THUMB_TIME.pop(uid)

    if not (thumb_path or uid in USER_THUMB_TIME):
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই।")
    else:
        await m.reply_text("আপনার থাম্বনেইল/থাম্বনেইল তৈরির সময় মুছে ফেলা হয়েছে।")


# *** সংশোধিত: Forwarded Photo হ্যান্ডেল করার জন্য Photo Handler ***
@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id

    # --- NEW LOGIC FOR CREATE POST MODE ---
    if uid in CREATE_POST_MODE and POST_CREATION_DATA.get(uid, {}).get('step') == 'wait_for_image':
        
        # FIX: The Pyrogram Photo object does not have file_name. 
        # We construct a safe, unique placeholder name with .jpg extension.
        original_file_name = f"post_image_{m.photo.file_unique_id}.jpg" 
        
        new_name_with_ext = generate_post_filename(original_file_name)
        
        # New: Use the standardized name for the downloaded file path
        # Ensure the path is unique and safe for the file system
        safe_base_name = new_name_with_ext.replace('[','').replace(']','').replace(' ','_').strip('_')
        out = TMP / f"{safe_base_name}_{uid}_{int(datetime.now().timestamp())}.jpg"
        
        try:
            # 1. Download the photo (handles both direct and forwarded photos)
            await m.download(file_name=str(out))
            
            # 2. Convert to JPEG and resize for consistent post image
            img = Image.open(out)
            img.thumbnail((1280, 1280)) # Resize image to max 1280 on the longest side
            img = img.convert("RGB")
            img.save(out, "JPEG")
            
            # 3. Update session data
            POST_CREATION_DATA[uid]['image_path'] = str(out)
            # Use a placeholder name to start
            POST_CREATION_DATA[uid]['image_name'] = "Image name" 
            POST_CREATION_DATA[uid]['delete_messages'].append(m.id)
            POST_CREATION_DATA[uid]['step'] = 'wait_for_image_name_change'
            
            await m.reply_text(
                f"ছবিটি সেভ হয়েছে। ফাইলের নাম পরিবর্তন হবে: `{new_name_with_ext}`।\n"
                "এখন ক্যাপশনের মধ্যে **\"Image name\"** এর জায়গায় কী নাম চান? (উদাহরণ: **My Awesome Title**)"
            )
            return
        except Exception as e:
            logger.error(f"Post image save error: {e}")
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_DATA.pop(uid, None)
            await m.reply_text(f"ছবি সেভ করতে সমস্যা: {e}")
            return
    # ----------------------------------------
    
    if uid in SET_THUMB_REQUEST:
        SET_THUMB_REQUEST.discard(uid)
        out = TMP / f"thumb_{uid}.jpg"
        try:
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((320, 320))
            img = img.convert("RGB")
            img.save(out, "JPEG")
            USER_THUMBS[uid] = str(out)
            # Make sure to clear the time setting if a photo is set
            USER_THUMB_TIME.pop(uid, None)
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        except Exception as e:
            await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")
    else:
        pass
# *** শেষ সংশোধিত Photo Handler ***


# Handlers for caption
@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_CAPTION_REQUEST.add(m.from_user.id)
    # Reset counter data when a new caption is about to be set
    USER_COUNTERS.pop(m.from_user.id, None)
    
    await m.reply_text(
        "ক্যাপশন দিন। এখন আপনি এই কোডগুলো ব্যবহার করতে পারবেন:\n"
        "1. **নম্বর বৃদ্ধি:** `[01]`, `[(01)]` (নম্বর স্বয়ংক্রিয়ভাবে বাড়বে)\n"
        "2. **গুণমানের সাইকেল:** `[re (480p, 720p)]`\n"
        "3. **শর্তসাপেক্ষ টেক্সট (নতুন):** `[TEXT (XX)]` - যেমন: `[End (02)]`, `[hi (05)]` (যদি বর্তমান পর্বের নম্বর `XX` এর **সমান** হয়, তাহলে `TEXT` যোগ হবে)।"
    )

@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    caption = USER_CAPTIONS.get(uid)
    if caption:
        await m.reply_text(f"আপনার সেভ করা ক্যাপশন:\n\n`{caption}`", reply_markup=delete_caption_keyboard())
    else:
        await m.reply_text("আপনার কোনো ক্যাপশন সেভ করা নেই। /set_caption দিয়ে সেট করুন।")

@app.on_callback_query(filters.regex("delete_caption"))
async def delete_caption_cb(c, cb):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return
    if uid in USER_CAPTIONS:
        USER_CAPTIONS.pop(uid)
        USER_COUNTERS.pop(uid, None) # New: delete counter data
        await cb.message.edit_text("আপনার ক্যাপশন মুছে ফেলা হয়েছে।")
    else:
        await cb.answer("আপনার কোনো ক্যাপশন সেভ করা নেই।", show_alert=True)

# Handler to toggle edit caption mode
@app.on_message(filters.command("edit_caption_mode") & filters.private)
async def toggle_edit_caption_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in EDIT_CAPTION_MODE:
        EDIT_CAPTION_MODE.discard(uid)
        await m.reply_text("edit video caption mod **OFF**.\nএখন থেকে আপলোড করা ভিডিওর রিনেম ও থাম্বনেইল পরিবর্তন হবে, এবং সেভ করা ক্যাপশন যুক্ত হবে।")
    else:
        EDIT_CAPTION_MODE.add(uid)
        await m.reply_text("edit video caption mod **ON**.\nএখন থেকে শুধু সেভ করা ক্যাপশন ভিডিওতে যুক্ত হবে। ভিডিওর নাম এবং থাম্বনেইল একই থাকবে।")

# --- HANDLER: /mkv_video_audio_change ---
@app.on_message(filters.command("mkv_video_audio_change") & filters.private)
async def toggle_audio_change_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in MKV_AUDIO_CHANGE_MODE:
        MKV_AUDIO_CHANGE_MODE.discard(uid)
        # Clean up any pending file path
        if uid in AUDIO_CHANGE_FILE:
            try:
                Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
                if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                    await c.delete_messages(m.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
            except Exception:
                pass
            AUDIO_CHANGE_FILE.pop(uid, None)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অফ** করা হয়েছে।")
    else:
        MKV_AUDIO_CHANGE_MODE.add(uid)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অন** করা হয়েছে।\nঅনুগ্রহ করে **MKV ফাইল** অথবা অন্য কোনো **ভিডিও ফাইল** পাঠান।\n(এই মোড ম্যানুয়ালি অফ না করা পর্যন্ত চালু থাকবে।)")

# --- HANDLER: /create_post ---
@app.on_message(filters.command("create_post") & filters.private)
async def toggle_create_post_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in CREATE_POST_MODE:
        # Toggling OFF: Cleanup
        CREATE_POST_MODE.discard(uid)
        if uid in POST_CREATION_DATA:
            try:
                img_path = POST_CREATION_DATA[uid].get('image_path')
                if img_path and Path(img_path).exists(): 
                    Path(img_path).unlink(missing_ok=True)
            except Exception: pass
            
            # Delete auxiliary messages if they exist
            if 'delete_messages' in POST_CREATION_DATA[uid]:
                try:
                    # Collect message IDs to delete, including the original command message
                    all_ids = list(POST_CREATION_DATA[uid]['delete_messages']) + [m.id]
                    await c.delete_messages(m.chat.id, all_ids)
                except Exception:
                    pass
            
            POST_CREATION_DATA.pop(uid, None)
            await m.reply_text("পোস্ট তৈরি মোড **অফ** করা হয়েছে।")
    else:
        # Toggling ON: Initialize
        CREATE_POST_MODE.add(uid)
        POST_CREATION_DATA[uid] = {
            'step': 'wait_for_image',
            'image_path': None,
            'image_name': None,
            'genres': None,
            'seasons': None,
            'delete_messages': [m.id] # Track the command message
        }
        await m.reply_text("পোস্ট তৈরি মোড **অন** করা হয়েছে। এখন **ছবি (photo)** পাঠান।")
# -----------------------------

# --- NEW HANDLER: /mode_check (Updated with Create Post Mode) ---
@app.on_message(filters.command("mode_check") & filters.private)
async def mode_check_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    post_status = "✅ ON" if uid in CREATE_POST_MODE else "❌ OFF" # NEW
    
    waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"
    
    post_step_text = "কোনো পোস্ট তৈরি চলছে না।"
    if uid in CREATE_POST_MODE:
        step = POST_CREATION_DATA.get(uid, {}).get('step', 'wait_for_image')
        if step == 'wait_for_image':
             post_step_text = "ইমেজ আপলোডের জন্য অপেক্ষা করছে।"
        else:
             post_step_text = f"স্টেপ: {step}"
    
    status_text = (
        "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
        f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
        f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
        f"2. **Edit Caption Mode:** `{caption_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
        f"3. **Create Post Mode:** `{post_status}`\n"
        f"   - *কাজ:* কাস্টম ক্যাপশন সহ ইমেজ পোস্ট তৈরি করে।\n"
        f"   - *স্ট্যাটাস:* {post_step_text}\n\n"
        "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
    )
    
    await m.reply_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)

# --- NEW CALLBACK: Mode Toggle Buttons (Updated with Create Post Mode) ---
@app.on_callback_query(filters.regex("toggle_(audio|caption|post)_mode"))
async def mode_toggle_callback(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return

    action = cb.data
    
    if action == "toggle_audio_mode":
        if uid in MKV_AUDIO_CHANGE_MODE:
            # Turning OFF: Clear mode and cleanup pending file
            MKV_AUDIO_CHANGE_MODE.discard(uid)
            if uid in AUDIO_CHANGE_FILE:
                try:
                    Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
                    if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                        await c.delete_messages(cb.message.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
                except Exception:
                    pass
                AUDIO_CHANGE_FILE.pop(uid, None)
            message = "MKV Audio Change Mode OFF."
        else:
            # Turning ON
            MKV_AUDIO_CHANGE_MODE.add(uid)
            message = "MKV Audio Change Mode ON."
            
    elif action == "toggle_caption_mode":
        if uid in EDIT_CAPTION_MODE:
            EDIT_CAPTION_MODE.discard(uid)
            message = "Edit Caption Mode OFF."
        else:
            EDIT_CAPTION_MODE.add(uid)
            message = "Edit Caption Mode ON."
            
    elif action == "toggle_post_mode": # NEW LOGIC
        if uid in CREATE_POST_MODE:
            CREATE_POST_MODE.discard(uid)
            if uid in POST_CREATION_DATA:
                try:
                    # Clean up file
                    img_path = POST_CREATION_DATA[uid].get('image_path')
                    if img_path and Path(img_path).exists(): Path(img_path).unlink(missing_ok=True)
                    # Delete auxiliary messages
                    if 'delete_messages' in POST_CREATION_DATA[uid]:
                        await c.delete_messages(cb.message.chat.id, POST_CREATION_DATA[uid]['delete_messages']) 
                except Exception: pass
                POST_CREATION_DATA.pop(uid, None)
            message = "Create Post Mode OFF."
        else:
            CREATE_POST_MODE.add(uid)
            POST_CREATION_DATA[uid] = {'step': 'wait_for_image', 'image_path': None, 'image_name': None, 'genres': None, 'seasons': None, 'delete_messages': []}
            message = "Create Post Mode ON. (একটি ছবি পাঠান)"

            
    # Refresh the keyboard and edit the original message (similar to mode_check_cmd)
    try:
        audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
        caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
        post_status = "✅ ON" if uid in CREATE_POST_MODE else "❌ OFF" 

        waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"
        
        post_step_text = "কোনো পোস্ট তৈরি চলছে না।"
        if uid in CREATE_POST_MODE:
            step = POST_CREATION_DATA.get(uid, {}).get('step', 'wait_for_image')
            if step == 'wait_for_image':
                 post_step_text = "ইমেজ আপলোডের জন্য অপেক্ষা করছে।"
            else:
                 post_step_text = f"স্টেপ: {step}"
        
        status_text = (
            "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
            f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
            f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
            f"2. **Edit Caption Mode:** `{caption_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
            f"3. **Create Post Mode:** `{post_status}`\n" 
            f"   - *কাজ:* কাস্টম ক্যাপশন সহ ইমেজ পোস্ট তৈরি করে।\n"
            f"   - *স্ট্যাটাস:* {post_step_text}\n\n" 
            "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
        )
        
        await cb.message.edit_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)
        await cb.answer(message, show_alert=True)
    except Exception as e:
        logger.error(f"Callback edit error: {e}")
        await cb.answer(message, show_alert=True)


@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    text = m.text.strip()
    
    # Handle set caption request
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None) # New: reset counter on new caption set
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # --- NEW LOGIC FOR CREATE POST MODE STEPS ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_DATA:
        await handle_post_creation_steps(c, m, uid, text)
        return
    # --------------------------------------------

    # --- Handle audio order input if in mode and file is set ---
    if uid in MKV_AUDIO_CHANGE_MODE and uid in AUDIO_CHANGE_FILE:
        file_data = AUDIO_CHANGE_FILE.get(uid)
        if not file_data or not file_data.get('tracks'):
            await m.reply_text("অডিও ট্র্যাকের তথ্য পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হচ্ছে।")
            # MKV_AUDIO_CHANGE_MODE.discard(uid) # <--- REMOVED: Keep mode ON
            AUDIO_CHANGE_FILE.pop(uid, None)
            return

        tracks = file_data['tracks']
        try:
            # Parse the input like "3,2,1"
            new_order_str = [x.strip() for x in text.split(',')]
            
            # Validation: Check if the number of tracks matches and they are valid indices
            if len(new_order_str) != len(tracks):
                 await m.reply_text(f"আপনার ইনপুট করা ট্র্যাকের সংখ্যা ({len(new_order_str)}) এবং ফাইলের অডিও ট্র্যাকের সংখ্যা ({len(tracks)}) মিলছে না। সঠিক অর্ডারে কমা-সেপারেটেড সংখ্যা দিন।")
                 return
            
            new_stream_map = []
            valid_user_indices = list(range(1, len(tracks) + 1))
            
            for user_track_num_str in new_order_str:
                user_track_num = int(user_track_num_str)
                if user_track_num not in valid_user_indices:
                     await m.reply_text(f"ভুল ট্র্যাক নম্বর: {user_track_num}। ট্র্যাক নম্বরগুলো হতে হবে: {', '.join(map(str, valid_user_indices))}")
                     return
                
                stream_index_to_map = tracks[user_track_num - 1]['stream_index']
                new_stream_map.append(f"0:{stream_index_to_map}") 

            track_list_message_id = file_data.get('message_id')

            # Start the audio remux process
            asyncio.create_task(
                handle_audio_remux(
                    c, m, file_data['path'], 
                    file_data['original_name'], 
                    new_stream_map, 
                    messages_to_delete=[track_list_message_id, m.id]
                )
            )

            # Clear state immediately
            # MKV_AUDIO_CHANGE_MODE.discard(uid) # <--- REMOVED: Keep mode ON
            AUDIO_CHANGE_FILE.pop(uid, None) # Clear only the waiting file state
            return
        except ValueError:
            await m.reply_text("ভুল ফরম্যাট। কমা-সেপারেটেড সংখ্যা দিন। উদাহরণ: `3,2,1`")
            return
        except Exception as e:
            logger.error(f"Audio remux preparation error: {e}")
            await m.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া শুরু করতে সমস্যা: {e}")
            # MKV_AUDIO_CHANGE_MODE.discard(uid) # <--- REMOVED: Keep mode ON
            AUDIO_CHANGE_FILE.pop(uid, None)
            return
    # -----------------------------------------------------

    # Handle auto URL upload
    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))

@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not m.command or len(m.command) < 2:
        await m.reply_text("ব্যবহার: /upload_url <url>\nউদাহরণ: /upload_url https://example.com/file.mp4")
        return
    url = m.text.split(None, 1)[1].strip()
    asyncio.create_task(handle_url_download_and_upload(c, m, url))

# --- HANDLER FUNCTION: Handle post creation steps ---
async def handle_post_creation_steps(c: Client, m: Message, uid: int, text: str):
    data = POST_CREATION_DATA[uid]
    # Add user message ID to the list for cleanup
    data['delete_messages'].append(m.id)
    step = data.get('step')

    if step == 'wait_for_image_name_change':
        # Name change step
        data['image_name'] = text.strip()
        data['step'] = 'wait_for_genres'
        await m.reply_text("এখন **Genres -** এর পরে কী কী জেনার যোগ করবেন? (উদাহরণ: **Comedy, Romance**)")
    elif step == 'wait_for_genres':
        # Genres step
        data['genres'] = text.strip()
        data['step'] = 'wait_for_seasons'
        await m.reply_text(
            "এখন **Season List** এর জন্য সিজন রেঞ্জ দিন।\n"
            "ফরম্যাট: `1` (**Season 01**), `1-2` (**Season 01** থেকে **Season 02**), `1-2 4-5` (**Season 01-02** and **Season 04-05**)।"
        )
    elif step == 'wait_for_seasons':
        # Seasons step
        data['seasons'] = text.strip()
        data['step'] = 'finish'
        # Final step: Generate Caption and Upload
        await finalize_post_upload(c, m, uid)
    else:
        await m.reply_text("পোস্ট তৈরির প্রক্রিয়ায় সমস্যা হয়েছে। মোড অফ করা হচ্ছে।")
        CREATE_POST_MODE.discard(uid)
        POST_CREATION_DATA.pop(uid, None)

# --- FINALIZER FUNCTION: Assemble caption and upload ---
async def finalize_post_upload(c: Client, m: Message, uid: int):
    data = POST_CREATION_DATA[uid]
    image_path = Path(data['image_path'])
    image_name = data['image_name'] # The customizable name
    genres_text = data['genres']
    seasons_text = data['seasons']

    # 1. Build the Seasons List
    seasons_list = []
    if seasons_text:
        # Regex to parse ranges like '1', '1-2', '4-5'
        ranges = re.findall(r'(\d+)(?:-(\d+))?', seasons_text)
        for start_match, end_match in ranges:
            try:
                start_num = int(start_match)
                end_num = int(end_match) if end_match else start_num
                # Ensure start <= end
                if start_num > end_num:
                    start_num, end_num = end_num, start_num
                for i in range(start_num, end_num + 1):
                    # Season number formatted as 01, 02, etc.
                    seasons_list.append(f"**\"{image_name}\" Season {i:02d}**")
            except ValueError:
                continue

    # 2. Build the Collapsed Season List String
    seasons_block = ""
    if seasons_list:
        # Add "Coming Soon..."
        seasons_list.append("**Coming Soon...**")
        seasons_content = "\n".join(seasons_list)
        # Combining Quote (>) and Spoiler (||text||) for collapse
        seasons_block = (
            f"> **\"{image_name}\" All Season List :-**\n"
            f"> ||\n"
            f"{seasons_content}\n"
            f"> ||"
        )

    # 3. Build the Main Caption
    # Genres text is also bold
    genres_line = f"‣ Genres - **{genres_text}**" if genres_text and genres_text.strip() else "‣ Genres - **Coming Soon**"
    # Main content is also bold
    caption = (
        f"**{image_name}**\n"
        "**────────────────────**\n"
        "**‣ Audio - Hindi Official**\n"
        "**‣ Quality - 480p, 720p, 1080p**\n"
        f"**{genres_line}**\n"
        "**────────────────────**\n"
    )

    # Append the seasons block if it exists
    if seasons_block:
        caption += "\n" + seasons_block

    try:
        # 4. Upload the Photo
        # final_file_name = image_path.name # এই লাইনটি থাকলেও সমস্যা নেই কারণ এটি ব্যবহার করা হচ্ছে না।
        await c.send_photo(
            chat_id=m.chat.id,
            photo=str(image_path),
            caption=caption,
            # file_name=final_file_name, # <-- এটিই সমস্যার কারণ ছিল, তাই মুছে ফেলা হলো।
            parse_mode=ParseMode.MARKDOWN # Use Markdown for **bold**, >quote, and ||spoiler||
        )

        # 5. Delete all auxiliary messages
        if data.get('delete_messages'):
            all_messages_to_delete = data['delete_messages']
            try:
                # Add the reply message ID to the list
                last_reply = m.reply_to_message.id if m.reply_to_message else None
                if last_reply not in all_messages_to_delete and last_reply:
                    all_messages_to_delete.append(last_reply)

                # Get the message ID of the photo itself to delete it from the chat
                # (assuming the upload was successful and returned a Message object,
                # which in a real Pyrogram environment would be a variable holding the result of send_photo)
                # Since we cannot modify the bot's flow here, we skip deleting the sent post. 
                
                # Delete auxiliary messages
                await c.delete_messages(m.chat.id, all_messages_to_delete)
            except Exception as e:
                logger.warning(f"Failed to delete auxiliary messages: {e}")
        
        # 6. Cleanup state and temporary file
        try:
            image_path.unlink(missing_ok=True)
        except Exception:
            pass
        CREATE_POST_MODE.discard(uid)
        POST_CREATION_DATA.pop(uid, None)
        logger.info(f"Post creation finalized for user {uid}.")

    except Exception as e:
        logger.error(f"Post upload failed for user {uid}: {traceback.format_exc()}")
        await m.reply_text(f"পোস্ট আপলোড ব্যর্থ: {e}")
        # Cleanup on failure
        try:
            if image_path and image_path.exists():
                image_path.unlink(missing_ok=True)
        except Exception:
            pass
        CREATE_POST_MODE.discard(uid)
        POST_CREATION_DATA.pop(uid, None)
        
# --- MKV Audio Remuxing Logic ---
async def handle_audio_remux(c: Client, m: Message, file_path: str, original_name: str, new_stream_map: list, messages_to_delete: list):
    """
    Handles the MKV audio stream reordering using FFmpeg and uploads the result.
    """
    uid = m.from_user.id
    
    status_msg = None
    try:
        status_msg = await m.reply_text("অডিও স্ট্রিম অর্ডার পরিবর্তন করা হচ্ছে... ⏳")
        
        input_file = Path(file_path)
        new_name = generate_new_filename(original_name)
        output_file = TMP / f"remuxed_{input_file.name}"
        
        # Build the FFmpeg command
        # Select all streams from the input file
        stream_map_args = ["-map", "0"] 
        
        # Add the audio stream reordering map (e.g., -map 0:v -map 0:a:3 -map 0:a:2 -map 0:a:1)
        # However, the current logic is to map them directly, so we just use the custom map logic
        # For simplicity and to only map the required tracks in the new order, let's use the full map:
        
        # -map 0:v -map 0:s:0 ... (map video and first subtitle stream)
        # We need to explicitly map the video, any subtitles, and then the reordered audio
        
        # 1. Get all stream indices (video, subtitle, audio) from original file
        cmd_probe = [
            "ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", str(input_file)
        ]
        result_probe = subprocess.run(cmd_probe, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result_probe.stdout)
        
        map_args = []
        
        # Map all streams, but then specifically remap audio in the custom order
        map_args.append("-map")
        map_args.append("0:v:0") # Map first video stream
        
        # Map reordered audio streams
        for stream_map in new_stream_map:
            # stream_map is like "0:3", where 3 is the original stream index
            map_args.append("-map")
            map_args.append(stream_map) 

        # Map all subtitle streams
        for stream in metadata.get('streams', []):
            if stream.get('codec_type') == 'subtitle':
                map_args.append("-map")
                map_args.append(f"0:{stream.get('index')}")
                
        # Map any remaining non-video, non-audio, non-subtitle streams (e.g., attachments)
        for stream in metadata.get('streams', []):
            codec_type = stream.get('codec_type')
            if codec_type not in ['video', 'audio', 'subtitle']:
                map_args.append("-map")
                map_args.append(f"0:{stream.get('index')}")

        ffmpeg_cmd = [
            "ffmpeg",
            "-i", str(input_file),
            "-c", "copy",
            *map_args, # Unpack the custom stream map arguments
            "-y", # Overwrite output files without asking
            str(output_file)
        ]

        logger.info(f"FFmpeg Remux Command: {' '.join(ffmpeg_cmd)}")

        # Execute FFmpeg command
        process = subprocess.Popen(ffmpeg_cmd, stderr=subprocess.PIPE)
        _, stderr_output = process.communicate(timeout=3600) # Wait up to 1 hour
        
        if process.returncode != 0:
            error_message = stderr_output.decode('utf-8', errors='ignore')
            raise RuntimeError(f"FFmpeg failed with code {process.returncode}. Error: {error_message[-1000:]}")
            
        # 5. Delete input file and update file_path
        try:
            input_file.unlink(missing_ok=True)
        except Exception:
            pass
            
        file_path = str(output_file)
        
        await status_msg.edit_text("অডিও স্ট্রিম অর্ডার পরিবর্তন সফল। আপলোড শুরু হচ্ছে... 🚀")
        
        # Proceed to Telegram upload logic
        await process_file_and_upload(c, status_msg, file_path, new_name, uid, is_remuxed=True, remux_msg_id=status_msg.id)
        
    except Exception as e:
        logger.error(f"MKV Remux failed for user {uid}: {traceback.format_exc()}")
        error_text = f"MKV অডিও পরিবর্তন ও আপলোড ব্যর্থ:\n`{e}`"
        if status_msg:
            await status_msg.edit_text(error_text)
        else:
            await m.reply_text(error_text)
    finally:
        # Final cleanup for the state and all messages
        # if uid in MKV_AUDIO_CHANGE_MODE: MKV_AUDIO_CHANGE_MODE.discard(uid) # Keep mode ON
        if uid in AUDIO_CHANGE_FILE: AUDIO_CHANGE_FILE.pop(uid)
        
        # Delete auxiliary messages
        if messages_to_delete:
            try:
                if status_msg:
                    messages_to_delete.append(status_msg.id)
                await c.delete_messages(m.chat.id, list(set(messages_to_delete))) # Use set to handle duplicates
            except Exception:
                pass
        
        # Delete the final file if it still exists after failed upload
        try:
            if 'output_file' in locals() and output_file.exists():
                output_file.unlink(missing_ok=True)
            if 'input_file' in locals() and input_file.exists():
                input_file.unlink(missing_ok=True)
        except Exception:
            pass
# ---------------------------------


# --- Main UPLOAD / RENAME / FORWARD Handler (Updated with MKV Audio Change) ---
@app.on_message(filters.document | filters.video | filters.audio & filters.private)
async def process_file_and_upload_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    # 1. Check for Create Post Mode (Ignore documents/videos if post mode is active)
    if uid in CREATE_POST_MODE:
        # Acknowledge and ignore the file input
        await m.reply_text("পোস্ট তৈরি মোড (Create Post Mode) চালু আছে। এই ফাইলটি উপেক্ষা করা হচ্ছে। মোড অফ করে আবার চেষ্টা করুন।")
        return

    # 2. Check for Rename Command
    if m.reply_to_message and m.text and m.text.startswith("/rename"):
        if not (m.reply_to_message.document or m.reply_to_message.video):
            await m.reply_text("রিনেম করার জন্য একটি ডকুমেন্ট বা ভিডিও মেসেজে রিপ্লাই করুন।")
            return
        
        if len(m.command) < 2:
            await m.reply_text("নতুন নাম দিন। উদাহরণ: `/rename New_Name.mp4`")
            return
            
        new_name_with_ext = m.text.split(None, 1)[1].strip()
        
        # Ensure the file/video object is available in the replied message
        file_obj = m.reply_to_message.document or m.reply_to_message.video
        if not file_obj or not file_obj.file_id:
             await m.reply_text("রিপ্লাই করা মেসেজটিতে কোনো ফাইল নেই।")
             return

        # Start the rename process (which involves downloading and re-uploading)
        status_msg = await m.reply_text("ফাইল ডাউনলোড শুরু হচ্ছে... 📥")
        asyncio.create_task(
            handle_file_download_and_upload(
                c, m.reply_to_message, new_name_with_ext, uid, status_msg.id, is_rename=True
            )
        )
        return
        
    # 3. Check for MKV Audio Change Mode
    if uid in MKV_AUDIO_CHANGE_MODE:
        file_obj = m.document or m.video
        if not file_obj or not file_obj.file_id:
            await m.reply_text("অডিও পরিবর্তন মোডের জন্য একটি ভিডিও বা ডকুমেন্ট ফাইল প্রয়োজন।")
            return
            
        if file_obj.file_size > 2 * 1024 * 1024 * 1024: # 2GB limit for remux
             await m.reply_text("রিমাক্সের জন্য ফাইলের সাইজ 2GB এর নিচে হতে হবে।")
             # MKV_AUDIO_CHANGE_MODE.discard(uid) # Keep mode ON
             return

        # Start download for audio track check
        status_msg = await m.reply_text("ফাইল ডাউনলোড শুরু হচ্ছে... (অডিও ট্র্যাক চেক করার জন্য) 📥")
        asyncio.create_task(
            handle_file_download_for_audio_check(
                c, m, uid, status_msg.id
            )
        )
        return
        
    # 4. Standard Forward/Upload (Default Flow)
    # The default flow handles forwarded files (without any command) or files sent by the admin directly.
    # The file is handled by handle_file_download_and_upload with original name.
    
    file_obj = m.document or m.video or m.audio
    if file_obj and file_obj.file_name:
        original_name = file_obj.file_name
        
        # Start download/upload with standard renaming
        status_msg = await m.reply_text("ফাইল ডাউনলোড শুরু হচ্ছে... 📥")
        asyncio.create_task(
            handle_file_download_and_upload(
                c, m, original_name, uid, status_msg.id, is_rename=False
            )
        )
    else:
        # Handle the case where a file without a name is forwarded or sent
        await m.reply_text("এই ফাইলটি প্রক্রিয়াকরণের জন্য উপযুক্ত নয় বা ফাইলের নাম পাওয়া যায়নি।")


# --- Main Logic for Download and Upload (Standard/Rename) ---
async def handle_file_download_and_upload(c: Client, m: Message, original_name: str, uid: int, status_msg_id: int, is_rename: bool):
    
    status_msg = await c.get_messages(m.chat.id, status_msg_id)
    file_obj = m.document or m.video or m.audio
    
    if is_rename:
        new_name = original_name # New name is passed as original_name when is_rename=True
        # Preserve original extension if new name doesn't have one (though typically it should)
        if not Path(new_name).suffix:
            new_name += Path(file_obj.file_name).suffix
    else:
        # Standard flow, use the bot's standard naming convention
        if uid in EDIT_CAPTION_MODE:
             # If EDIT_CAPTION_MODE is ON, keep original name
             new_name = original_name 
        else:
             # Otherwise, use bot's custom renaming utility
             new_name = generate_new_filename(original_name)
    
    # Create a unique temporary path for the file
    out = TMP / f"{file_obj.file_id}_{uid}_{int(datetime.now().timestamp())}_{Path(new_name).name}"
    
    try:
        # 1. Download the file
        await status_msg.edit_text("ফাইল ডাউনলোড হচ্ছে... ⏳")
        start_time = time.time()
        
        # Download the file using Pyrogram's download method
        download_start_time = time.time()
        download_path = await c.download_media(
            m, 
            file_name=str(out),
            progress=pyrogram_progress_wrapper,
            progress_args=(status_msg, download_start_time, "Download")
        )

        if not download_path:
            await status_msg.edit_text("ডাউনলোড ব্যর্থ।")
            return
        
        file_path = download_path
        
        # 2. Process and Upload
        await process_file_and_upload(c, status_msg, file_path, new_name, uid, m.id)
        
    except Exception as e:
        logger.error(f"Download/Upload failed for user {uid}: {traceback.format_exc()}")
        await status_msg.edit_text(f"ফাইল ডাউনলোড বা আপলোড ব্যর্থ: {e}")
        
    finally:
        # Cleanup temporary file
        try:
            if 'file_path' in locals() and Path(file_path).exists():
                Path(file_path).unlink(missing_ok=True)
        except Exception:
            pass


# --- Main Logic for Download from URL ---
async def handle_url_download_and_upload(c: Client, m: Message, url: str):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
        
    # Check for Create Post Mode
    if uid in CREATE_POST_MODE:
        await m.reply_text("পোস্ট তৈরি মোড চালু আছে। URL আপলোড করার আগে মোড অফ করুন।")
        return

    # Check for MKV Audio Change Mode
    if uid in MKV_AUDIO_CHANGE_MODE:
        await m.reply_text("MKV অডিও পরিবর্তন মোড চালু আছে। URL আপলোড করার আগে মোড অফ করুন।")
        return
        
    # Parse URL to get a sensible name
    original_name = Path(url).name
    if not original_name or original_name == 'download':
        original_name = f"url_file_{int(time.time())}.mp4"
        
    # Use bot's standard naming convention
    new_name = generate_new_filename(original_name)
    out = TMP / f"url_file_{uid}_{int(datetime.now().timestamp())}_{Path(new_name).name}"
    
    status_msg = await m.reply_text(f"ফাইল ডাউনলোড শুরু হচ্ছে... (`{original_name}`) 📥")
    cancel_event = asyncio.Event()
    TASKS[status_msg.id] = cancel_event
    
    try:
        is_success, error = (False, None)
        
        # 1. Download the file
        if is_drive_url(url):
            file_id = extract_drive_id(url)
            if file_id:
                is_success, error = await download_drive_file(file_id, out, message=status_msg, cancel_event=cancel_event)
            else:
                is_success, error = False, "Google Drive ID পাওয়া যায়নি।"
        else:
            is_success, error = await download_url_generic(url, out, message=status_msg, cancel_event=cancel_event)
            
        if not is_success:
            raise RuntimeError(error)
            
        file_path = str(out)
        
        # 2. Process and Upload
        await process_file_and_upload(c, status_msg, file_path, new_name, uid, m.id)
        
    except RuntimeError as e:
        error_text = str(e)
        if "ব্যবহারকারী দ্বারা বাতিল করা হয়েছে" in error_text:
            await status_msg.edit_text("ডাউনলোড বাতিল করা হয়েছে।")
        else:
            await status_msg.edit_text(f"ডাউনলোড ব্যর্থ: {error_text}")
    except Exception as e:
        logger.error(f"URL Download/Upload failed for user {uid}: {traceback.format_exc()}")
        await status_msg.edit_text(f"URL ডাউনলোড বা আপলোড ব্যর্থ: {e}")
        
    finally:
        TASKS.pop(status_msg.id, None)
        # Cleanup temporary file
        try:
            if 'file_path' in locals() and Path(file_path).exists():
                Path(file_path).unlink(missing_ok=True)
        except Exception:
            pass


# --- Logic for Audio Track Check (MKV Audio Change Mode) ---
async def handle_file_download_for_audio_check(c: Client, m: Message, uid: int, status_msg_id: int):
    status_msg = await c.get_messages(m.chat.id, status_msg_id)
    file_obj = m.document or m.video
    original_name = file_obj.file_name or f"temp_file_{file_obj.file_unique_id}.mkv"
    
    # Create a temporary path
    out = TMP / f"audio_check_{file_obj.file_id}_{uid}_{int(datetime.now().timestamp())}_{Path(original_name).name}"
    
    try:
        await status_msg.edit_text("ফাইল ডাউনলোড হচ্ছে... (অডিও ট্র্যাক চেক করার জন্য) ⏳")
        download_start_time = time.time()
        download_path = await c.download_media(
            m, 
            file_name=str(out),
            progress=pyrogram_progress_wrapper,
            progress_args=(status_msg, download_start_time, "Download")
        )
        
        if not download_path:
            await status_msg.edit_text("ডাউনলোড ব্যর্থ।")
            return
            
        file_path = Path(download_path)
        
        # 2. Use FFprobe to get audio tracks
        await status_msg.edit_text("অডিও ট্র্যাকের তথ্য বিশ্লেষণ করা হচ্ছে... 🧐")
        tracks = get_audio_tracks_ffprobe(file_path)
        
        if not tracks:
            await status_msg.edit_text("এই ফাইলে কোনো অডিও ট্র্যাক পাওয়া যায়নি, অথবা ফাইলটি MKV ফরম্যাটে নেই। প্রক্রিয়া বাতিল করা হচ্ছে।")
            file_path.unlink(missing_ok=True)
            # MKV_AUDIO_CHANGE_MODE.discard(uid) # Keep mode ON
            return
            
        # 3. Store file and tracks, prompt user for new order
        AUDIO_CHANGE_FILE[uid] = {
            'path': str(file_path),
            'original_name': original_name,
            'tracks': tracks,
            'message_id': status_msg.id # Store message ID for later deletion
        }
        
        track_list_text = "ফাইলের অডিও ট্র্যাকগুলি:\n"
        track_list_text += "```\n"
        for i, track in enumerate(tracks):
            user_index = i + 1
            track_list_text += f"{user_index}. Original Stream Index: {track['stream_index']} | Title: {track['title']} | Language: {track['language']}\n"
        track_list_text += "```\n"
        track_list_text += f"\nআপনার পছন্দ অনুযায়ী অডিও ট্র্যাকের নতুন **ক্রম (কমা দিয়ে আলাদা করে)** লিখুন। (যেমন: প্রথম ট্র্যাকটিকে শেষে, দ্বিতীয়টিকে প্রথমে এবং তৃতীয়টিকে মাঝে রাখতে চাইলে লিখুন: **`2,3,1`**)\n"
        
        # Update the status message with the track list and prompt
        await status_msg.edit_text(track_list_text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Audio Check failed for user {uid}: {traceback.format_exc()}")
        await status_msg.edit_text(f"অডিও ট্র্যাক চেক করতে সমস্যা: {e}")
        # Cleanup on failure
        try:
            if 'file_path' in locals() and file_path.exists():
                file_path.unlink(missing_ok=True)
        except Exception:
            pass
        # MKV_AUDIO_CHANGE_MODE.discard(uid) # Keep mode ON
        AUDIO_CHANGE_FILE.pop(uid, None)

# --- Final Upload Logic ---
async def process_file_and_upload(c: Client, status_msg: Message, file_path: str, new_name: str, uid: int, original_msg_id: int = None, is_remuxed: bool = False, remux_msg_id: int = None):
    
    file_path = Path(file_path)
    # Check if file still exists (e.g., if it wasn't cancelled)
    if not file_path.exists():
        await status_msg.edit_text("ফাইল প্রক্রিয়াকরণের সময় পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হয়েছে।")
        return
        
    # Get file type info
    mime_type = file_path.suffix.lower()
    
    # Check for custom thumbnail setting
    thumb_path = USER_THUMBS.get(uid)
    thumb_time = USER_THUMB_TIME.get(uid)
    thumb = None
    
    # 1. Auto-generate thumbnail if time is set and it's a video
    if (mime_type in ['.mp4', '.mkv', '.avi', '.mov'] or not is_remuxed) and thumb_time is not None:
        try:
            await status_msg.edit_text(f"ভিডিও থেকে থাম্বনেইল তৈরি হচ্ছে... ({thumb_time} সেকেন্ডে) 🖼️")
            video_duration = get_video_duration(file_path)
            
            # Use min(thumb_time, duration/2) for safe seeking
            seek_time = min(thumb_time, video_duration // 2)
            
            output_thumb_path = TMP / f"auto_thumb_{uid}_{int(time.time())}.jpg"
            
            # FFmpeg command to extract frame
            ffmpeg_cmd = [
                "ffmpeg",
                "-ss", str(seek_time),
                "-i", str(file_path),
                "-vframes", "1",
                "-an",
                "-vf", "scale=320:320:force_original_aspect_ratio=decrease,format=rgb24",
                "-y",
                str(output_thumb_path)
            ]
            
            subprocess.run(ffmpeg_cmd, check=True, timeout=30, capture_output=True)
            if output_thumb_path.exists():
                thumb = str(output_thumb_path)
        except Exception as e:
            logger.warning(f"Auto-thumbnail generation failed: {e}")
            thumb = None

    # 2. Use manually set thumbnail
    if thumb_path and Path(thumb_path).exists():
        thumb = thumb_path
        
    # 3. Handle Caption
    caption_template = USER_CAPTIONS.get(uid)
    final_caption = ""
    if caption_template:
        await status_msg.edit_text("ক্যাপশন প্রক্রিয়া করা হচ্ছে... 📝")
        
        # Extract the current episode number from the counter, defaulting to 1
        current_counter = USER_COUNTERS.get(uid, 1)
        
        # 3a. Process the Auto-Increment markers (e.g., [01], [(01)])
        def replace_counter(match):
            format_str = match.group(1).replace('0', '')
            if ')' in format_str: # Check for [(01)] format
                 return f"({current_counter:02d})"
            return f"{current_counter:02d}"
        
        caption_with_counter = re.sub(r'\[(\(01\)|01)]', replace_counter, caption_template)

        # 3b. Process the Quality Cycler markers (e.g., [re (480p, 720p)])
        def replace_quality(match):
            parts = [p.strip() for p in match.group(1).split(',')]
            if not parts:
                return ""
                
            # Cycle index based on the current counter, 1-based
            index = (current_counter - 1) % len(parts)
            return parts[index]

        caption_with_quality = re.sub(r'\[re\s*\((.*?)\)]', replace_quality, caption_with_counter, flags=re.IGNORECASE)
        
        # 3c. Process the Conditional Text markers (e.g., [End (02)])
        def replace_conditional_text(match):
            text = match.group(1).strip()
            episode_num_str = match.group(2).strip()
            
            try:
                target_episode = int(episode_num_str)
            except ValueError:
                return f"[{text} ({episode_num_str})]" # Return original if number is invalid
            
            if current_counter == target_episode:
                return text
            else:
                return "" # Replace with empty string if condition is not met

        final_caption = re.sub(r'\[(.*)\s*\((.*?)\)]', replace_conditional_text, caption_with_quality)
        
        # 3d. Update the counter for the next use
        USER_COUNTERS[uid] = current_counter + 1
        
    
    # 4. Perform the Upload
    await status_msg.edit_text("Telegram-এ আপলোড হচ্ছে... 🚀")
    
    upload_start_time = time.time()
    
    try:
        # Determine the Telegram method to use
        if mime_type in ['.mp4', '.mkv', '.avi', '.mov', '.ts', '.webm', '.flv'] or is_remuxed:
            # Video Upload
            await c.send_video(
                chat_id=status_msg.chat.id,
                video=str(file_path),
                caption=final_caption,
                file_name=new_name,
                thumb=thumb,
                supports_streaming=True,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, upload_start_time, "Upload")
            )
        elif mime_type in ['.mp3', '.ogg', '.flac', '.wav']:
            # Audio Upload
            await c.send_audio(
                chat_id=status_msg.chat.id,
                audio=str(file_path),
                caption=final_caption,
                file_name=new_name,
                thumb=thumb,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, upload_start_time, "Upload")
            )
        else:
            # Document Upload (Fallback for other types)
            await c.send_document(
                chat_id=status_msg.chat.id,
                document=str(file_path),
                caption=final_caption,
                file_name=new_name,
                thumb=thumb,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, upload_start_time, "Upload")
            )
        
        # 5. Finalize - Delete Status Message and Original Message
        await status_msg.edit_text("আপলোড সফল! ✅")
        
        try:
            # Clean up the status message after a short delay
            await asyncio.sleep(5)
            await c.delete_messages(status_msg.chat.id, [status_msg.id])
            if original_msg_id:
                await c.delete_messages(status_msg.chat.id, [original_msg_id])
            if remux_msg_id and remux_msg_id != status_msg.id:
                await c.delete_messages(status_msg.chat.id, [remux_msg_id])
        except Exception:
            pass # Ignore if deletion fails
        
    except Exception as e:
        logger.error(f"Telegram upload failed: {traceback.format_exc()}")
        await status_msg.edit_text(f"Telegram-এ আপলোড ব্যর্থ: {e}")
        
    finally:
        # Cleanup auto-generated thumbnail
        try:
            if 'output_thumb_path' in locals() and Path(output_thumb_path).exists():
                Path(output_thumb_path).unlink(missing_ok=True)
        except Exception:
            pass
            
@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return
        
    if cb.message.id in TASKS:
        TASKS[cb.message.id].set()
        await cb.message.edit_text("অপারেশন বাতিল করা হচ্ছে... ❌")
        await cb.answer("অপারেশন বাতিল করা হয়েছে।")
    else:
        await cb.answer("কোনো সক্রিয় অপারেশন নেই বা বাতিল করা যাবে না।")


# Flask app for pinging
@flask_app.route('/')
def home():
    return render_template_string("Bot is running!"), 200

# Function to ping the service to keep it alive
def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        # Using a more robust way to handle environment variable checking
        try:
            int(os.environ["RENDER_EXTERNAL_HOSTNAME_IS_NOT_SET_SO_PING_IS_DISABLED"])
        except ValueError:
             print("Render URL is not set. Ping service is disabled.")
        return

    url = f"http://{RENDER_EXTERNAL_HOSTNAME}"
    while True:
        try:
            response = requests.get(url, timeout=10)
            print(f"Pinged {url} | Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error pinging {url}: {e}")
        time.sleep(600)

def run_flask_and_ping():
    flask_thread = threading.Thread(target=lambda: flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False))
    flask_thread.start()
    ping_thread = threading.Thread(target=ping_service)
    ping_thread.start()
    print("Flask and Ping services started.")

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
    print("Bot চালু হচ্ছে... Flask and Ping threads start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask_and_ping, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(set_bot_commands())
        loop.create_task(periodic_cleanup())
        app.run()
    except Exception as e:
        print(f"Bot startup failed: {e}")
