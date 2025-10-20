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

# --- NEW STATE FOR POST CREATION ---
CREATE_POST_MODE = set()
# Stores the state of the post creation process {uid: {'image_path': str, 'message_ids': list, 'state': str, 'post_data': dict, 'post_message_id': int}}
POST_CREATION_STATE = {} 

# --- New states for post data (initial values) ---
DEFAULT_POST_DATA = {
    'image_name': "Image Name",
    'genres': "",
    'season_list_raw': "1, 2" # Stores the raw input, used for dynamic season list
}
# ------------------------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
MAX_SIZE = 4 * 1024 * 1024 * 1024

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Add start_time attribute for uptime calculation
app.start_time = time.time() 

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

# --- UTILITY: Generate Post Caption (NEW - MODIFIED) ---
def generate_post_caption(data: dict) -> str:
    """Generates the full caption based on the post_data with required formatting."""
    image_name = data.get('image_name', DEFAULT_POST_DATA['image_name'])
    genres = data.get('genres', DEFAULT_POST_DATA['genres'])
    season_list_raw = data.get('season_list_raw', DEFAULT_POST_DATA['season_list_raw'])

    # 1. Dynamic Season List Generation
    season_entries = []
    
    # Clean up the input string and split by comma or space
    parts = re.split(r'[,\s]+', season_list_raw.strip())
    parts = [p.strip() for p in parts if p.strip()]

    for part in parts:
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                # Ensure start <= end to avoid infinite loop
                if start > end:
                    start, end = end, start
                for i in range(start, end + 1):
                    # Use two digits padding for season numbers (e.g. 01, 02)
                    season_entries.append(f"**{image_name} Season {i:02d}**")
            except ValueError:
                continue
        else:
            try:
                num = int(part)
                season_entries.append(f"**{image_name} Season {num:02d}**")
            except ValueError:
                continue

    # Remove duplicates and ensure list has at least "Coming Soon..."
    unique_season_entries = list(dict.fromkeys(season_entries))
    if not unique_season_entries:
        unique_season_entries.append("**Coming Soon...**")
    # Add Coming Soon if it's not the last entry and there are other entries
    elif unique_season_entries[-1] != "**Coming Soon...**" and unique_season_entries[0] != "**Coming Soon...**":
        unique_season_entries.append("**Coming Soon...**")
        
    # Season list with newlines between entries
    season_text = "\n".join(unique_season_entries)

    # 2. Main Caption Template (All bold as per user request)
    base_caption = (
        f"**{image_name}**\n"
        f"**────────────────────**\n"
        f"**‣ Audio - Hindi Official**\n"
        f"**‣ Quality - 480p, 720p, 1080p**\n"
        f"**‣ Genres - {genres}**\n"
        f"**────────────────────**"
    )

    # 3. The Collapsible/Quote Block Part (Modified for correct spacing)
    # The quote block mimics a collapsible section in standard Telegram Markdown.
    
    collapsible_text_parts = [
        # 1. Header
        f"> {image_name} All Season List :-", 
        # 2. Empty line after the header (user requested)
        "> " 
    ]
    
    # Add each season entry, prepending a quote character '>' and an empty line after it
    season_lines = season_text.split('\n')
    for i, line in enumerate(season_lines):
        # Season entry added
        collapsible_text_parts.append(f"> {line.strip()}")
        
        # 3. Add an empty line ("> ") after each entry except the last one (user requested)
        if i < len(season_lines) - 1:
            collapsible_text_parts.append("> ")
            
    collapsible_text = "\n".join(collapsible_text_parts)

    # Combine everything
    final_caption = f"{base_caption}\n\n{collapsible_text}"
    
    return final_caption
# ---------------------------------------------

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

# --- UTILITY: Keyboard for Mode Check ---
def mode_check_keyboard(uid: int) -> InlineKeyboardMarkup:
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    
    # Check if a file is waiting for track order input
    waiting_status = " (অর্ডার বাকি)" if uid in AUDIO_CHANGE_FILE else " (No File Waiting)"
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")],
        [InlineKeyboardButton(f"Create Post Mode {'✅ ON' if uid in CREATE_POST_MODE else '❌ OFF'}", callback_data="toggle_post_mode")]
    ]
    return InlineKeyboardMarkup(keyboard)
# ---------------------------------------------


# --- UTILITY: FFprobe to get audio tracks ---
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
# Placeholder for Pyrogram's progress bar (if needed in future, currently minimal)
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
        BotCommand("create_post", "নতুন ছবি পোস্ট তৈরি করুন (admin only)"), # NEW COMMAND
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
        "/create_post - নতুন ছবি পোস্ট তৈরি করুন (admin only)\n" # NEW COMMAND in help
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


@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id
    
    # --- NEW: Handle Create Post Mode ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE and POST_CREATION_STATE[uid]['state'] == 'awaiting_image':
        
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id) # Track user's image message
        
        out = TMP / f"post_img_{uid}.jpg"
        try:
            download_msg = await m.reply_text("ছবি ডাউনলোড হচ্ছে...")
            state_data['message_ids'].append(download_msg.id)
            
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((1080, 1080)) # Resize for reasonable Telegram limit
            img = img.convert("RGB")
            img.save(out, "JPEG")
            
            state_data['image_path'] = str(out)
            state_data['state'] = 'awaiting_name_change'
            
            # Initial Post Send (for display and ID)
            initial_caption = generate_post_caption(state_data['post_data'])
            
            post_msg = await c.send_photo(
                chat_id=m.chat.id, 
                photo=str(out), 
                caption=initial_caption, 
                parse_mode=ParseMode.MARKDOWN
            )
            state_data['post_message_id'] = post_msg.id # Store the post ID
            state_data['message_ids'].append(post_msg.id) # Track the post message ID for final cleanup exclusion
            
            # Send prompt for the first edit step
            prompt_msg = await m.reply_text(
                f"✅ পোস্টের ছবি সেট হয়েছে।\n\n**এখন ছবির নামটি পরিবর্তন করুন।**\n"
                f"বর্তমান নাম: `{state_data['post_data']['image_name']}`\n"
                f"অনুগ্রহ করে শুধু **নামটি** পাঠান। উদাহরণ: `One Piece`"
            )
            state_data['message_ids'].append(prompt_msg.id)

        except Exception as e:
            logger.error(f"Post creation image error: {e}")
            await m.reply_text(f"ছবি সেভ করতে সমস্যা: {e}")
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            if out.exists(): out.unlink()
        return
    # --- END NEW: Handle Create Post Mode ---

    # Handlers for setthumb
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

# --- NEW HANDLER: /create_post ---
@app.on_message(filters.command("create_post") & filters.private)
async def toggle_create_post_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if uid in CREATE_POST_MODE:
        CREATE_POST_MODE.discard(uid)
        # Clean up any pending state
        if uid in POST_CREATION_STATE:
            state_data = POST_CREATION_STATE.pop(uid)
            try:
                # Delete image file
                if state_data.get('image_path'):
                    Path(state_data['image_path']).unlink(missing_ok=True)
                # Delete all conversation messages except the final post if it was created
                messages_to_delete = state_data.get('message_ids', [])
                post_id = state_data.get('post_message_id')
                # Remove the final post ID from the delete list
                if post_id and post_id in messages_to_delete:
                    messages_to_delete.remove(post_id) 
                if messages_to_delete:
                    await c.delete_messages(m.chat.id, messages_to_delete)
            except Exception as e:
                logger.warning(f"Post mode OFF cleanup error: {e}")
                
        await m.reply_text("Create Post Mode **অফ** করা হয়েছে।")
    else:
        CREATE_POST_MODE.add(uid)
        # Initialize state, track command message ID
        POST_CREATION_STATE[uid] = {
            'image_path': None, 
            'message_ids': [m.id], 
            'state': 'awaiting_image', 
            'post_data': DEFAULT_POST_DATA.copy(),
            'post_message_id': None
        }
        await m.reply_text("Create Post Mode **অন** করা হয়েছে।\nএকটি ছবি (**Photo**) পাঠান যা পোস্টের ইমেজ হিসেবে ব্যবহার হবে।")
# ---------------------------------------------


# --- NEW HANDLER: /mode_check ---
@app.on_message(filters.command("mode_check") & filters.private)
async def mode_check_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    post_status = "✅ ON" if uid in CREATE_POST_MODE else "❌ OFF"
    
    waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"
    
    status_text = (
        "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
        f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
        f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
        f"2. **Edit Caption Mode:** `{caption_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
        f"3. **Create Post Mode:** `{post_status}`\n"
        f"   - *কাজ:* ছবি ও টেক্সট দিয়ে নতুন পোস্ট তৈরি করে।\n\n"
        "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
    )
    
    await m.reply_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)

# --- NEW CALLBACK: Mode Toggle Buttons ---
@app.on_callback_query(filters.regex("toggle_(audio|caption|post)_mode"))
async def mode_toggle_callback(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return

    action = cb.data
    message = ""
    
    if action == "toggle_audio_mode":
        if uid in MKV_AUDIO_CHANGE_MODE:
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
            MKV_AUDIO_CHANGE_MODE.add(uid)
            message = "MKV Audio Change Mode ON."
            
    elif action == "toggle_caption_mode":
        if uid in EDIT_CAPTION_MODE:
            EDIT_CAPTION_MODE.discard(uid)
            message = "Edit Caption Mode OFF."
        else:
            EDIT_CAPTION_MODE.add(uid)
            message = "Edit Caption Mode ON."
            
    elif action == "toggle_post_mode":
        if uid in CREATE_POST_MODE:
            CREATE_POST_MODE.discard(uid)
            if uid in POST_CREATION_STATE:
                state_data = POST_CREATION_STATE.pop(uid)
                if state_data.get('image_path'):
                    Path(state_data['image_path']).unlink(missing_ok=True)
                try:
                    # Attempt to delete all conversation messages tracked by the state
                    messages_to_delete = state_data.get('message_ids', [])
                    post_id = state_data.get('post_message_id')
                    if post_id and post_id in messages_to_delete:
                        messages_to_delete.remove(post_id) 
                    if messages_to_delete:
                        await c.delete_messages(cb.message.chat.id, messages_to_delete)
                except Exception:
                    pass
            message = "Create Post Mode OFF."
        else:
            CREATE_POST_MODE.add(uid)
            # Initialize state
            POST_CREATION_STATE[uid] = {
                'image_path': None, 
                'message_ids': [], 
                'state': 'awaiting_image', 
                'post_data': DEFAULT_POST_DATA.copy(),
                'post_message_id': None
            }
            message = "Create Post Mode ON. Please send a Photo."
            
    # Refresh the keyboard and edit the original message (similar to mode_check_cmd)
    try:
        audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
        caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
        post_status = "✅ ON" if uid in CREATE_POST_MODE else "❌ OFF"
        
        waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"

        status_text = (
            "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
            f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে।\n"
            f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
            f"2. **Edit Caption Mode:** `{caption_status}`\n"
            f"   - *কাজ:* শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
            f"3. **Create Post Mode:** `{post_status}`\n"
            f"   - *কাজ:* ছবি ও টেক্সট দিয়ে নতুন পোস্ট তৈরি করে।\n\n"
            "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
        )
        
        await cb.message.edit_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)
        await cb.answer(message, show_alert=True)
    except Exception as e:
        logger.error(f"Callback edit error: {e}")
        await cb.answer(message, show_alert=True)


# --- UTILITY: Apply Caption Logic (Dynamic Counter logic) ---
def apply_caption_logic(caption_template: str, current_data: dict) -> tuple[str, dict]:
    
    # 1. Counter logic: [01] or [(01)]
    if 'count' not in current_data:
        current_data['count'] = 1 # Start from 1
    
    final_caption = caption_template
    
    # 1.1 Incremental counter logic (e.g., [01], [(01)])
    match_ep_num = re.search(r'\[\(?0+(\d+)\)?\]', final_caption)
    
    if match_ep_num:
        # Check for both [01] and [(01)]
        ep_placeholder = match_ep_num.group(0)
        padding = len(match_ep_num.group(1)) # Number of digits in the template
        new_ep_num = str(current_data['count']).zfill(padding)
        final_caption = final_caption.replace(ep_placeholder, new_ep_num, 1) # Replace only the first instance
        
    # 1.2 Quality cycle logic: [re (480p, 720p)]
    match_re_cycle = re.search(r'\[re\s*\((.*?)\)\]', final_caption, re.IGNORECASE)
    if match_re_cycle:
        options_str = match_re_cycle.group(1).strip()
        options = [x.strip() for x in options_str.split(',') if x.strip()]
        
        if options:
            if 're_index' not in current_data:
                current_data['re_index'] = 0
            
            selected_option = options[current_data['re_index']]
            final_caption = final_caption.replace(match_re_cycle.group(0), selected_option)
            current_data['re_index'] = (current_data['re_index'] + 1) % len(options)

    # 1.3 Conditional Text logic: [TEXT (XX)]
    # Use re.findall to find all occurrences
    conditional_matches = re.findall(r'(\[(.+?)\s*\((\d+)\)\])', final_caption)
    
    for full_placeholder, text_to_insert, target_count_str in conditional_matches:
        try:
            target_count = int(target_count_str)
            if current_data['count'] == target_count:
                # Replace with the actual text
                final_caption = final_caption.replace(full_placeholder, text_to_insert, 1)
            else:
                # Remove the placeholder
                final_caption = final_caption.replace(full_placeholder, "", 1) 
        except ValueError:
            # If target_count is not an integer, remove the placeholder
            final_caption = final_caption.replace(full_placeholder, "", 1)
            
    # Increment counter after all processing
    if match_ep_num or conditional_matches: # Only increment if any counter/conditional logic was used
        current_data['count'] += 1
        
    return final_caption, current_data


@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        # Handle auto URL upload for non-admin in text_handler if allowed (currently only for admin)
        return
    text = m.text.strip()
    
    # Handle set caption request
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None) # New: reset counter on new caption set
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # --- Handle audio order input if in mode and file is set ---
    if uid in MKV_AUDIO_CHANGE_MODE and uid in AUDIO_CHANGE_FILE:
        file_data = AUDIO_CHANGE_FILE.get(uid)
        if not file_data or not file_data.get('tracks'):
            await m.reply_text("অডিও ট্র্যাকের তথ্য পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হচ্ছে।")
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
                
                # Get the stream index from the 1-based user input
                stream_index_to_map = tracks[user_track_num - 1]['stream_index']
                new_stream_map.append(f"0:{stream_index_to_map}") 

            track_list_message_id = file_data.get('message_id')
            
            # Start the audio remux process
            asyncio.create_task(
                handle_audio_remux(
                    c, m, Path(file_data['path']), 
                    file_data['original_name'], 
                    new_stream_map, 
                    messages_to_delete=[track_list_message_id, m.id]
                )
            )

            # Clear state immediately
            AUDIO_CHANGE_FILE.pop(uid, None) # Clear only the waiting file state
            return

        except ValueError:
            await m.reply_text("ভুল ফরম্যাট। কমা-সেপারেটেড সংখ্যা দিন। উদাহরণ: `3,2,1`")
            return
        except Exception as e:
            logger.error(f"Audio remux preparation error: {e}")
            await m.reply_text(f"অডিও পরিবর্তন প্রক্রিয়া শুরু করতে সমস্যা: {e}")
            AUDIO_CHANGE_FILE.pop(uid, None)
            return
    # -----------------------------------------------------

    # --- NEW: Handle Post Creation Editing Steps ---
    if uid in CREATE_POST_MODE and uid in POST_CREATION_STATE:
        state_data = POST_CREATION_STATE[uid]
        state_data['message_ids'].append(m.id) # Track user's response message
        
        current_state = state_data['state']
        
        if current_state == 'awaiting_name_change':
            # Step 1: Image Name Change
            if not text:
                prompt_msg = await m.reply_text("নাম খালি রাখা যাবে না। সঠিক নামটি দিন।")
                state_data['message_ids'].append(prompt_msg.id)
                return
            
            state_data['post_data']['image_name'] = text
            state_data['state'] = 'awaiting_genres_add'
            
            new_caption = generate_post_caption(state_data['post_data'])
            # Edit the post's caption
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in name change: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                CREATE_POST_MODE.discard(uid)
                POST_CREATION_STATE.pop(uid, None)
                return

            # Send prompt for the next edit step
            prompt_msg = await m.reply_text(
                f"✅ ছবির নাম সেট হয়েছে: `{text}`\n\n**এখন Genres যোগ করুন।**\n"
                f"উদাহরণ: `Comedy, Romance, Action`"
            )
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_genres_add':
            # Step 2: Genres Add
            state_data['post_data']['genres'] = text # Text can be empty here if user wants no genres
            state_data['state'] = 'awaiting_season_list'
            
            new_caption = generate_post_caption(state_data['post_data'])
            
            # Edit the post's caption
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in genres add: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                CREATE_POST_MODE.discard(uid)
                POST_CREATION_STATE.pop(uid, None)
                return

            # Send prompt for the final edit step
            prompt_msg = await m.reply_text(
                f"✅ Genres সেট হয়েছে: `{text}`\n\n**এখন Season List পরিবর্তন করুন।**\n"
                f"Change Season List এর মানে \"{state_data['post_data']['image_name']}\" Season 01 কয়টি add করব?\n"
                f"ফরম্যাট: সিজন নম্বর অথবা রেঞ্জ কমা বা স্পেস-সেপারেটেড দিন।\n"
                f"উদাহরণ:\n"
                f"‣ `1` (Season 01)\n"
                f"‣ `1-2` (Season 01 থেকে Season 02)\n"
                f"‣ `1-2 4-5` বা `1-2, 4-5` (Season 01-02 এবং 04-05)"
            )
            state_data['message_ids'].append(prompt_msg.id)
            
        elif current_state == 'awaiting_season_list':
            # Step 3: Season List Change (FINAL STEP)
            if not text.strip():
                state_data['post_data']['season_list_raw'] = ""
            else:
                state_data['post_data']['season_list_raw'] = text
            
            # Final Caption Update
            new_caption = generate_post_caption(state_data['post_data'])

            # Edit the post's caption
            try:
                await c.edit_message_caption(m.chat.id, state_data['post_message_id'], caption=new_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Edit caption error in season list: {e}")
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
                CREATE_POST_MODE.discard(uid)
                POST_CREATION_STATE.pop(uid, None)
                return

            # Cleanup and Final Message
            all_messages = state_data.get('message_ids', [])
            # Remove the final post ID from the delete list
            post_id = state_data.get('post_message_id')
            if post_id and post_id in all_messages:
                all_messages.remove(post_id)
                
            # Delete all conversation messages
            if all_messages:
                try:
                    await c.delete_messages(m.chat.id, all_messages)
                except Exception as e:
                    logger.warning(f"Error deleting post creation messages: {e}")

            # Cleanup state image_path = state_data['image_path']
            image_path = state_data['image_path']
            if image_path and Path(image_path).exists():
                Path(image_path).unlink(missing_ok=True)
                
            CREATE_POST_MODE.discard(uid)
            POST_CREATION_STATE.pop(uid, None)
            await m.reply_text("✅ পোস্ট তৈরি সফলভাবে সম্পন্ন হয়েছে এবং সমস্ত অতিরিক্ত বার্তা মুছে ফেলা হয়েছে।")
            return
            
    # --- END NEW: Handle Post Creation Editing Steps ---

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


async def handle_url_download_and_upload(c: Client, m: Message, url: str):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)

    try:
        status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    except Exception:
        status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())

    tmp_in = None
    try:
        fname = url.split("/")[-1].split("?")[0] or f"download_{int(datetime.now().timestamp())}"
        safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)
        
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        if not any(safe_name.lower().endswith(ext) for ext in video_exts):
            # Check file extension and default to .mp4 if generic
            if '.' not in safe_name:
                safe_name += ".mp4"
            
        tmp_in = TMP / f"dl_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
        
        ok, err = False, None
        
        try:
            await status_msg.edit("ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
            status_msg = await m.reply_text("ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
        
        if is_drive_url(url):
            fid = extract_drive_id(url)
            if not fid:
                try:
                    await status_msg.edit("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি। সঠিক লিংক দিন।", reply_markup=None)
                except Exception:
                    await m.reply_text("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি। সঠিক লিংক দিন।", reply_markup=None)
                TASKS[uid].remove(cancel_event)
                return
            ok, err = await download_drive_file(fid, tmp_in, status_msg, cancel_event=cancel_event)
        else:
            ok, err = await download_url_generic(url, tmp_in, status_msg, cancel_event=cancel_event)

        if not ok:
            try:
                await status_msg.edit(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            except Exception:
                await m.reply_text(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            try:
                if tmp_in and tmp_in.exists():
                    tmp_in.unlink()
            except:
                pass
            TASKS[uid].remove(cancel_event)
            return

        try:
            await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
        except Exception:
            await m.reply_text("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
        
        # NEW RENAME FEATURE: URL আপলোডের জন্য নাম পরিবর্তন
        renamed_file = generate_new_filename(safe_name)
        # -------------------------------------------------------

        await process_file_and_upload(c, m, tmp_in, original_name=renamed_file, status_msg=status_msg, cancel_event=cancel_event)

    except Exception as e:
        logger.error("Error in handle_url_download_and_upload: %s", traceback.format_exc())
        try:
            await m.reply_text(f"URL ডাউনলোড বা আপলোডে সাধারণ সমস্যা: {e}")
        except Exception:
            pass
    finally:
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)
        # Clean up in case of error before upload
        if tmp_in and tmp_in.exists():
            try:
                tmp_in.unlink()
            except:
                pass

# --- HANDLER: /rename ---
@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
        
    if not m.reply_to_message:
        await m.reply_text("একটি ভিডিও বা ডকুমেন্ট মেসেজে রিপ্লাই করে এই কমান্ডটি দিন।")
        return

    file_to_rename = m.reply_to_message.video or m.reply_to_message.document or m.reply_to_message.animation
    if not file_to_rename:
        await m.reply_text("আপনার রিপ্লাই করা মেসেজটি একটি ভিডিও বা ডকুমেন্ট নয়।")
        return

    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /rename <newname.ext>\nউদাহরণ: /rename MyAwesomeVideo.mp4")
        return

    new_name = m.text.split(None, 1)[1].strip()
    
    # Extract the original extension to maintain file integrity
    original_file_name = file_to_rename.file_name or "file"
    original_ext = Path(original_file_name).suffix
    
    # Ensure the new name includes the extension, or add the original one
    if not Path(new_name).suffix or Path(new_name).suffix.lower() != original_ext.lower():
        new_name = f"{new_name.split('.')[0]}{original_ext}"


    # Start the rename process
    asyncio.create_task(
        handle_rename_and_upload(c, m, file_to_rename, new_name)
    )

async def handle_rename_and_upload(c: Client, m: Message, file_to_rename, new_name: str):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    # Send a status message
    status_msg = await m.reply_text(f"রিনেম করার জন্য ফাইল ডাউনলোড হচ্ছে: `{new_name}`...", reply_markup=progress_keyboard())
    
    tmp_in = TMP / f"rn_{uid}_{int(datetime.now().timestamp())}_{file_to_rename.file_id}_{Path(new_name).name}"
    
    try:
        # Download the file
        await status_msg.edit("ভিডিও ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
        await c.download_media(file_to_rename, file_name=str(tmp_in), progress=pyrogram_progress_wrapper, progress_args=(status_msg, time.time(), "ডাউনলোড"))

        # Process and upload the file with the new name
        await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ নতুন নামে আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_in, original_name=new_name, status_msg=status_msg, cancel_event=cancel_event)

    except asyncio.CancelledError:
        try:
            await status_msg.edit("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            pass
    except Exception as e:
        logger.error("Error in handle_rename_and_upload: %s", traceback.format_exc())
        try:
            await status_msg.edit(f"রিনেম বা আপলোডে সমস্যা: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"রিনেম বা আপলোডে সমস্যা: {e}")
    finally:
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)
        if tmp_in.exists():
            try:
                tmp_in.unlink()
            except:
                pass


# --- HANDLER: Documents and Videos (The main working handler) ---
@app.on_message(filters.document | filters.video | filters.animation | filters.private)
async def handle_document_and_video(c, m: Message):
    if not is_admin(m.from_user.id):
        return

    uid = m.from_user.id
    
    # Ignore if in Create Post Mode and expecting a photo (should be handled by filters.photo)
    if uid in CREATE_POST_MODE:
        return 

    media = m.document or m.video or m.animation
    if not media:
        return # Should not happen

    # 1. Handle MKV Audio Change Mode
    if uid in MKV_AUDIO_CHANGE_MODE and uid not in AUDIO_CHANGE_FILE:
        
        file_path = TMP / f"mkv_dl_{uid}_{media.file_name}"
        status_msg = await m.reply_text("ফাইল ডাউনলোড হচ্ছে...")
        
        try:
            await c.download_media(m, file_name=str(file_path), progress=pyrogram_progress_wrapper, progress_args=(status_msg, datetime.now(), "ডাউনলোড"))
            await status_msg.edit("ডাউনলোড সম্পন্ন, অডিও ট্র্যাক পরীক্ষা করা হচ্ছে...")

            tracks = get_audio_tracks_ffprobe(file_path)
            
            audio_track_list = []
            if tracks:
                for i, track in enumerate(tracks):
                    audio_track_list.append(
                        f"**{i+1}.** Stream Index: `{track['stream_index']}` | Language: `{track['language']}` | Title: `{track['title']}`"
                    )
            
            if not audio_track_list:
                 await status_msg.edit("ফাইলটিতে কোনো অডিও ট্র্যাক পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হচ্ছে।")
                 Path(file_path).unlink(missing_ok=True)
                 return
                 
            # Store file info and tracks
            AUDIO_CHANGE_FILE[uid] = {
                'path': str(file_path),
                'original_name': media.file_name,
                'tracks': tracks
            }
            
            track_list_message = (
                "**MKV অডিও ট্র্যাক লিস্ট:**\n"
                "--------------------------------\n"
                f"{'\n'.join(audio_track_list)}\n"
                "--------------------------------\n"
                "আপনি কোন অর্ডারে ট্র্যাকগুলি চান? কমা-সেপারেটেড সংখ্যা দিন।\n"
                f"উদাহরণ (যদি 3টি ট্র্যাক থাকে): `3,2,1` (এই অর্ডারে ট্র্যাকগুলো রিমাক্স হবে)"
            )
            
            prompt_msg = await c.send_message(m.chat.id, track_list_message, parse_mode=ParseMode.MARKDOWN, reply_to_message_id=m.id)
            AUDIO_CHANGE_FILE[uid]['message_id'] = prompt_msg.id # Store the prompt message ID for deletion

        except Exception as e:
            logger.error(f"MKV audio change error: {e}")
            await status_msg.edit(f"MKV অডিও পরিবর্তন মোডে সমস্যা: {e}")
            Path(file_path).unlink(missing_ok=True)
            if uid in AUDIO_CHANGE_FILE: AUDIO_CHANGE_FILE.pop(uid)
        return

    # 2. Handle standard file upload
    file_path = TMP / f"tg_dl_{uid}_{media.file_name}"
    status_msg = await m.reply_text("ফাইল ডাউনলোড হচ্ছে...")
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    try:
        # Download the file
        await c.download_media(m, file_name=str(file_path), progress=pyrogram_progress_wrapper, progress_args=(status_msg, datetime.now(), "ডাউনলোড"))
        await status_msg.edit("ডাউনলোড সম্পন্ন, আপলোড হচ্ছে...")
        
        # Process and upload the file
        await process_file_and_upload(c, m, file_path, media.file_name, status_msg, cancel_event)
        
    except asyncio.CancelledError:
        try:
            await status_msg.edit("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Telegram download/upload error: {e}")
        error_message = f"❌ Telegram ফাইল আপলোডে সমস্যা: {e}"
        try:
            await status_msg.edit(error_message, reply_markup=None)
        except Exception:
            await m.reply_text(error_message, reply_markup=None)

    finally:
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)
        Path(file_path).unlink(missing_ok=True) # Clean up the downloaded file
            

async def process_file_and_upload(c: Client, m: Message, file_path: Path, original_name: str, status_msg: Message, cancel_event: asyncio.Event):
    # এটি মূল আপলোড লজিক (রিনেম, থাম্বনেইল, ক্যাপশন)
    uid = m.from_user.id
    new_name = original_name
    thumb_path = None
    video_duration = 0
    auto_thumb_out = None

    try:
        # 1. Rename logic (only if not in EDIT_CAPTION_MODE)
        if uid not in EDIT_CAPTION_MODE and file_path.name.startswith(("dl_", "tg_dl_", "rn_", "mkv_dl_")):
            # Standard renaming for new uploads/renames, ignore if it's already a remuxed file
            new_name = generate_new_filename(original_name)
        else:
             new_name = original_name # Keep the name for edit caption mode or remuxed files

        # 2. Get Thumbnail (Use user's custom thumb path first)
        thumb_path = USER_THUMBS.get(uid)
        thumb_time = USER_THUMB_TIME.get(uid)
        
        # 3. Generate automatic thumbnail if user set time and no custom thumb set and not in edit caption mode
        if uid not in EDIT_CAPTION_MODE and not thumb_path and thumb_time and file_path.suffix.lower() in {".mp4", ".mkv"}:
            
            video_duration = get_video_duration(file_path)
            if video_duration > 0:
                # Use custom time or fallback to a safe time
                time_to_extract = min(thumb_time, video_duration) if thumb_time else int(video_duration * 0.1)
                time_to_extract = max(1, time_to_extract) # Ensure at least 1 second

                # Check for automatic thumbnail
                auto_thumb_out = TMP / f"auto_thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
                cmd = [
                    "ffmpeg",
                    "-i", str(file_path),
                    "-ss", str(time_to_extract),
                    "-vframes", "1",
                    "-vf", "scale='min(320,iw)':min'(320,ih)':force_original_aspect_ratio=decrease,format=rgb24",
                    "-y", str(auto_thumb_out)
                ]
                subprocess.run(cmd, check=True, capture_output=True, timeout=120)

                if auto_thumb_out.exists():
                    img = Image.open(auto_thumb_out)
                    img = img.convert("RGB")
                    img.save(auto_thumb_out, "JPEG")
                    thumb_path = str(auto_thumb_out)
        
        # 4. Dynamic Caption Generation
        caption_template = USER_CAPTIONS.get(uid)
        final_caption = None
        if caption_template:
            # Use the counter from global state
            final_caption, USER_COUNTERS[uid] = apply_caption_logic(caption_template, USER_COUNTERS.get(uid, {}))
        
        # 5. Upload
        if file_path.suffix.lower() in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}:
            # Send as Video
            await c.send_video(
                chat_id=m.chat.id,
                video=str(file_path),
                caption=final_caption,
                file_name=new_name,
                thumb=thumb_path,
                supports_streaming=True,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, time.time(), "আপলোড")
            )
        else:
            # Send as Document
            await c.send_document(
                chat_id=m.chat.id,
                document=str(file_path),
                caption=final_caption,
                file_name=new_name,
                thumb=thumb_path,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, time.time(), "আপলোড")
            )
        
        await status_msg.delete()
        
    except Exception as e:
        logger.error("Error during upload: %s", traceback.format_exc())
        upload_error = f"ফাইল আপলোড করতে সমস্যা: {e}"
        if cancel_event.is_set():
            upload_error = "❌ আপলোড বাতিল করা হয়েছে।"

        try:
            await status_msg.edit(upload_error, reply_markup=None)
        except Exception:
            await m.reply_text(upload_error, reply_markup=None)
    finally:
        # Cleanup temporary files
        if file_path.exists():
            file_path.unlink(missing_ok=True)
        if auto_thumb_out and Path(auto_thumb_out).exists():
            Path(auto_thumb_out).unlink(missing_ok=True)


# --- FUNCTION: Handle Audio Remux (Called from text_handler) ---
async def handle_audio_remux(c: Client, m: Message, file_path: Path, original_name: str, stream_map: list, messages_to_delete: list):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    tmp_in = file_path
    
    # Generate the output name based on the original name but with a unique ID
    safe_base_name = re.sub(r"[\\/*?\"<>|:]", "_", Path(original_name).stem)
    tmp_out = TMP / f"remuxed_{uid}_{int(datetime.now().timestamp())}_{safe_base_name}.mkv"
    
    status_msg = None
    try:
        # 1. Send initial status
        status_msg = await c.send_message(m.chat.id, "অডিও ট্র্যাক পরিবর্তন প্রক্রিয়া শুরু হচ্ছে...", reply_to_message_id=m.id, reply_markup=progress_keyboard())
        
        # 2. Construct FFmpeg command
        map_args = sum([['-map', x] for x in stream_map], [])
        
        cmd = [
            "ffmpeg",
            "-i", str(tmp_in),
            "-c", "copy",
            "-map", "0:v:0", # Map the first video stream
            *map_args,       # Map the audio streams in new order
            "-map", "0:s?",  # Map all subtitle streams (if any)
            "-map", "0:d?",  # Map all data streams (if any)
            "-map", "0:t?",  # Map all attachment streams (if any)
            # Metadata for first two audio tracks (standard assumption)
            "-metadata:s:a:0", "title=Hindi Official", 
            "-metadata:s:a:1", "title=English",
            "-max_muxing_queue_size", "1024",
            "-f", "mkv",
            "-y", str(tmp_out)
        ]
        
        # 3. Execute FFmpeg (synchronously in a thread or process)
        await status_msg.edit("অডিও পরিবর্তন হচ্ছে (Remuxing)...", reply_markup=progress_keyboard())
        
        # Use asyncio.to_thread for long-running blocking subprocess
        process = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=7200)

        if process.returncode != 0:
            error_output = process.stderr or "Unknown FFmpeg error"
            raise Exception(f"FFmpeg error:\n{error_output[-500:]}")
            
        if not tmp_out.exists():
            raise Exception("FFmpeg প্রক্রিয়া সম্পন্ন হলেও আউটপুট ফাইল খুঁজে পাওয়া যায়নি।")

        # 4. Upload the remuxed file
        # Rename to a standardized name for upload
        renamed_file = generate_new_filename(original_name)
        
        await status_msg.edit("Telegram-এ নতুন ফাইলে আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_out, original_name=renamed_file, status_msg=status_msg, cancel_event=cancel_event)
        
        # 5. Final Cleanup (Delete conversation messages including the track list and input)
        if messages_to_delete:
            try:
                await c.delete_messages(m.chat.id, messages_to_delete)
            except Exception as e:
                logger.warning(f"Error deleting remux conversation messages: {e}")
        
    except asyncio.CancelledError:
        try:
            if status_msg:
                await status_msg.edit("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।", reply_markup=None)
            else:
                await m.reply_text("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"FFmpeg or Remux error: {traceback.format_exc()}")
        error_message = f"অডিও পরিবর্তন ব্যর্থ: {e}"
        if status_msg:
            try:
                await status_msg.edit(error_message, reply_markup=None)
            except Exception:
                await m.reply_text(error_message, reply_markup=None)
        else:
             await m.reply_text(error_message, reply_markup=None)
    finally:
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)
        # Clean up both input and output files
        if tmp_in.exists(): tmp_in.unlink(missing_ok=True)
        if tmp_out.exists(): tmp_out.unlink(missing_ok=True)
        
        
# --- HANDLER: Cancel Task Callback ---
@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return
    
    # Set the event for all running tasks for this user
    for event in TASKS.get(uid, []):
        event.set()
        
    # Clear the list
    TASKS.pop(uid, None) # Clear the list
    
    # Clear audio change file state if pending
    if uid in AUDIO_CHANGE_FILE:
        try:
            Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
            if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                 await c.delete_messages(cb.message.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
        except Exception:
            pass
        AUDIO_CHANGE_FILE.pop(uid, None)
        
    await cb.message.edit_text("❌ অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
    await cb.answer("অপারেশন বাতিল করা হয়েছে।")

# --- HANDLER: Broadcast ---
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
        
    if not m.reply_to_message and len(m.command) < 2:
        await m.reply_text("ব্যবহার: `/broadcast <টেক্সট>` অথবা একটি মেসেজে রিপ্লাই করে `/broadcast` দিন।")
        return

    text = m.text.split(None, 1)[1] if len(m.command) >= 2 else None
    
    if m.reply_to_message:
        await send_broadcast(c, message=m.reply_to_message, m=m)
    elif text:
        await send_broadcast(c, text=text, m=m)

async def send_broadcast(c: Client, m: Message, message: Message = None, text: str = None):
    # Sends the broadcast to all subscribers
    success, failed = 0, 0
    
    # Add sender's chat ID to subscribers list for safety, in case it was accidentally removed
    SUBSCRIBERS.add(m.chat.id)
    
    sub_list = list(SUBSCRIBERS)
    total = len(sub_list)
    
    broadcast_message = await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে... ({total} ব্যবহারকারী)")
    
    for uid in sub_list: # Iterate over the list
        try:
            if message:
                await message.copy(uid)
            elif text:
                await c.send_message(uid, text)
            success += 1
            await asyncio.sleep(0.1) # small delay to avoid flood waits
        except Exception:
            failed += 1
            SUBSCRIBERS.discard(uid) # Remove failed subscribers
        
    await broadcast_message.edit_text(f"✅ ব্রডকাস্ট সম্পন্ন!\nসফল: {success}\nব্যর্থ: {failed}")

# --- FLASK APP and PING SERVICE (for deployment platforms like Render) ---

@flask_app.route('/')
def web_index():
    # Show status of bot and uptime
    uptime = str(timedelta(seconds=int(time.time() - app.start_time))) if hasattr(app, 'start_time') else "N/A"
    return render_template_string(
        "<h1>URL Uploader Bot Status</h1>"
        f"<p>Status: <b>Running</b></p>"
        f"<p>Uptime: <b>{uptime}</b></p>"
    )

def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        print("Render URL is not set. Ping service is disabled.")
        return

    url = f"http://{RENDER_EXTERNAL_HOSTNAME}"
    while True:
        try:
            # Use requests.get which is synchronous
            response = requests.get(url, timeout=10)
            print(f"Pinged {url} | Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error pinging {url}: {e}")
        time.sleep(600)

def run_flask_and_ping():
    # Start Flask app on a separate thread
    flask_thread = threading.Thread(target=lambda: flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False))
    flask_thread.start()
    
    # Start the ping service on a separate thread
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
                        # Delete files older than 3 days
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
        # Set bot commands synchronously before app.run() starts the client
        loop.run_until_complete(set_bot_commands())
        # Start cleanup task
        loop.create_task(periodic_cleanup())
        # Start the Pyrogram client (blocking call)
        app.run()
    except KeyboardInterrupt:
        print("\nBot বন্ধ হচ্ছে...")
    except Exception as e:
        logger.error(f"Main execution error: {e}")
