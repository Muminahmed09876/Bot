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

# --- UTILITY: Generate Post Caption (NEW) ---
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

    # 3. The Collapsible/Quote Block Part (All bold and in a quote block)
    # The quote block mimics a collapsible section in standard Telegram Markdown.
    
    # Start the quote block with the header
    collapsible_text_parts = [
        f"> **\"{image_name}\" All Season List :-**",
        "> "
    ]
    
    # Add each season entry, prepending a quote character '>'
    for line in season_text.split('\n'):
        collapsible_text_parts.append(f"> {line}")
        
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

# --- NEW UTILITY: Keyboard for Mode Check ---
def mode_check_keyboard(uid: int) -> InlineKeyboardMarkup:
    audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
    caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
    
    # Check if a file is waiting for track order input
    waiting_status = " (অর্ডার বাকি)" if uid in AUDIO_CHANGE_FILE else ""
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")]
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
    
    waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"
    
    status_text = (
        "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
        f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
        f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
        f"2. **Edit Caption Mode:** `{caption_status}`\n"
        f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
        "নিচের বাটনগুলিতে ক্লিক করে মোড পরিবর্তন করুন।"
    )
    
    await m.reply_text(status_text, reply_markup=mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)

# --- NEW CALLBACK: Mode Toggle Buttons ---
@app.on_callback_query(filters.regex("toggle_(audio|caption)_mode"))
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
            
    # Refresh the keyboard and edit the original message (similar to mode_check_cmd)
    try:
        audio_status = "✅ ON" if uid in MKV_AUDIO_CHANGE_MODE else "❌ OFF"
        caption_status = "✅ ON" if uid in EDIT_CAPTION_MODE else "❌ OFF"
        
        waiting_status_text = "একটি ফাইল ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে।" if uid in AUDIO_CHANGE_FILE else "কোনো ফাইল অপেক্ষা করছে না।"

        status_text = (
            "🤖 **বর্তমান মোড স্ট্যাটাস:**\n\n"
            f"1. **MKV Audio Change Mode:** `{audio_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড/ডাউনলোড করা MKV/ভিডিও ফাইলের অডিও ট্র্যাক অর্ডার পরিবর্তন করে। (ম্যানুয়ালি অফ না করা পর্যন্ত ON থাকবে)\n"
            f"   - *স্ট্যাটাস:* {waiting_status_text}\n\n"
            f"2. **Edit Caption Mode:** `{caption_status}`\n"
            f"   - *কাজ:* ফরওয়ার্ড করা ভিডিওর রিনেম বা থাম্বনেইল পরিবর্তন না করে শুধু সেভ করা ক্যাপশন যুক্ত করে।\n\n"
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
                    
            # Cleanup state image_path
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
    
    try:
        fname = url.split("/")[-1].split("?")[0] or f"download_{int(datetime.now().timestamp())}"
        safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        if not any(safe_name.lower().endswith(ext) for ext in video_exts):
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
                if tmp_in.exists():
                    tmp_in.unlink()
            except:
                pass
            TASKS[uid].remove(cancel_event)
            return

        # The part where upload starts
        await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=progress_keyboard())
        
        await handle_file_upload(c, m, tmp_in, safe_name, status_msg, cancel_event)

    except Exception as e:
        logger.error(f"Error in handle_url_download_and_upload: {e}", exc_info=True)
        try:
            await status_msg.edit(f"অজানা ত্রুটি: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"অজানা ত্রুটি: {e}", reply_markup=None)
        
        try:
            if tmp_in.exists():
                tmp_in.unlink()
        except:
            pass
        
    finally:
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)


@app.on_message(filters.command("rename") & filters.private & filters.reply)
async def rename_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        await m.reply_text("একটি ভিডিও বা ডকুমেন্ট মেসেজে রিপ্লাই করে এই কমান্ড ব্যবহার করুন।")
        return

    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /rename <নতুন_নাম.ext>\nউদাহরণ: /rename MyMovie.mp4")
        return

    new_name = m.text.split(None, 1)[1].strip()
    
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    status_msg = None
    tmp_in = None
    try:
        status_msg = await m.reply_text("ফাইল ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
        
        file_obj = m.reply_to_message.video or m.reply_to_message.document
        
        # Get the original file extension
        original_ext = Path(file_obj.file_name or "").suffix
        # Ensure the new name has an extension (if not, use the original one)
        if not Path(new_name).suffix and original_ext:
            new_name += original_ext
        
        tmp_in = TMP / f"rn_{uid}_{int(datetime.now().timestamp())}_{new_name}"
        
        # Download the file
        await c.download_media(
            m.reply_to_message,
            file_name=str(tmp_in),
            progress=pyrogram_progress_wrapper,
            progress_args=(status_msg, time.time(), "ডাউনলোড"),
        )
        
        await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=progress_keyboard())
        
        await handle_file_upload(c, m, tmp_in, new_name, status_msg, cancel_event)

    except Exception as e:
        logger.error(f"Error in rename_cmd: {e}", exc_info=True)
        if status_msg:
            try:
                await status_msg.edit(f"রিনেম ব্যর্থ: {e}", reply_markup=None)
            except Exception:
                pass
        
    finally:
        if tmp_in and tmp_in.exists():
            tmp_in.unlink(missing_ok=True)
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)


@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
        
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /broadcast <বার্তা>")
        return

    text = m.text.split(None, 1)[1].strip()
    sent_count = 0
    fail_count = 0
    
    broadcast_msg = await m.reply_text("ব্রডকাস্ট শুরু হচ্ছে...", reply_markup=None)
    
    for uid in list(SUBSCRIBERS):
        if uid == m.chat.id:
            continue
        try:
            await c.send_message(uid, text)
            sent_count += 1
            await asyncio.sleep(0.1) # Be nice to Telegram's flood limits
        except Exception:
            fail_count += 1
            SUBSCRIBERS.discard(uid) # Assume user blocked bot
            
    await broadcast_msg.edit(f"ব্রডকাস্ট সম্পন্ন!\nসফল: {sent_count}\nব্যর্থ: {fail_count}")

# --- Core File Handling Function (Unified Logic for Upload) ---
async def handle_file_upload(c: Client, m: Message, file_path: Path, new_file_name: str, status_msg: Message, cancel_event: asyncio.Event):
    uid = m.from_user.id
    
    # Check for cancellation
    if cancel_event.is_set():
        raise Exception("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।")

    # 1. Prepare file attributes
    is_video = any(file_path.name.lower().endswith(ext) for ext in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"})
    is_image = any(file_path.name.lower().endswith(ext) for ext in {".jpg", ".jpeg", ".png"})
    
    thumb = None
    duration = 0
    width = 0
    height = 0
    
    # 2. Get Thumbnail / Duration / Resolution if it is a video
    if is_video:
        duration = get_video_duration(file_path)
        
        # Determine thumbnail path or create from video if time is set
        thumb_path = USER_THUMBS.get(uid)
        thumb_time = USER_THUMB_TIME.get(uid)
        
        if thumb_path and Path(thumb_path).exists():
            thumb = str(thumb_path)
        elif thumb_time and duration > 0:
            # Create thumbnail from video at specified time
            temp_thumb_path = TMP / f"thumb_auto_{uid}.jpg"
            try:
                # Ensure the seek time doesn't exceed the duration
                seek_time = min(thumb_time, duration - 1) 
                
                # Use subprocess to run ffmpeg
                cmd = [
                    "ffmpeg",
                    "-ss", str(seek_time),
                    "-i", str(file_path),
                    "-vframes", "1",
                    "-y",
                    str(temp_thumb_path)
                ]
                subprocess.run(cmd, check=True, capture_output=True, timeout=60)
                
                # Resize the created thumbnail
                img = Image.open(temp_thumb_path)
                img.thumbnail((320, 320))
                img = img.convert("RGB")
                img.save(temp_thumb_path, "JPEG")
                thumb = str(temp_thumb_path)
            except Exception as e:
                logger.error(f"Auto thumbnail creation failed: {e}")
        
        # Get width and height using ffprobe (assuming ffprobe is available)
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "json",
                str(file_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
            metadata = json.loads(result.stdout)
            stream = metadata.get('streams', [{}])[0]
            width = stream.get('width', 0)
            height = stream.get('height', 0)
        except Exception as e:
            logger.warning(f"ffprobe resolution failed: {e}")


    # 3. Handle Caption
    caption = USER_CAPTIONS.get(uid, "")
    
    # Caption dynamic replacement logic
    if caption:
        # Counter management (used for [01], [(01)] etc.)
        if uid not in USER_COUNTERS:
            USER_COUNTERS[uid] = {}
            
        def replace_counter(match):
            placeholder = match.group(0) # e.g., '[01]', '[(01)]'
            
            # Extract number of digits for padding and bracket type
            is_paren = placeholder.startswith('[(')
            num_str = placeholder.strip('[]() ')
            padding = len(num_str)
            
            # Use the full placeholder string as the key to support multiple counters
            key = placeholder
            
            if key not in USER_COUNTERS[uid]:
                # Initialize counter, starting from 1 (or value in placeholder if available)
                # The user-provided text like '01' is only for padding hint, so start at 1
                USER_COUNTERS[uid][key] = 1 
            else:
                # Increment the counter
                USER_COUNTERS[uid][key] += 1
                
            current_count = USER_COUNTERS[uid][key]
            
            formatted_count = f"{current_count:0{padding}d}"
            
            if is_paren:
                return f"({formatted_count})"
            else:
                return formatted_count

        # 1. Replace all counter placeholders (e.g., [01], [(01)])
        # Use a regex that captures both [01] and [(01)] style counters
        caption = re.sub(r'\[(\s*\(*\s*\d+\s*\)*\s*)\]', replace_counter, caption)

        # 2. Handle conditional text: [TEXT (XX)]
        # This relies on the last incremented counter's value. We need the first counter's value.
        # Find the first counter key and its value
        first_counter_key = next(iter(USER_COUNTERS[uid]), None)
        current_episode_number = USER_COUNTERS[uid].get(first_counter_key, 0)
        
        def replace_conditional_text(match):
            text_to_insert = match.group(1).strip()
            # The number inside the parentheses is the condition
            condition_num = int(match.group(2))
            
            if current_episode_number == condition_num:
                return text_to_insert
            else:
                return ""

        # Regex for [TEXT (XX)] format, e.g., [End (02)]
        caption = re.sub(r'\[([^\](]+)\s*\(\s*(\d+)\s*\)\]', replace_conditional_text, caption)
        
        # 3. Handle quality cycle: [re (480p, 720p)]
        def replace_quality_cycle(match):
            placeholder = match.group(0) # e.g., '[re (480p, 720p)]'
            cycle_list_raw = match.group(1).strip() # e.g., '480p, 720p'
            
            key = placeholder
            
            if key not in USER_COUNTERS[uid]:
                # Initialize index to 0
                USER_COUNTERS[uid][key] = 0
            
            cycle_list = [s.strip() for s in cycle_list_raw.split(',') if s.strip()]
            if not cycle_list:
                return placeholder # Return original if list is empty
            
            # Get and increment index
            current_index = USER_COUNTERS[uid][key]
            
            # The index should be based on the last incremented counter, so it should cycle on every upload.
            # However, since a new counter is created for each new caption, we'll use the episode number for the index.
            # Using current_episode_number (from the first counter) for the index is a better assumption.
            if current_episode_number > 0:
                current_index = (current_episode_number - 1) % len(cycle_list)
            else:
                # If no episode counter, just use the internal counter for cycle index
                # Increment the internal index after use, but not for the episode counter.
                current_index = USER_COUNTERS[uid][key] % len(cycle_list)
                USER_COUNTERS[uid][key] = current_index + 1
            
            return cycle_list[current_index]

        # Regex for [re (list, of, items)]
        caption = re.sub(r'\[re\s*\(([^)]+)\)\]', replace_quality_cycle, caption)


    # 4. Handle Upload
    try:
        if cancel_event.is_set():
            raise Exception("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।")

        await status_msg.edit("আপলোড হচ্ছে...", reply_markup=progress_keyboard())
        
        if is_video:
            # Check if edit caption mode is ON
            if uid in EDIT_CAPTION_MODE and m.reply_to_message:
                # Use original file name and thumbnail
                video = m.reply_to_message.video
                new_file_name = video.file_name
                thumb = video.thumbs[0].file_id if video.thumbs else None
                duration = video.duration
                width = video.width
                height = video.height
                
                # Upload as document/video with original info
                await c.send_document(
                    chat_id=m.chat.id,
                    document=str(file_path),
                    caption=caption,
                    file_name=new_file_name,
                    force_document=False, # Try to send as video if applicable
                    thumb=thumb,
                    progress=pyrogram_progress_wrapper,
                    progress_args=(status_msg, time.time(), "আপলোড"),
                    reply_markup=None # No progress keyboard after upload starts
                )
            else:
                # Regular upload with new name and custom/generated thumbnail
                await c.send_video(
                    chat_id=m.chat.id,
                    video=str(file_path),
                    caption=caption,
                    file_name=generate_new_filename(new_file_name), # Apply standardized name
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb,
                    progress=pyrogram_progress_wrapper,
                    progress_args=(status_msg, time.time(), "আপলোড"),
                    reply_markup=None
                )

        elif is_image:
            await c.send_photo(
                chat_id=m.chat.id,
                photo=str(file_path),
                caption=caption,
                file_name=new_file_name,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, time.time(), "আপলোড"),
                reply_markup=None
            )
        else:
            await c.send_document(
                chat_id=m.chat.id,
                document=str(file_path),
                caption=caption,
                file_name=new_file_name,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, time.time(), "আপলোড"),
                reply_markup=None
            )

        await status_msg.delete()
        await m.reply_text("✅ ফাইল সফলভাবে আপলোড হয়েছে।", reply_markup=None)

    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        # Check for cancellation before reporting error
        if not cancel_event.is_set():
            await status_msg.edit(f"আপলোড ব্যর্থ: {e}", reply_markup=None)
        
    finally:
        # Cleanup temp files
        file_path.unlink(missing_ok=True)
        if thumb and thumb.startswith(str(TMP)):
            Path(thumb).unlink(missing_ok=True)
        
        # The cancel_event should be removed in the calling functions (handle_url_download_and_upload or rename_cmd)
        # to ensure it's removed only once and the list isn't modified during iteration/concurrent access.
        pass


@app.on_message(filters.video | filters.document & filters.private)
async def video_document_handler(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    # --- NEW: Handle MKV Audio Change Mode (Primary logic) ---
    if uid in MKV_AUDIO_CHANGE_MODE and not m.reply_to_message: 
        
        file_obj = m.video or m.document
        if not file_obj:
            await m.reply_text("এটি কোনো ভিডিও বা ডকুমেন্ট নয়। অনুগ্রহ করে একটি ভিডিও বা MKV ফাইল দিন।")
            return

        cancel_event = asyncio.Event()
        TASKS.setdefault(uid, []).append(cancel_event)
        
        # Clean up any previous pending file for this user
        if uid in AUDIO_CHANGE_FILE:
            try:
                Path(AUDIO_CHANGE_FILE[uid]['path']).unlink(missing_ok=True)
                if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                    await c.delete_messages(m.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
            except Exception:
                pass
            AUDIO_CHANGE_FILE.pop(uid, None)
            
        status_msg = None
        tmp_in = None
        
        try:
            status_msg = await m.reply_text("অডিও ট্র্যাকের তথ্য পাওয়ার জন্য ফাইল ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())
            
            # Use original file name
            original_file_name = file_obj.file_name or f"download_{int(datetime.now().timestamp())}.mkv"
            tmp_in = TMP / f"audio_in_{uid}_{int(datetime.now().timestamp())}_{original_file_name}"
            
            # Download the file
            await c.download_media(
                m,
                file_name=str(tmp_in),
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, time.time(), "ডাউনলোড"),
            )
            
            if cancel_event.is_set():
                raise Exception("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।")

            # Get Audio Tracks
            audio_tracks = get_audio_tracks_ffprobe(tmp_in)
            
            if not audio_tracks:
                await status_msg.edit("এই ফাইলে কোনো অডিও ট্র্যাক পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হচ্ছে।", reply_markup=None)
                tmp_in.unlink(missing_ok=True)
                return

            track_info_text = "**অডিও ট্র্যাকের তালিকা:**\n"
            for i, track in enumerate(audio_tracks, 1):
                track_info_text += (
                    f"**{i}.** ইন্ডেক্স: `{track['stream_index']}` | ভাষা: `{track['language']}` | শিরোনাম: `{track['title']}`\n"
                )
                
            track_info_text += "\n**এখন নতুন অডিও ট্র্যাক অর্ডার দিন (কমা দিয়ে সেপারেটেড)।**\n"
            track_info_text += f"মোট ট্র্যাক: {len(audio_tracks)}। উদাহরণ: `3,2,1` (যদি আপনি ৩, ২, ১ ক্রমে চান)।"

            # Store state and wait for next message (audio order)
            AUDIO_CHANGE_FILE[uid] = {
                'path': str(tmp_in),
                'original_name': original_file_name,
                'tracks': audio_tracks,
                'message_id': status_msg.id # Store the status message ID to delete later
            }
            
            await status_msg.edit(track_info_text, parse_mode=ParseMode.MARKDOWN, reply_markup=None)


        except Exception as e:
            if not cancel_event.is_set():
                logger.error(f"Error in video_document_handler (audio mode): {e}", exc_info=True)
                if status_msg:
                    try:
                        await status_msg.edit(f"অডিও পরিবর্তনের প্রস্তুতি ব্যর্থ: {e}", reply_markup=None)
                    except Exception:
                        pass
            
            if tmp_in and tmp_in.exists():
                tmp_in.unlink(missing_ok=True)
                
        finally:
            if cancel_event in TASKS.get(uid, []):
                TASKS[uid].remove(cancel_event)
                
        return
    # --- END NEW: Handle MKV Audio Change Mode ---

    # --- Fallback: Regular File Upload / Forward ---
    
    # If it's a forwarded message, handle it as a regular file for rename/caption.
    if m.forward_date:
        # The logic here assumes that for renaming/captioning a forwarded media, 
        # the user must explicitly use a command like /rename or /upload_url.
        pass
        
    else:
        # For a newly uploaded video/document without a command, 
        # Pyrogram's automatic handling takes over.
        pass
    
# --- ASYNC TASK: Handle MKV Audio Remux ---
async def handle_audio_remux(c: Client, m: Message, in_path: Path, original_name: str, stream_map: list, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    status_msg = None
    tmp_out = TMP / f"audio_out_{uid}_{int(datetime.now().timestamp())}_{original_name}"
    
    try:
        if messages_to_delete:
            # Delete the track list message and user input message
            try:
                await c.delete_messages(m.chat.id, messages_to_delete)
            except Exception:
                pass
                
        status_msg = await m.reply_text("অডিও ট্র্যাক পরিবর্তন শুরু হচ্ছে (Remux)...", reply_markup=progress_keyboard())
        
        # Build the command
        map_args = []
        for map_item in stream_map:
            map_args.extend(["-map", map_item])
            
        cmd = [
            "ffmpeg",
            "-i", str(in_path),
            "-c", "copy",
            *map_args, # Dynamic stream mapping
            "-y",
            str(tmp_out)
        ]
        
        logger.info(f"FFMPEG command: {' '.join(cmd)}")
        
        # Run the FFMPEG command (non-blocking in a thread or separate process)
        # Using subprocess.run in a thread for long-running blocking tasks
        def run_ffmpeg():
            try:
                subprocess.run(
                    cmd, 
                    check=True, 
                    capture_output=True, 
                    timeout=7200 # 2 hours timeout for remux
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"FFMPEG remux failed: {e.stderr.decode()}")
                raise Exception(f"Remux failed. Error: {e.stderr.decode()}")
            except Exception as e:
                logger.error(f"FFMPEG remux unknown error: {e}")
                raise Exception(f"Remux failed. Unknown error: {e}")

        await asyncio.to_thread(run_ffmpeg)

        if cancel_event.is_set():
            raise Exception("অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।")

        await status_msg.edit("ট্র্যাক পরিবর্তন সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=progress_keyboard())
        
        # Proceed to upload the remuxed file
        await handle_file_upload(c, m, tmp_out, original_name, status_msg, cancel_event)

    except Exception as e:
        if not cancel_event.is_set():
            logger.error(f"Error in handle_audio_remux: {e}", exc_info=True)
            if status_msg:
                try:
                    await status_msg.edit(f"অডিও ট্র্যাক পরিবর্তন ব্যর্থ: {e}", reply_markup=None)
                except Exception:
                    pass
            
    finally:
        # Cleanup temp files
        in_path.unlink(missing_ok=True) # Delete the input file
        tmp_out.unlink(missing_ok=True) # Delete the output file (if it wasn't deleted by handle_file_upload)
        
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)


@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return

    if uid in TASKS and TASKS[uid]:
        # Cancel all pending tasks for this user
        for cancel_event in TASKS[uid]:
            cancel_event.set()
        
        # Clear the list
        TASKS.pop(uid)
        
        # Clean up audio change pending state if present
        if uid in AUDIO_CHANGE_FILE:
            file_data = AUDIO_CHANGE_FILE.pop(uid)
            Path(file_data['path']).unlink(missing_ok=True)
            try:
                await c.delete_messages(cb.message.chat.id, file_data.get('message_id'))
            except Exception:
                pass

        try:
            await cb.message.edit_text("❌ অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            await cb.answer("❌ অপারেশন বাতিল করা হয়েছে।", show_alert=True)
    else:
        await cb.answer("কোনো চলমান টাস্ক নেই।", show_alert=True)
        try:
            await cb.message.delete()
        except Exception:
            pass
            

# ---- FLASK AND MAIN EXECUTION ----
@flask_app.route('/')
def home():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Bot Status</title>
    </head>
    <body>
        <h1>Telegram Bot Status</h1>
        <p>Bot is running...</p>
    </body>
    </html>
    """
    return render_template_string(html_content)

def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        # A placeholder to avoid runtime error if RENDER_EXTERNAL_HOSTNAME is not set. 
        # In a real environment, this might be a default value or raise a warning.
        print("Render URL is not set. Ping service is disabled.")
        return

    url = f"http://{RENDER_EXTERNAL_HOSTNAME}"
    while True:
        try:
            response = requests.get(url, timeout=10)
            print(f"Pinged {url} | Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error pinging {url}: {e}")
        time.sleep(600) # Ping every 10 minutes

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
                        # Delete files older than 3 days
                        if now - datetime.fromtimestamp(p.stat().st_mtime) > timedelta(days=3):
                            p.unlink()
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(3600) # Check every hour

if __name__ == "__main__":
    print("Bot চালু হচ্ছে... Flask and Ping threads start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask_and_ping, daemon=True)
    t.start()
    try:
        # Start Pyrogram client and keep it running
        app.start()
        # Run cleanup task in the event loop
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
        # Block until Pyrogram client disconnects
        app.join()
    except KeyboardInterrupt:
        print("Bot বন্ধ হচ্ছে...")
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}", exc_info=True)
    finally:
        print("Bot বন্ধ হয়েছে।")
