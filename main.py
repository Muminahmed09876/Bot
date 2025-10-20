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

# --- UTILITY: Generate Post Caption (UPDATED) ---
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
                    season_entries.append(f"**{image_name} Season {i:02d}**") # CHANGE: Included image_name
            except ValueError:
                continue
        else:
            try:
                num = int(part)
                season_entries.append(f"**{image_name} Season {num:02d}**") # CHANGE: Included image_name
            except ValueError:
                continue

    # Remove duplicates and ensure list has at least "Coming Soon..."
    unique_season_entries = list(dict.fromkeys(season_entries))
    if not unique_season_entries:
        unique_season_entries.append("**Coming Soon...**")
    # Add Coming Soon if it's not the last entry and there are other entries
    elif unique_season_entries[-1] != "**Coming Soon...**" and unique_season_entries[0] != "**Coming Soon...**":
        unique_season_entries.append("**Coming Soon...**")
        
    # season_text is now just a list of the bolded season/coming soon entries, separated by \n
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
        "> " # ফাঁকা লাইন যোগ
    ]
    
    # Add each season entry, prepending a quote character '>' and adding a blank line after it.
    for line in season_text.split('\n'):
        collapsible_text_parts.append(f"> {line}") # Season line
        collapsible_text_parts.append("> ") # Blank line after season
        
    # Remove the extra blank quote line added after the last season/Coming Soon... entry
    if collapsible_text_parts and collapsible_text_parts[-1] == "> ":
        collapsible_text_parts.pop()
        
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
            except Exception:
                pass
            TASKS[uid].remove(cancel_event)
            return

        # Handle MKV Audio Change Mode logic
        if uid in MKV_AUDIO_CHANGE_MODE:
            # Check if it's a video/mkv file
            is_video = True 
            if is_video:
                # Use ffprobe to get audio tracks
                audio_tracks = get_audio_tracks_ffprobe(tmp_in)
                
                if not audio_tracks:
                    # If no audio tracks found, proceed to normal upload with a warning
                    await status_msg.edit("অডিও ট্র্যাক পাওয়া যায়নি। সাধারণ আপলোড হচ্ছে...", reply_markup=None)
                elif len(audio_tracks) == 1:
                    # If only one track, no change needed, proceed to normal upload
                    await status_msg.edit("ফাইলে শুধু একটি অডিও ট্র্যাক আছে। সাধারণ আপলোড হচ্ছে...", reply_markup=None)
                else:
                    # Multiple tracks found, ask for order
                    track_list_message = "ফাইলে একাধিক অডিও ট্র্যাক পাওয়া গেছে। নতুন ট্র্যাক অর্ডারে (কমা-সেপারেটেড) নম্বরগুলো লিখুন:\n"
                    for i, track in enumerate(audio_tracks, 1):
                        track_list_message += f"**{i}**. Index: `{track['stream_index']}`, Language: `{track['language']}`, Title: `{track['title']}`\n"
                    track_list_message += "\nউদাহরণ: `2,1` (দ্বিতীয় ট্র্যাকটি প্রথমে এবং প্রথম ট্র্যাকটি দ্বিতীয়তে যাবে)"
                    
                    # Store data and wait for user input
                    AUDIO_CHANGE_FILE[uid] = {
                        'path': str(tmp_in), 
                        'original_name': safe_name, 
                        'tracks': audio_tracks
                    }
                    
                    # Send track list and store its ID for later deletion
                    list_msg = await status_msg.edit(track_list_message, reply_markup=None, parse_mode=ParseMode.MARKDOWN)
                    AUDIO_CHANGE_FILE[uid]['message_id'] = list_msg.id
                    
                    TASKS[uid].remove(cancel_event)
                    return # Exit: waiting for user input
            # If not a video (e.g., photo, document), proceed to normal upload.

        # Rename and Upload (Normal flow or if not in MKV Audio Change Mode/single track)
        
        # --- Handle Renaming ---
        # Only rename if Edit Caption Mode is OFF
        if uid not in EDIT_CAPTION_MODE:
            new_fname = generate_new_filename(safe_name)
            tmp_out = TMP / new_fname
            try:
                tmp_in.rename(tmp_out)
                tmp_in = tmp_out # Update the path to the renamed file
                safe_name = new_fname
            except Exception as e:
                logger.error(f"File rename error: {e}")
                
        # --- Get Thumbnail (if file is a video) ---
        thumb_path = USER_THUMBS.get(uid)
        thumb_time = USER_THUMB_TIME.get(uid)
        
        if not thumb_path and safe_name.lower().endswith(tuple(video_exts)):
            try:
                # Default to 5 seconds if no time is set
                time_offset = thumb_time if thumb_time is not None else 5
                
                # Check video duration to ensure offset is not too large
                duration = get_video_duration(tmp_in)
                if duration == 0:
                    time_offset = 5 # Fallback if duration extraction fails
                elif time_offset >= duration:
                    # If offset is beyond video duration, use a safe offset (e.g., 5 seconds from start)
                    time_offset = min(5, duration - 1) if duration > 1 else 0

                thumb_path = TMP / f"auto_thumb_{uid}.jpg"
                
                # FFmpeg command to extract frame at time_offset
                cmd = [
                    "ffmpeg",
                    "-i", str(tmp_in),
                    "-ss", str(time_offset),
                    "-vframes", "1",
                    "-s", "320x180", # Standard small size for thumb
                    "-an", # No audio
                    "-y", # Overwrite output files without asking
                    str(thumb_path)
                ]
                
                subprocess.run(cmd, check=True, capture_output=True, timeout=60)

                # Validate and resize the generated thumbnail
                if Path(thumb_path).exists():
                    img = Image.open(thumb_path)
                    img.thumbnail((320, 320))
                    img = img.convert("RGB")
                    img.save(thumb_path, "JPEG")
                else:
                    thumb_path = None

            except Exception as e:
                logger.error(f"Auto thumb generation error: {e}")
                thumb_path = None
        
        # --- Get Caption ---
        caption_text = USER_CAPTIONS.get(uid)
        
        if caption_text:
            # Dynamic caption processing
            
            # 1. Episode Number Increment/Display
            if uid not in USER_COUNTERS:
                USER_COUNTERS[uid] = 1 # Initialize counter if not present
                
            current_count = USER_COUNTERS[uid]
            next_count = current_count + 1
            
            # The pattern for simple increment: [01], [(01)]
            # Matches any two-digit number in [NN] or [(NN)]
            def replace_counter(match):
                # Always replaces with the current count (01, 02, etc.)
                return f"{current_count:02d}"

            # 2. Conditional Text (New Feature)
            # Pattern: [TEXT (XX)] where XX is episode number
            # Example: [End (02)] -> 'End' if current_count == 2
            def replace_conditional(match):
                text = match.group(1).strip()
                ep_num_str = match.group(2).strip()
                try:
                    ep_num = int(ep_num_str)
                    if current_count == ep_num:
                        return text
                except ValueError:
                    pass # Ignore if episode number is not a valid integer
                return "" # Replace with empty string if condition not met

            # Process conditional text first (it might contain NN patterns)
            caption_text = re.sub(r'\[(.*)\s*\((.*?)\)\]', replace_conditional, caption_text)
            
            # Process counter after conditional to ensure NN in conditional text is not replaced
            caption_text = re.sub(r'\[(\(?)(\d{2})(\)?)\]', replace_counter, caption_text)
            
            # 3. Quality Cycle (Placeholder/Future use, keep the current pattern matching)
            # The pattern for quality cycle: [re (480p, 720p)]
            # Note: The current code does not implement the cycle logic, it just replaces the first matching group with the second group.
            # I will implement a basic cycling for now.
            def replace_cycle(match):
                options = [o.strip() for o in match.group(2).split(',')]
                if not options:
                    return ""
                # Cycle through the options based on episode number (simple modulo)
                index = (current_count - 1) % len(options)
                return options[index]
            
            # The pattern for quality cycle: [re (item1, item2, ...)]
            caption_text = re.sub(r'\[re\s*\((.*?)\)\]', replace_cycle, caption_text)
            
            # Update counter for next run
            USER_COUNTERS[uid] = next_count

        try:
            await status_msg.edit("আপলোড হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
            status_msg = await m.reply_text("আপলোড হচ্ছে...", reply_markup=progress_keyboard())

        # Upload
        try:
            if safe_name.lower().endswith(tuple(video_exts)):
                await c.send_video(
                    chat_id=m.chat.id,
                    video=str(tmp_in),
                    caption=caption_text,
                    file_name=safe_name,
                    thumb=thumb_path,
                    duration=get_video_duration(tmp_in),
                    progress=pyrogram_progress_wrapper,
                    progress_args=(status_msg, time.time(), "ভিডিও আপলোড"),
                    supports_streaming=True
                )
            else:
                await c.send_document(
                    chat_id=m.chat.id,
                    document=str(tmp_in),
                    caption=caption_text,
                    file_name=safe_name,
                    thumb=thumb_path,
                    progress=pyrogram_progress_wrapper,
                    progress_args=(status_msg, time.time(), "ফাইল আপলোড")
                )
            
            await status_msg.delete()

        except Exception as e:
            logger.error(f"Upload error: {e}")
            await status_msg.edit(f"আপলোড ব্যর্থ: {e}", reply_markup=None)

    finally:
        # Cleanup
        TASKS[uid].remove(cancel_event)
        try:
            if tmp_in.exists():
                tmp_in.unlink()
            if thumb_path and Path(thumb_path).exists() and thumb_path not in USER_THUMBS.values():
                Path(thumb_path).unlink()
        except Exception:
            pass
        

# --- HANDLER: /rename ---
@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    if not m.reply_to_message:
        await m.reply_text("একটি ভিডিও বা ফাইল রিপ্লাই করুন এবং নতুন নাম দিন।\nব্যবহার: `/rename new_name.mp4` (reply to a file)")
        return
        
    if not m.command or len(m.command) < 2:
        await m.reply_text("নতুন ফাইলের নাম দিন।\nব্যবহার: `/rename new_name.mp4`")
        return

    new_name = m.text.split(None, 1)[1].strip()
    
    # Check for file/video/document
    msg = m.reply_to_message
    if msg.video:
        file_id = msg.video.file_id
    elif msg.document:
        file_id = msg.document.file_id
    else:
        await m.reply_text("আপনি একটি ভিডিও বা ফাইলে রিপ্লাই করেননি।")
        return

    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    
    try:
        # Download the file
        tmp_in = TMP / f"rename_dl_{uid}_{int(time.time())}.dl"
        await msg.download(file_name=str(tmp_in), progress=pyrogram_progress_wrapper, progress_args=(status_msg, time.time(), "রিনেম করার জন্য ডাউনলোড"))
        
        # Determine the final output path
        tmp_out = TMP / new_name
        
        # Rename the file on the local file system (fast)
        tmp_in.rename(tmp_out)
        
        # Get thumbnail
        thumb_path = USER_THUMBS.get(uid) or msg.video.thumb.file_id if msg.video and msg.video.thumb else None
        
        # Get caption
        caption_text = USER_CAPTIONS.get(uid)
        
        # Upload the renamed file
        await status_msg.edit("আপলোড হচ্ছে...", reply_markup=progress_keyboard())

        if msg.video:
            await c.send_video(
                chat_id=m.chat.id,
                video=str(tmp_out),
                caption=caption_text,
                file_name=new_name,
                thumb=thumb_path,
                duration=msg.video.duration,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, time.time(), "রিনেম আপলোড"),
                supports_streaming=True
            )
        elif msg.document:
            await c.send_document(
                chat_id=m.chat.id,
                document=str(tmp_out),
                caption=caption_text,
                file_name=new_name,
                thumb=thumb_path,
                progress=pyrogram_progress_wrapper,
                progress_args=(status_msg, time.time(), "রিনেম আপলোড")
            )
            
        await status_msg.delete()
        await msg.delete() # Delete original message after successful rename/upload

    except Exception as e:
        logger.error(f"Rename error: {e}")
        await status_msg.edit(f"রিনেম ব্যর্থ: {e}", reply_markup=None)
    finally:
        TASKS[uid].remove(cancel_event)
        try:
            if tmp_in.exists(): tmp_in.unlink()
            if tmp_out.exists(): tmp_out.unlink()
        except Exception:
            pass


# --- CORE: Handle Audio Remuxing ---
async def handle_audio_remux(c: Client, m: Message, file_path: str, original_name: str, stream_map: list, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    tmp_in = Path(file_path)
    tmp_out = TMP / f"remux_{uid}_{int(time.time())}_{original_name}"
    
    status_msg = await m.reply_text("অডিও অর্ডার পরিবর্তন (Remuxing) শুরু হচ্ছে...", reply_markup=progress_keyboard())
    
    try:
        # Build the stream map arguments: -map 0:v -map [stream_map[0]] -map [stream_map[1]] ...
        map_args = ["-map", "0:v"]
        for stream in stream_map:
            # stream format is "0:index" where index is the stream index from ffprobe
            map_args.extend(["-map", stream]) 
            
        # FFmpeg command for remuxing (no re-encoding)
        cmd = [
            "ffmpeg",
            "-i", str(tmp_in),
            "-c", "copy" # Copy streams without re-encoding
        ]
        cmd.extend(map_args)
        cmd.extend([
            "-y", # Overwrite output
            str(tmp_out)
        ])
        
        subprocess.run(cmd, check=True, capture_output=True, timeout=3600) # 1 hour timeout
        
        # --- Get Thumbnail ---
        thumb_path = USER_THUMBS.get(uid)
        
        # --- Get Caption ---
        caption_text = USER_CAPTIONS.get(uid)
        
        # Upload the remuxed file
        await status_msg.edit("আপলোড হচ্ছে...", reply_markup=progress_keyboard())

        # Determine duration
        duration = get_video_duration(tmp_out)

        # Upload (Assuming it's a video based on the mode)
        await c.send_video(
            chat_id=m.chat.id,
            video=str(tmp_out),
            caption=caption_text,
            file_name=original_name, # Use original name
            thumb=thumb_path,
            duration=duration,
            progress=pyrogram_progress_wrapper,
            progress_args=(status_msg, time.time(), "MKV রিমাক্স আপলোড"),
            supports_streaming=True
        )
            
        await status_msg.delete()
        
        # Delete related conversation messages
        if messages_to_delete:
            try:
                await c.delete_messages(m.chat.id, messages_to_delete)
            except Exception as e:
                logger.warning(f"Error deleting audio remux messages: {e}")


    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode('utf-8', errors='ignore') if e.stderr else "Unknown FFmpeg error."
        logger.error(f"FFmpeg Remuxing error: {error_output}")
        await status_msg.edit(f"অডিও পরিবর্তন ব্যর্থ: FFmpeg এরর।\n{error_output[:500]}...", reply_markup=None)
    except Exception as e:
        logger.error(f"Remux/Upload error: {e}")
        await status_msg.edit(f"অডিও পরিবর্তন ব্যর্থ: {e}", reply_markup=None)
    finally:
        # Cleanup
        TASKS[uid].remove(cancel_event)
        try:
            if tmp_in.exists(): tmp_in.unlink()
            if tmp_out.exists(): tmp_out.unlink()
        except Exception:
            pass


@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: `/broadcast <text>`")
        return
        
    text = m.text.split(None, 1)[1]
    
    success = 0
    failed = 0
    total = len(SUBSCRIBERS)
    
    status_msg = await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে... {total} ব্যবহারকারী।", reply_markup=None)
    
    for uid in list(SUBSCRIBERS):
        if uid == m.chat.id:
            continue
        try:
            await c.send_message(chat_id=uid, text=text)
            success += 1
            await asyncio.sleep(0.1) # Small delay to avoid flood waits
        except Exception as e:
            logger.warning(f"Broadcast to {uid} failed: {e}")
            failed += 1
            # Remove from subscriber list if error is due to user block/not found
            if "USER_IS_BLOCKED" in str(e) or "PEER_ID_INVALID" in str(e):
                SUBSCRIBERS.discard(uid)

    await status_msg.edit(f"ব্রডকাস্ট শেষ হয়েছে।\nসাফল্য: {success}\nব্যর্থ: {failed}\nমোট: {total}")

@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb: CallbackQuery):
    uid = cb.from_user.id
    if uid not in TASKS or not TASKS[uid]:
        await cb.answer("কোনো চলমান টাস্ক নেই।", show_alert=True)
        return
        
    for event in TASKS[uid]:
        event.set() # Set the cancel flag for all tasks
        
    TASKS[uid].clear()
    
    # Check for pending audio remux file and clean up
    if uid in AUDIO_CHANGE_FILE:
        file_data = AUDIO_CHANGE_FILE.pop(uid)
        try:
            Path(file_data['path']).unlink(missing_ok=True)
            if 'message_id' in file_data:
                await c.delete_messages(cb.message.chat.id, file_data['message_id'])
        except Exception:
            pass
            
    # Check for pending post creation state and clean up
    if uid in CREATE_POST_MODE:
        CREATE_POST_MODE.discard(uid)
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
                    await c.delete_messages(cb.message.chat.id, messages_to_delete)
            except Exception as e:
                logger.warning(f"Task cancel post cleanup error: {e}")
        
    try:
        await cb.message.edit_text("টাস্ক বাতিল করা হয়েছে।", reply_markup=None)
    except Exception:
        await cb.answer("টাস্ক বাতিল করা হয়েছে।")

# ---- Flask Server for Keep Alive ----
flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    # Show basic info or status
    html = f"""
    <html>
        <head>
            <title>Bot Status</title>
        </head>
        <body>
            <h1>Bot Running</h1>
            <p>Status: Active</p>
            <p>Uptime: {datetime.now()}</p>
        </body>
    </html>
    """
    return render_template_string(html)

def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
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
    except KeyboardInterrupt:
        print("বট বন্ধ হচ্ছে।")
    except Exception as e:
        print(f"একটি ত্রুটি হয়েছে: {e}")
