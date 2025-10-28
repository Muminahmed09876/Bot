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
        f"> **{image_name} All Season List :-**", 
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
        BotCommand("create_post", "নতুন পোস্ট তৈরি করুন (admin only)"), # NEW COMMAND
        BotCommand("mode_check", "বর্তমান মোড স্ট্যাটাস চেক করুন (admin only)"), 
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
        "/create_post - নতুন পোস্ট তৈরি করুন (admin only)\n" # NEW COMMAND in help
        "/mode_check - বর্তমান মোড স্ট্যাটাস চেক করুন এবং পরিবর্তন করুন (admin only)\n" 
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
            if out.exists(): out.unlink(missing_ok=True)
        return
    # --- END NEW: Handle Create Post Mode ---
    
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
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অন** করা হয়েছে। এখন আপনি একটি **MKV ফাইল** অথবা অন্য কোনো **ভিডিও ফাইল** পাঠান।\n(এই মোড ম্যানুয়ালি অফ না করা পর্যন্ত চালু থাকবে।)")

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

# --- NEW COROUTINE TO HANDLE VIDEO/DOCUMENT INPUT FOR AUDIO CHANGE ---
async def start_audio_change_process(c: Client, m: Message, file_path: Path, original_file_name: str, tracks: list):
    uid = m.from_user.id
    track_count = len(tracks)

    # --- MODIFICATION 1: Auto Upload for Single Audio Track ---
    if track_count == 1:
        # শুধুমাত্র ১টি ট্র্যাক থাকলে, এটিই নতুন অর্ডার হিসেবে ধরে অটো-আপলোড হবে।
        stream_index = tracks[0]['stream_index']
        new_stream_map = [f"0:{stream_index}"]
        await m.reply_text(f"✅ শুধুমাত্র ১টি অডিও ট্র্যাক পাওয়া গেছে। কোনো ট্র্যাক অর্ডার দিতে হবে না। অটো আপলোড শুরু হচ্ছে।", reply_to_message_id=m.id)
        
        # Start the audio remux process (handle_audio_remux function is assumed to exist)
        asyncio.create_task(
            handle_audio_remux(
                c, m, file_path, 
                original_file_name, 
                new_stream_map, 
                messages_to_delete=[] 
            )
        )
        # যেহেতু কোনো অর্ডার চাওয়া হয়নি, তাই AUDIO_CHANGE_FILE state সেট করার প্রয়োজন নেই।
        return
    # ------------------------------------------------------------
    
    # একাধিক ট্র্যাকের জন্য: অর্ডার জিজ্ঞাসা করুন
    
    # 1. Format the tracks list for display
    track_list_text = "ফাইলের অডিও ট্র্যাকগুলি হল:\n"
    for i, track in enumerate(tracks):
        # i+1 হলো ইউজার-ফেসিং ট্র্যাক নম্বর
        track_list_text += f"{i+1}. Index: `{track['stream_index']}` | Title: `{track['title']}` | Lang: `{track['language']}`\n"
        
    track_list_text += "\n**এখন নতুন অডিও ট্র্যাক অর্ডার কমা-সেপারেটেড সংখ্যায় দিন।**\n"
    track_list_text += f"মোট ট্র্যাক: {track_count}টি। (৫টি বা তার বেশি ট্র্যাক থাকলে, আপনি কম সংখ্যা দিতে পারেন।)\nউদাহরণ: `3,2,1` বা `2,1`"

    # 2. Store state and prompt user
    prompt_msg = await m.reply_text(track_list_text, reply_to_message_id=m.id)
    
    AUDIO_CHANGE_FILE[uid] = {
        'path': str(file_path),
        'original_name': original_file_name,
        'tracks': tracks,
        'message_id': prompt_msg.id
    }


# Insert this new handler after the existing handlers (for downloaded/forwarded videos/documents)
@app.on_message((filters.video | filters.document) & filters.private & ~filters.command(["rename", "upload_url"]))
async def general_video_document_handler(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    
    # Check if the user is in MKV Audio Change Mode AND not currently awaiting input for a previous file
    if uid in MKV_AUDIO_CHANGE_MODE and uid not in AUDIO_CHANGE_FILE:
        
        # Only process if it's a video or a document that looks like a video (e.g., mkv, mp4)
        file_ref = m.video or (m.document if m.document and m.document.mime_type.startswith('video/') else None)

        if file_ref:
            status_msg = await m.reply_text("ফাইল ডাউনলোড হচ্ছে...")
            tmp_in = None
            try:
                # 1. Download the file
                original_name = file_ref.file_name or "video_file.mkv"
                tmp_in = TMP / f"mkv_audio_in_{uid}_{int(datetime.now().timestamp())}_{original_name}"
                
                if file_ref.file_size and file_ref.file_size > MAX_SIZE:
                    await status_msg.edit("ফাইলের সাইজ 4GB এর বেশি হতে পারে না।")
                    return

                await m.download(file_name=str(tmp_in), progress=pyrogram_progress_wrapper, progress_args=(status_msg, time.time(), "ডাউনলোড"))
                
                # 2. Get audio tracks
                tracks = get_audio_tracks_ffprobe(tmp_in)
                
                if not tracks:
                    await status_msg.edit("এই ফাইলে কোনো অডিও ট্র্যাক পাওয়া যায়নি।")
                    tmp_in.unlink(missing_ok=True)
                    return
                
                # 3. Start the process (which includes the single track check - Modification 1)
                await status_msg.delete() 
                await start_audio_change_process(c, m, tmp_in, original_name, tracks)
                
            except Exception as e:
                logger.error(f"Error in MKV audio change process start: {e}")
                error_message = f"ফাইল প্রসেসিং-এ সমস্যা: {type(e).__name__}"
                if status_msg.text != error_message: 
                    await status_msg.edit(error_message)
                if tmp_in and tmp_in.exists():
                    tmp_in.unlink(missing_ok=True)
                AUDIO_CHANGE_FILE.pop(uid, None) # Clear file state but keep mode ON
        # অন্য ফাইল হলে, কিছু করার দরকার নেই।


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

    # --- Handle audio order input if in mode and file is set (MODIFIED BLOCK) ---
    if uid in MKV_AUDIO_CHANGE_MODE and uid in AUDIO_CHANGE_FILE:
        file_data = AUDIO_CHANGE_FILE.get(uid)
        if not file_data or not file_data.get('tracks'):
            await m.reply_text("অডিও ট্র্যাকের তথ্য পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হচ্ছে।")
            AUDIO_CHANGE_FILE.pop(uid, None)
            return

        tracks = file_data['tracks']
        total_tracks = len(tracks)
        try:
            # Parse the input like "3,2,1"
            # শুধুমাত্র সংখ্যাগুলো কমা-সেপারেটেড হিসেবে বিবেচনা করা হচ্ছে।
            new_order_str = [x.strip() for x in text.split(',') if x.strip().isdigit()] 
            
            # --- MODIFICATION 2: Audio Deletion Logic (5+ tracks) ---
            input_track_count = len(new_order_str)
            valid_user_indices = list(range(1, total_tracks + 1))
            
            if input_track_count == 0:
                 await m.reply_text("কমপক্ষে ১টি ট্র্যাক নম্বর দিন।")
                 return
                 
            # Check for valid indices and duplicates
            unique_user_tracks = set()
            
            for user_track_num_str in new_order_str:
                user_track_num = int(user_track_num_str)
                
                if user_track_num not in valid_user_indices:
                     await m.reply_text(f"ভুল ট্র্যাক নম্বর: {user_track_num}। ট্র্যাক নম্বরগুলো হতে হবে: {', '.join(map(str, valid_user_indices))}")
                     return
                
                if user_track_num in unique_user_tracks:
                    await m.reply_text("ইনপুটে একই ট্র্যাক নম্বর একাধিকবার দেওয়া যাবে না।")
                    return
                unique_user_tracks.add(user_track_num)


            if total_tracks < 5:
                # For 1-4 tracks: strict match required
                if input_track_count != total_tracks:
                     await m.reply_text(f"এই ফাইলে মোট {total_tracks}টি অডিও ট্র্যাক আছে। (৫টির কম হওয়ায়) **সবগুলো ট্র্যাকের অর্ডারে** কমা-সেপারেটেড সংখ্যা দিন।")
                     return
            else:
                # For 5+ tracks: allow fewer tracks (deletion)
                if input_track_count > total_tracks: 
                     await m.reply_text(f"আপনার ইনপুট করা ট্র্যাকের সংখ্যা ({input_track_count}) মোট ট্র্যাকের ({total_tracks}) থেকে বেশি হতে পারে না।")
                     return
            
            
            # Build the stream map based only on the provided and validated tracks (new_order_str)
            # যেহেতু new_stream_map-এ শুধু ইউজার-দেওয়া ট্র্যাকগুলি যুক্ত হচ্ছে, তাই বাকি ট্র্যাকগুলি ডিলেট হয়ে যাবে।
            new_stream_map = []
            for user_track_num_str in new_order_str:
                user_track_num = int(user_track_num_str)
                # user_track_num is 1-indexed, tracks list is 0-indexed
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
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
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
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
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
                await m.reply_text("ক্যাপশন এডিট করতে সমস্যা হয়েছে। প্রক্রিয়া বাতিল করা হচ্ছে। /create_post দিয়ে মোড অফ করুন।")
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
            if tmp_in.exists():
                tmp_in.unlink(missing_ok=True)
            return

        await status_msg.edit("ফাইল ডাউনলোড সম্পন্ন হয়েছে। আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        
        # Determine final filename and caption
        final_filename = tmp_in.name
        final_caption = USER_CAPTIONS.get(uid)
        
        # RENAME LOGIC
        if uid not in EDIT_CAPTION_MODE:
            # New filename logic from original code
            new_base_name = generate_new_filename(final_filename)
            tmp_out = tmp_in.parent / new_base_name
            # Rename the file temporarily (or just use the new name for upload)
            tmp_in.rename(tmp_out)
            tmp_in = tmp_out
            final_filename = new_base_name # Update final_filename for upload

        # Apply Dynamic Caption logic
        if final_caption:
            final_caption = apply_dynamic_caption(uid, final_caption, final_filename)

        # GET THUMBNAIL
        thumb_path = USER_THUMBS.get(uid)
        thumb_time = USER_THUMB_TIME.get(uid)
        
        if not thumb_path and thumb_time and tmp_in.suffix.lower() in video_exts:
            duration = get_video_duration(tmp_in)
            seek_time = min(thumb_time, duration - 1)
            thumb_path = TMP / f"auto_thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            if generate_thumbnail(tmp_in, thumb_path, seek_time):
                thumb_path = str(thumb_path)
            else:
                thumb_path = None

        await c.send_document(
            chat_id=m.chat.id,
            document=str(tmp_in),
            caption=final_caption,
            file_name=final_filename,
            thumb=thumb_path,
            progress=pyrogram_progress_wrapper,
            progress_args=(status_msg, time.time(), "আপলোড")
        )
        await status_msg.delete()

    except asyncio.CancelledError:
        try:
            await status_msg.edit("অপারেশন বাতিল করা হয়েছে।")
        except:
            pass
        if 'tmp_in' in locals() and tmp_in.exists():
            tmp_in.unlink(missing_ok=True)
        return
    except Exception as e:
        logger.error(f"Upload task failed: {e}")
        try:
            await status_msg.edit(f"আপলোড ব্যর্থ: {e}")
        except:
            await m.reply_text(f"আপলোড ব্যর্থ: {e}")
        if 'tmp_in' in locals() and tmp_in.exists():
            tmp_in.unlink(missing_ok=True)
    finally:
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)
        # Cleanup auto-generated thumbnail
        if 'thumb_path' in locals() and thumb_path and Path(thumb_path).name.startswith("auto_thumb"):
            Path(thumb_path).unlink(missing_ok=True)

@app.on_message(filters.command("rename") & filters.private & filters.reply)
async def rename_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    if not m.reply_to_message or not m.reply_to_message.document:
        await m.reply_text("একটি ফাইল রিপ্লাই করুন।")
        return

    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /rename <newname.ext>\nউদাহরণ: /rename Episode 1.mkv")
        return

    new_name = m.text.split(None, 1)[1].strip()
    file_ref = m.reply_to_message.document
    uid = m.from_user.id
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)

    status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড হচ্ছে...", reply_markup=progress_keyboard())

    tmp_in = None
    try:
        original_name = file_ref.file_name
        tmp_in = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{original_name}"
        
        await m.reply_to_message.download(file_name=str(tmp_in), progress=pyrogram_progress_wrapper, progress_args=(status_msg, time.time(), "ডাউনলোড"))
        
        await status_msg.edit("ডাউনলোড সম্পন্ন হয়েছে। আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())

        # Determine thumbnail and caption
        final_caption = USER_CAPTIONS.get(uid)
        if final_caption:
            final_caption = apply_dynamic_caption(uid, final_caption, new_name)

        thumb_path = USER_THUMBS.get(uid)
        
        await c.send_document(
            chat_id=m.chat.id,
            document=str(tmp_in),
            caption=final_caption,
            file_name=new_name,
            thumb=thumb_path,
            progress=pyrogram_progress_wrapper,
            progress_args=(status_msg, time.time(), "আপলোড")
        )
        await status_msg.delete()
        await m.reply_to_message.delete()
        
    except asyncio.CancelledError:
        try:
            await status_msg.edit("অপারেশন বাতিল করা হয়েছে।")
        except:
            pass
        if tmp_in and tmp_in.exists():
            tmp_in.unlink(missing_ok=True)
        return
    except Exception as e:
        logger.error(f"Rename task failed: {e}")
        try:
            await status_msg.edit(f"রিনেম ব্যর্থ: {e}")
        except:
            await m.reply_text(f"রিনেম ব্যর্থ: {e}")
        if tmp_in and tmp_in.exists():
            tmp_in.unlink(missing_ok=True)
    finally:
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)

# --- NEW REMUX HANDLER FOR AUDIO CHANGE ---
async def handle_audio_remux(c: Client, m: Message, in_path: Path, original_name: str, new_stream_map: list, messages_to_delete: list):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    if messages_to_delete:
        try:
            await c.delete_messages(m.chat.id, messages_to_delete)
        except Exception as e:
            logger.warning(f"Failed to delete initial messages: {e}")

    status_msg = await m.reply_text("অডিও রিমুক্সিং শুরু হচ্ছে...", reply_markup=progress_keyboard())
    out_path = None
    try:
        # Construct the output filename (preserving original extension)
        original_ext = in_path.suffix
        out_name = f"remuxed_{uid}_{int(datetime.now().timestamp())}{original_ext}"
        out_path = TMP / out_name
        
        # Build the FFmpeg command
        # -i input_file (the downloaded file)
        # -map 0:v:0 (map the first video track)
        # -map [stream_map] (map the re-ordered audio tracks)
        # -c copy (copy all streams without re-encoding)
        map_args = ["-map", "0:v:0"] # Select the first video track
        for audio_map in new_stream_map:
            map_args.extend(["-map", audio_map]) # Add user's audio track order

        cmd = [
            "ffmpeg",
            "-i", str(in_path),
            *map_args, 
            "-c", "copy",
            "-metadata", f"title={original_name}", # Optional: keep some metadata
            "-y", # Overwrite output files
            str(out_path)
        ]

        logger.info(f"FFmpeg command: {' '.join(cmd)}")
        
        # Run the FFmpeg command
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Monitor the process for cancellation
        try:
            # Wait for FFmpeg to complete or for cancellation
            await status_msg.edit("অডিও ট্র্যাক পরিবর্তন চলছে (রিমুক্সিং)...", reply_markup=progress_keyboard())
            
            # Since FFmpeg output is hard to parse for accurate progress, we'll rely on completion time
            await asyncio.wait_for(process.wait(), timeout=None) 
            
            if cancel_event.is_set():
                 raise asyncio.CancelledError
            
        except asyncio.TimeoutError:
             # Should not happen with timeout=None, but added for safety
             pass
        except asyncio.CancelledError:
             process.terminate()
             await process.wait()
             raise

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_details = stderr.decode(errors='ignore')
            logger.error(f"FFmpeg failed with error: {error_details}")
            raise Exception(f"রিমুক্সিং ব্যর্থ হয়েছে। FFmpeg এরর: {error_details[:500]}...")

        if not out_path.exists():
            raise FileNotFoundError("রিমুক্সিং সফল হলেও আউটপুট ফাইল খুঁজে পাওয়া যায়নি।")

        await status_msg.edit("রিমুক্সিং সম্পন্ন হয়েছে। আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())

        # Final Upload preparation
        # Determine final filename and caption
        final_filename = original_name 
        final_caption = USER_CAPTIONS.get(uid)
        
        # RENAME LOGIC
        if uid not in EDIT_CAPTION_MODE:
            # New filename logic from original code
            new_base_name = generate_new_filename(original_name)
            final_filename = new_base_name # Update final_filename for upload

        # Apply Dynamic Caption logic
        if final_caption:
            final_caption = apply_dynamic_caption(uid, final_caption, final_filename)

        # GET THUMBNAIL (same logic as in handle_url_download_and_upload)
        thumb_path = USER_THUMBS.get(uid)
        thumb_time = USER_THUMB_TIME.get(uid)
        
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        
        if not thumb_path and thumb_time and out_path.suffix.lower() in video_exts:
            duration = get_video_duration(out_path)
            seek_time = min(thumb_time, duration - 1)
            thumb_path_auto = TMP / f"auto_thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            if generate_thumbnail(out_path, thumb_path_auto, seek_time): # Assume generate_thumbnail exists
                thumb_path = str(thumb_path_auto)
            else:
                thumb_path = None
        else:
            thumb_path_auto = None # No auto thumb generated

        await c.send_document(
            chat_id=m.chat.id,
            document=str(out_path),
            caption=final_caption,
            file_name=final_filename,
            thumb=thumb_path,
            progress=pyrogram_progress_wrapper,
            progress_args=(status_msg, time.time(), "আপলোড")
        )
        await status_msg.delete()

    except asyncio.CancelledError:
        try:
            await status_msg.edit("অপারেশন বাতিল করা হয়েছে।")
        except:
            pass
        if 'process' in locals() and process.returncode is None:
            process.terminate()
        return
    except Exception as e:
        logger.error(f"Audio remux failed: {e}")
        try:
            await status_msg.edit(f"অডিও ট্র্যাক পরিবর্তন ব্যর্থ: {e}")
        except:
            await m.reply_text(f"অডিও ট্র্যাক পরিবর্তন ব্যর্থ: {e}")
        
    finally:
        # Cleanup
        if in_path.exists():
            in_path.unlink(missing_ok=True)
        if out_path and out_path.exists():
            out_path.unlink(missing_ok=True)
        if cancel_event in TASKS.get(uid, []):
            TASKS[uid].remove(cancel_event)
        if 'thumb_path_auto' in locals() and thumb_path_auto and thumb_path_auto.exists():
            thumb_path_auto.unlink(missing_ok=True)

# --- DYNAMIC CAPTION LOGIC (assumed to be needed for the new handlers) ---
def apply_dynamic_caption(uid: int, caption: str, filename: str) -> str:
    # This function should implement the dynamic caption logic (e.g. [01], [re (...)] etc.)
    # Since the full original logic is not provided, this is a placeholder based on the snippet.
    
    # 1. Number Increment Logic
    match_num = re.search(r"\[\(?(\d+)\)?\]", caption)
    if match_num:
        original_num_str = match_num.group(1)
        original_format = match_num.group(0) # e.g., [01] or [(01)]
        original_num = int(original_num_str)

        # Get the counter for the user/format
        # Use the format string (e.g. "[01]") as the key for the counter
        counter_key = original_format
        current_counter = USER_COUNTERS.get(uid, {}).get(counter_key, original_num)
        
        # New counter for next upload
        new_counter = current_counter + 1
        
        # Format the new number string with correct padding
        padding = len(original_num_str)
        new_num_str = str(new_counter).zfill(padding)

        # Update the caption with the new number
        new_caption = caption.replace(original_num_str, new_num_str)
        
        # Store the updated counter
        if uid not in USER_COUNTERS:
            USER_COUNTERS[uid] = {}
        USER_COUNTERS[uid][counter_key] = new_counter

        # Update the main caption for further replacements
        caption = new_caption

    # 2. Quality Cycling Logic
    match_re = re.search(r"\[re\s*\((.*?)\)\]", caption)
    if match_re:
        options_str = match_re.group(1).strip()
        options = [o.strip() for o in options_str.split(',') if o.strip()]
        
        if options:
            re_key = match_re.group(0)
            
            # Determine which option to use based on the counter for this pattern
            re_counter = USER_COUNTERS.get(uid, {}).get(re_key, 0)
            selected_option = options[re_counter % len(options)]
            
            # Replace the pattern with the selected option
            caption = caption.replace(re_key, selected_option)
            
            # Update the counter for next use
            USER_COUNTERS[uid][re_key] = re_counter + 1

    # 3. Conditional Text Logic (requires the episode number to be available)
    # The original logic uses the incremented number from [01] as the episode number for conditional text
    if match_num:
        ep_num = new_counter # Use the number that was about to be set for the next episode
        
        # Pattern: [TEXT (XX)] - where XX is the target number
        for match in re.finditer(r"\[(.*?) \((.*?)\)\]", caption):
            full_match = match.group(0)
            text_to_insert = match.group(1)
            target_num_str = match.group(2)
            
            try:
                target_num = int(target_num_str)
                if ep_num == target_num:
                    caption = caption.replace(full_match, text_to_insert)
                else:
                    caption = caption.replace(full_match, "")
            except ValueError:
                # Ignore if target number is not an integer
                caption = caption.replace(full_match, "")

    return caption

# --- THUMBNAIL GENERATION (assumed to be needed) ---
def generate_thumbnail(video_path: Path, output_path: Path, seek_time: int) -> bool:
    """Generates a thumbnail from a video file using FFmpeg."""
    try:
        cmd = [
            "ffmpeg",
            "-i", str(video_path),
            "-ss", str(seek_time),
            "-vframes", "1",
            "-filter:v", "scale='min(320,iw)':'min(320,ih)':force_original_aspect_ratio=decrease,pad=320:320:(ow-iw)/2:(oh-ih)/2",
            "-y",
            str(output_path)
        ]
        subprocess.run(cmd, check=True, capture_output=True, timeout=30)
        if output_path.exists():
            # Resize with PIL just to be safe (as in photo_handler)
            img = Image.open(output_path)
            img.thumbnail((320, 320))
            img = img.convert("RGB")
            img.save(output_path, "JPEG")
            return True
        return False
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}")
        return False

# --- PING SERVICE AND STARTUP (existing code) ---
@flask_app.route("/")
def index():
    return render_template_string("Bot is running!")

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
        loop.create_task(periodic_cleanup())
        app.run()
    except Exception as e:
        print(f"Main bot execution error: {e}")
