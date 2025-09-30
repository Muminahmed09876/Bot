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

# New Import for MongoDB
from motor.motor_asyncio import AsyncIOMotorClient 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "5000"))
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 
# NEW ENV VAR: MUST BE SET FOR PERSISTENCE
MONGO_URI = os.getenv("MONGO_URI") 

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# --- DATABASE SETUP ---
db_client = None
user_settings = None

if MONGO_URI:
    try:
        # Connect to MongoDB asynchronously
        db_client = AsyncIOMotorClient(MONGO_URI)
        db = db_client.get_database("BotDatabase") # Use a default database name
        user_settings = db.get_collection("user_settings")
        logger.info("MongoDB client initialized.")
    except Exception as e:
        logger.error(f"MongoDB initialization failed: {e}")
        # If DB connection fails, persistence features will be disabled.
        MONGO_URI = None 
# ----------------------

# state (In-memory state for temporary/session-specific data)
# NOTE: All persistent user settings (thumb, caption, modes) are now in MongoDB.
# Only session-specific/temporary state remains in memory.
TASKS = {}
SET_THUMB_REQUEST = set() # Remains in-memory (tracks immediate next action)
SUBSCRIBERS = set()
SET_CAPTION_REQUEST = set() # Remains in-memory (tracks immediate next action)

# --- STATE FOR AUDIO CHANGE (Local file paths must remain in memory) ---
# Stores the path of the downloaded file waiting for audio order
AUDIO_CHANGE_FILE = {} # MUST REMAIN IN-MEMORY (local file path)
# ------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
MAX_SIZE = 4 * 1024 * 1024 * 1024

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

# ---- DATABASE HELPERS (New) ----
async def get_user_data(uid: int):
    """Fetches all settings for a user from MongoDB."""
    if not user_settings:
        return {} # Return empty dict if DB is disabled
    return await user_settings.find_one({"_id": uid}) or {}

async def save_user_data(uid: int, update_data: dict):
    """Saves/updates specific fields for a user in MongoDB."""
    if not user_settings:
        return
    await user_settings.update_one(
        {"_id": uid},
        {"$set": update_data},
        upsert=True
    )
    
async def delete_user_fields(uid: int, fields: list):
    """Deletes specific fields for a user in MongoDB."""
    if not user_settings:
        return
    await user_settings.update_one(
        {"_id": uid},
        {"$unset": {field: "" for field in fields}}
    )

async def is_audio_change_mode(uid: int) -> bool:
    data = await get_user_data(uid)
    return data.get('audio_change_mode', False)

async def is_edit_caption_mode(uid: int) -> bool:
    data = await get_user_data(uid)
    return data.get('edit_caption_mode', False)
# --------------------------


# ---- utilities ----
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url or "docs.google.com" in url

# Function to safely delete the file
def delete_file(file_path):
    try:
        if file_path and Path(file_path).exists():
            Path(file_path).unlink()
    except Exception as e:
        logger.error(f"Error deleting file {file_path}: {e}")

def parse_time(time_str: str) -> int:
    total_seconds = 0
    match_s = re.findall(r'(\d+)\s*s', time_str, re.IGNORECASE)
    match_m = re.findall(r'(\d+)\s*m', time_str, re.IGNORECASE)
    match_h = re.findall(r'(\d+)\s*h', time_str, re.IGNORECASE)

    if match_s:
        total_seconds += sum(int(s) for s in match_s)
    if match_m:
        total_seconds += sum(int(m) for m in match_m) * 60
    if match_h:
        total_seconds += sum(int(h) for h in match_h) * 3600
    
    return total_seconds

# Function to get video duration using ffprobe
def get_video_duration(file_path: Path) -> int:
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
             '-of', 'default=noprint_wrappers=1:nokey=1', str(file_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        duration = float(result.stdout.strip())
        return math.ceil(duration)
    except Exception as e:
        logger.error(f"ffprobe error: {e}")
        return 0

def progress_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel_task")]])

def delete_caption_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Delete Caption 🗑️", callback_data="delete_caption")]])

async def set_bot_commands():
    commands = [
        BotCommand("start", "Welcome message and commands list"),
        BotCommand("upload_url", "URL থেকে ডাউনলোড ও আপলোড (admin only)"),
        BotCommand("setthumb", "থাম্বনেইল সেট করুন (admin only)"),
        BotCommand("view_thumb", "থাম্বনেইল দেখুন (admin only)"),
        BotCommand("del_thumb", "থাম্বনেইল মুছে ফেলুন (admin only)"),
        BotCommand("set_caption", "ক্যাপশন সেট করুন (admin only)"),
        BotCommand("view_caption", "ক্যাপশন দেখুন (admin only)"),
        BotCommand("edit_caption_mode", "শুধু ক্যাপশন এডিট মোড টগল করুন (admin only)"),
        BotCommand("rename", "ভিডিও রিনেম করুন (reply) (admin only)"),
        BotCommand("mkv_video_audio_change", "MKV অডিও ট্র্যাক পরিবর্তন (admin only)"),
        BotCommand("mode_check", "বর্তমান মোড স্ট্যাটাস চেক করুন (admin only)"),
        BotCommand("broadcast", "ব্রডকাস্ট (admin only)"),
        BotCommand("help", "সাহায্য"),
    ]
    try:
        await app.set_bot_commands(commands)
    except Exception as e:
        logger.warning(f"Set commands error: {e}")

# --- NEW UTILITY: Keyboard for Mode Check ---
async def mode_check_keyboard(uid: int) -> InlineKeyboardMarkup:
    audio_status = "✅ ON" if await is_audio_change_mode(uid) else "❌ OFF"
    caption_status = "✅ ON" if await is_edit_caption_mode(uid) else "❌ OFF"
    
    # Check if a file is waiting for track order input (remains in-memory)
    waiting_status = " (অর্ডার বাকি)" if uid in AUDIO_CHANGE_FILE else ""
    
    keyboard = [
        [InlineKeyboardButton(f"MKV Audio Change Mode {audio_status}{waiting_status}", callback_data="toggle_audio_mode")],
        [InlineKeyboardButton(f"Edit Caption Mode {caption_status}", callback_data="toggle_caption_mode")]
    ]
    return InlineKeyboardMarkup(keyboard)
# ---------------------------------------------


# FFmpeg progress callback (using an external file for progress tracking)
async def progress_callback(current, total, status_msg: Message, cancel_event: asyncio.Event):
    if cancel_event.is_set():
        raise asyncio.CancelledError()

    percent = (current / total) * 100
    progress_bar = f"{'█' * int(percent // 10)}{'░' * (10 - int(percent // 10))}"
    
    # Safely convert bytes to appropriate units
    def format_bytes(b):
        if b >= 1024**3:
            return f"{b / 1024**3:.2f} GB"
        if b >= 1024**2:
            return f"{b / 1024**2:.2f} MB"
        if b >= 1024:
            return f"{b / 1024:.2f} KB"
        return f"{b} B"

    # Time calculations
    if 'start_time' not in status_msg.dict:
        status_msg.dict['start_time'] = time.time()
        status_msg.dict['last_edit_time'] = time.time()
    
    elapsed = time.time() - status_msg.dict['start_time']
    
    if current > 0 and elapsed > 0:
        speed = current / elapsed
        eta = (total - current) / speed
    else:
        speed = 0
        eta = 0

    # Avoid excessive editing
    if time.time() - status_msg.dict['last_edit_time'] > 3 or current == total:
        status_msg.dict['last_edit_time'] = time.time()
        
        caption_text = (
            f"**{status_msg.caption if status_msg.caption else 'Downloading/Uploading...'}**\n"
            f"Progress: `{progress_bar}`\n"
            f"Status: **{percent:.2f}%**\n"
            f"Size: **{format_bytes(current)}** / **{format_bytes(total)}**\n"
            f"Speed: **{format_bytes(speed)}/s**\n"
            f"ETA: **{timedelta(seconds=int(eta))}**"
        )
        
        try:
            await status_msg.edit(caption_text, reply_markup=progress_keyboard())
        except Exception as e:
            # Handle FloodWait or MessageNotModified
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                logger.error(f"Error editing progress message: {e}")

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
        "/mkv_video_audio_change - MKV ভিডিওর অডিও ট্র্যাক পরিবর্তন (admin only)\n"
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
            # DB CHANGE: Save thumb_time to MongoDB
            await save_user_data(uid, {'thumb_time': seconds})
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
    
    # DB CHANGE: Fetch user data
    user_data = await get_user_data(uid)
    thumb_path = user_data.get('thumb_path')
    thumb_time = user_data.get('thumb_time')
    
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
    
    # DB CHANGE: Fetch and delete
    user_data = await get_user_data(uid)
    thumb_path = user_data.get('thumb_path')
    thumb_time_set = 'thumb_time' in user_data

    # Safely delete the local file
    delete_file(thumb_path)
    
    # Remove both fields from DB
    await delete_user_fields(uid, ['thumb_path', 'thumb_time'])

    if not (thumb_path or thumb_time_set):
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই।")
    else:
        await m.reply_text("আপনার থাম্বনেইল/থাম্বনেইল তৈরির সময় মুছে ফেলা হয়েছে।")


@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id
    if uid in SET_THUMB_REQUEST:
        SET_THUMB_REQUEST.discard(uid)
        out = TMP / f"thumb_{uid}.jpg"
        try:
            await m.download(file_name=str(out))
            img = Image.open(out)
            img.thumbnail((320, 320))
            img = img.convert("RGB")
            img.save(out, "JPEG")
            
            # DB CHANGE: Save new path and clear time setting
            await save_user_data(uid, {'thumb_path': str(out)})
            await delete_user_fields(uid, ['thumb_time'])
            
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
    uid = m.from_user.id
    SET_CAPTION_REQUEST.add(uid)
    
    # DB CHANGE: Reset counter data when a new caption is about to be set
    await delete_user_fields(uid, ['counters']) 
    
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
    
    # DB CHANGE: Fetch caption
    user_data = await get_user_data(uid)
    caption = user_data.get('caption')
    
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
    
    # DB CHANGE: Delete caption and counters
    user_data = await get_user_data(uid)
    if user_data.get('caption'):
        await delete_user_fields(uid, ['caption', 'counters'])
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
    
    is_on = await is_edit_caption_mode(uid)

    if is_on:
        # DB CHANGE: Set mode to False
        await save_user_data(uid, {'edit_caption_mode': False})
        await m.reply_text("edit video caption mod **OFF**.\nএখন থেকে আপলোড করা ভিডিওর রিনেম ও থাম্বনেইল পরিবর্তন হবে, এবং সেভ করা ক্যাপশন যুক্ত হবে।")
    else:
        # DB CHANGE: Set mode to True
        await save_user_data(uid, {'edit_caption_mode': True})
        await m.reply_text("edit video caption mod **ON**.\nএখন থেকে শুধু সেভ করা ক্যাপশন ভিডিওতে যুক্ত হবে। ভিডিওর নাম এবং থাম্বনেইল একই থাকবে।")

# --- HANDLER: /mkv_video_audio_change ---
@app.on_message(filters.command("mkv_video_audio_change") & filters.private)
async def toggle_audio_change_mode(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    is_on = await is_audio_change_mode(uid)

    if is_on:
        # DB CHANGE: Set mode to False
        await save_user_data(uid, {'audio_change_mode': False})
        
        # Clean up any pending file path (in-memory state cleanup)
        if uid in AUDIO_CHANGE_FILE:
            try:
                delete_file(AUDIO_CHANGE_FILE[uid]['path'])
                if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                    await c.delete_messages(m.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
            except Exception:
                pass
            AUDIO_CHANGE_FILE.pop(uid, None)
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অফ** করা হয়েছে।")
    else:
        # DB CHANGE: Set mode to True
        await save_user_data(uid, {'audio_change_mode': True})
        await m.reply_text("MKV অডিও পরিবর্তন মোড **অন** করা হয়েছে।\nঅনুগ্রহ করে **MKV ফাইল** অথবা অন্য কোনো **ভিডিও ফাইল** পাঠান।\n(এই মোড ম্যানুয়ালি অফ না করা পর্যন্ত চালু থাকবে।)")

# --- NEW HANDLER: /mode_check ---
@app.on_message(filters.command("mode_check") & filters.private)
async def mode_check_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    # DB CHANGE: Read modes
    audio_on = await is_audio_change_mode(uid)
    caption_on = await is_edit_caption_mode(uid)
    
    audio_status = "✅ ON" if audio_on else "❌ OFF"
    caption_status = "✅ ON" if caption_on else "❌ OFF"
    
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
    
    await m.reply_text(status_text, reply_markup=await mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)

# --- NEW CALLBACK: Mode Toggle Buttons ---
@app.on_callback_query(filters.regex("toggle_(audio|caption)_mode"))
async def mode_toggle_callback(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return

    action = cb.data
    
    if action == "toggle_audio_mode":
        is_on = await is_audio_change_mode(uid)
        if is_on:
            # Turning OFF: Clear mode and cleanup pending file
            await save_user_data(uid, {'audio_change_mode': False})
            if uid in AUDIO_CHANGE_FILE:
                try:
                    delete_file(AUDIO_CHANGE_FILE[uid]['path'])
                    if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                        await c.delete_messages(cb.message.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
                except Exception:
                    pass
                AUDIO_CHANGE_FILE.pop(uid, None)
            message = "MKV Audio Change Mode OFF."
        else:
            # Turning ON
            await save_user_data(uid, {'audio_change_mode': True})
            message = "MKV Audio Change Mode ON."
            
    elif action == "toggle_caption_mode":
        is_on = await is_edit_caption_mode(uid)
        if is_on:
            await save_user_data(uid, {'edit_caption_mode': False})
            message = "Edit Caption Mode OFF."
        else:
            await save_user_data(uid, {'edit_caption_mode': True})
            message = "Edit Caption Mode ON."
            
    # Refresh the keyboard and edit the original message (similar to mode_check_cmd)
    try:
        audio_status = "✅ ON" if await is_audio_change_mode(uid) else "❌ OFF"
        caption_status = "✅ ON" if await is_edit_caption_mode(uid) else "❌ OFF"
        
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
        
        await cb.message.edit_text(status_text, reply_markup=await mode_check_keyboard(uid), parse_mode=ParseMode.MARKDOWN)
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
        # DB CHANGE: Save caption and reset counter
        await save_user_data(uid, {'caption': text})
        await delete_user_fields(uid, ['counters']) 
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # --- Handle audio order input if in mode and file is set ---
    if await is_audio_change_mode(uid) and uid in AUDIO_CHANGE_FILE:
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

# Utility function for URL downloading (re-used from original code)
async def download_file(url, out_path, status_msg, cancel_event):
    logger.info(f"Downloading from: {url}")
    file_size = 0
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return False, f"HTTP Error: {response.status}"
                
                if cancel_event.is_set():
                    return False, "Cancelled"

                file_size = int(response.headers.get('content-length', 0))
                if file_size > MAX_SIZE:
                    return False, f"ফাইল সাইজ {file_size/1024/1024/1024:.2f} GB যা {MAX_SIZE/1024/1024/1024} GB এর বেশি। বাতিল করা হলো।"

                current = 0
                chunk_size = 1024 * 1024 * 5 # 5MB chunk
                with open(out_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        if cancel_event.is_set():
                            return False, "Cancelled"
                        f.write(chunk)
                        current += len(chunk)
                        await progress_callback(current, file_size, status_msg, cancel_event)
        
        if file_size == 0 and out_path.stat().st_size == 0:
             return False, "Download failed or file size is zero."

        return True, None
    except asyncio.CancelledError:
        delete_file(out_path)
        return False, "Cancelled by user"
    except Exception as e:
        logger.error(f"Download error: {e}")
        delete_file(out_path)
        return False, str(e)

async def handle_url_download_and_upload(c: Client, m: Message, url: str):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)

    status_msg = None
    in_path = None
    
    try:
        original_name = url.split('/')[-1]
        
        # Simple name cleaning for files without clear extension
        if '.' not in original_name:
            original_name = "downloaded_file"

        in_path = TMP / f"{uid}_{datetime.now().timestamp()}_{original_name}"
        
        try:
            status_msg = await m.reply_text(f"ডাউনলোড শুরু হচ্ছে...\nURL: `{url}`", reply_markup=progress_keyboard())
        except Exception:
            status_msg = await m.reply_text(f"ডাউনলোড শুরু হচ্ছে...\nURL: `{url}`", reply_markup=progress_keyboard())

        ok, err = await download_file(url, in_path, status_msg, cancel_event)
        
        if not ok:
            if err == "Cancelled by user":
                await status_msg.edit("ডাউনলোড বাতিল করা হয়েছে।", reply_markup=None)
            else:
                await status_msg.edit(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            return

        # Start upload process
        await status_msg.edit("ডাউনলোড সম্পন্ন। এখন আপলোড প্রক্রিয়া শুরু হচ্ছে...", reply_markup=progress_keyboard())

        # Process and Upload
        await process_file_and_upload(c, m, in_path, original_name=original_name, messages_to_delete=[status_msg.id])

    except Exception as e:
        logger.error(f"Main download/upload task error: {e}")
        error_msg = f"প্রক্রিয়াকরণে অপ্রত্যাশিত ত্রুটি: {e}"
        try:
            if status_msg:
                await status_msg.edit(error_msg, reply_markup=None)
            else:
                await m.reply_text(error_msg)
        except Exception:
            await m.reply_text(error_msg)
    finally:
        try:
            delete_file(in_path)
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass


async def handle_caption_only_upload(c: Client, m: Message):
    uid = m.from_user.id
    
    # DB CHANGE: Get user data and caption
    user_data = await get_user_data(uid)
    caption_to_use = user_data.get('caption')
    
    if not caption_to_use:
        await m.reply_text("ক্যাপশন এডিট মোড চালু আছে কিন্তু কোনো সেভ করা ক্যাপশন নেই। /set_caption দিয়ে ক্যাপশন সেট করুন।")
        return

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    try:
        status_msg = await m.reply_text("ক্যাপশন এডিট করা হচ্ছে...", reply_markup=progress_keyboard())
    except Exception:
        status_msg = await m.reply_text("ক্যাপশন এডিট করা হচ্ছে...", reply_markup=progress_keyboard())
    
    try:
        source_message = m
        file_info = source_message.video or source_message.document

        if not file_info:
            try:
                await status_msg.edit("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            except Exception:
                await m.reply_text("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            return
        
        # DB CHANGE: Process the dynamic caption (now an async function)
        final_caption = await process_dynamic_caption(uid, caption_to_use, user_data)
        
        if file_info.file_id:
            if source_message.video:
                 # Edit the video message caption
                await c.edit_message_caption(
                    chat_id=m.chat.id,
                    message_id=source_message.id,
                    caption=final_caption,
                    parse_mode=ParseMode.MARKDOWN
                )
            elif source_message.document:
                 # Edit the document message caption
                await c.edit_message_caption(
                    chat_id=m.chat.id,
                    message_id=source_message.id,
                    caption=final_caption,
                    parse_mode=ParseMode.MARKDOWN
                )
        
        # New code to auto-delete the success message
        try:
            success_msg = await status_msg.edit("ক্যাপশন সফলভাবে আপডেট করা হয়েছে।", reply_markup=None)
            await asyncio.sleep(5)
            await success_msg.delete()
        except Exception:
            success_msg = await m.reply_text("ক্যাপশন সফলভাবে আপডেট করা হয়েছে।", reply_markup=None)
            await asyncio.sleep(5)
            await success_msg.delete()

    except asyncio.CancelledError:
        try:
            await status_msg.edit("ক্যাপশন এডিট বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Caption only upload error: {e}")
        try:
            await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_message(filters.private & (filters.video | filters.document))
async def forwarded_file_or_direct_file(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return

    # --- Check for MKV Audio Change Mode first (DB CHANGE) ---
    if await is_audio_change_mode(uid):
        await handle_audio_change_file(c, m)
        return
    # -------------------------------------------------

    # Check if the user is in edit caption mode (DB CHANGE)
    if await is_edit_caption_mode(uid) and m.forward_date: # Only apply to forwarded media to avoid accidental re-upload of direct files
        await handle_caption_only_upload(c, m)
        return

    # If not in any special mode, and it's a forwarded video/document, start the download/re-upload process
    if m.forward_date:
        if m.video or m.document:
            cancel_event = asyncio.Event()
            TASKS.setdefault(uid, []).append(cancel_event)
            
            status_msg = None
            in_path = None
            
            try:
                status_msg = await m.reply_text("ফাইল ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
                
                in_path = await c.download_media(
                    message=m,
                    file_name=str(TMP / f"{uid}_{datetime.now().timestamp()}_{m.video.file_name if m.video else m.document.file_name}"),
                    progress=progress_callback,
                    progress_args=(status_msg, cancel_event)
                )

                if cancel_event.is_set():
                    await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
                    return

                await status_msg.edit("ডাউনলোড সম্পন্ন। এখন আপলোড প্রক্রিয়া শুরু হচ্ছে...", reply_markup=progress_keyboard())

                original_name = m.video.file_name if m.video else m.document.file_name

                # Process and Upload
                await process_file_and_upload(c, m, Path(in_path), original_name=original_name, messages_to_delete=[status_msg.id])

            except asyncio.CancelledError:
                await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
            except Exception as e:
                logger.error(f"Forwarded file upload error: {e}")
                error_msg = f"প্রক্রিয়াকরণে অপ্রত্যাশিত ত্রুটি: {e}"
                try:
                    if status_msg:
                        await status_msg.edit(error_msg, reply_markup=None)
                    else:
                        await m.reply_text(error_msg)
                except Exception:
                    await m.reply_text(error_msg)
            finally:
                try:
                    delete_file(in_path)
                    TASKS[uid].remove(cancel_event)
                except Exception:
                    pass
    else:
        # A direct video/document which isn't handled by another mode. Pass.
        pass

# --- HANDLER FUNCTION: Handle file in audio change mode ---
async def handle_audio_change_file(c: Client, m: Message):
    uid = m.from_user.id
    
    file_info = m.video or m.document
    if not file_info:
        await m.reply_text("MKV অডিও পরিবর্তন মোড চালু আছে। অনুগ্রহ করে একটি ভিডিও/ডকুমেন্ট ফাইল পাঠান।")
        return

    # Clear any previous pending file (in-memory state cleanup)
    if uid in AUDIO_CHANGE_FILE:
        try:
            delete_file(AUDIO_CHANGE_FILE[uid]['path'])
            if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                 await c.delete_messages(m.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
        except Exception:
            pass
        AUDIO_CHANGE_FILE.pop(uid, None)

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    status_msg = None
    in_path = None

    try:
        original_name = file_info.file_name if file_info.file_name else f"file_{file_info.file_unique_id}"
        in_path = TMP / f"{uid}_{datetime.now().timestamp()}_{original_name}"
        
        try:
            status_msg = await m.reply_text("অডিও ট্র্যাক বিশ্লেষণের জন্য ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
            status_msg = await m.reply_text("অডিও ট্র্যাক বিশ্লেষণের জন্য ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())

        in_path = await c.download_media(
            message=m,
            file_name=str(in_path),
            progress=progress_callback,
            progress_args=(status_msg, cancel_event)
        )

        if cancel_event.is_set():
            await status_msg.edit("ডাউনলোড বাতিল করা হয়েছে।", reply_markup=None)
            return

        # Check for MKV format and extract audio streams
        if Path(in_path).suffix.lower() not in ['.mkv', '.mp4']:
            await status_msg.edit("এই মোডটি শুধুমাত্র MKV এবং MP4 ফাইলের জন্য উপযুক্ত। প্রক্রিয়া বাতিল করা হলো।", reply_markup=None)
            return
            
        # Get audio track info using ffprobe
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-select_streams', 'a', '-show_entries', 
             'stream=index:codec_name:tags=language', '-of', 'json', str(in_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            await status_msg.edit(f"FFprobe ত্রুটি: অডিও ট্র্যাক বিশ্লেষণ করা সম্ভব হয়নি।\n{result.stderr}", reply_markup=None)
            return

        tracks_data = json.loads(result.stdout).get('streams', [])
        
        if not tracks_data:
            await status_msg.edit("ফাইলটিতে কোনো অডিও ট্র্যাক পাওয়া যায়নি। প্রক্রিয়া বাতিল করা হলো।", reply_markup=None)
            return
            
        track_list = []
        for i, stream in enumerate(tracks_data):
            language = stream.get('tags', {}).get('language', 'N/A')
            codec = stream.get('codec_name', 'N/A')
            # Storing the actual FFmpeg index for mapping
            track_list.append({
                'user_index': i + 1,
                'stream_index': stream.get('index'),
                'details': f"({i+1}) - {language} ({codec})"
            })
            
        track_message = (
            "**অডিও ট্র্যাকের তালিকা:**\n"
            "----------------------------\n"
            + "\n".join([t['details'] for t in track_list]) + "\n"
            "----------------------------\n"
            f"মোট ট্র্যাক: {len(track_list)}\n\n"
            "আপনি যে অর্ডারে ট্র্যাকগুলি রাখতে চান, সেই অর্ডারে কমা-সেপারেটেড সংখ্যাগুলো টাইপ করে পাঠান।\n"
            "উদাহরণ: যদি আপনি ৩য় ট্র্যাকটি প্রথমে, ২য় ট্র্যাকটি দ্বিতীয়তে, এবং ১ম ট্র্যাকটি শেষে চান, তবে লিখুন: `3,2,1`"
        )
        
        # Send the track list and save the file info for the next step
        track_msg = await c.send_message(m.chat.id, track_message)
        
        AUDIO_CHANGE_FILE[uid] = {
            'path': str(in_path),
            'original_name': original_name,
            'tracks': track_list,
            'message_id': track_msg.id # To delete later
        }

        # Delete the download status message
        await status_msg.delete()
        
    except asyncio.CancelledError:
        try:
            if status_msg:
                await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            pass
        delete_file(in_path)
    except Exception as e:
        logger.error(f"Audio change mode initial handler error: {e}")
        error_msg = f"অডিও ট্র্যাক বিশ্লেষণে ত্রুটি: {e}"
        try:
            if status_msg:
                await status_msg.edit(error_msg, reply_markup=None)
            else:
                await m.reply_text(error_msg)
        except Exception:
            await m.reply_text(error_msg)
        delete_file(in_path)
        
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass


# --- HANDLER FUNCTION: Handle audio remux ---
async def handle_audio_remux(c: Client, m: Message, in_path: Path, original_name: str, new_stream_map: list, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    status_msg = None
    out_path = TMP / f"{in_path.stem}_remux.mkv"
    messages_to_delete = messages_to_delete or []

    try:
        map_args = sum([['-map', map_entry] for map_entry in new_stream_map], [])
        
        ffmpeg_cmd = [
            'ffmpeg',
            '-i', str(in_path), 
            '-map', '0:v', # Always map the video stream
        ] + map_args + [
            '-c', 'copy', # Copy all streams
            '-c:a', 'copy', # Redundant, but ensures audio is copied
            '-metadata', 'title="Remuxed"', # Optional metadata
            '-y', str(out_path) # Output file
        ]

        logger.info(f"FFmpeg Remux Command: {' '.join(ffmpeg_cmd)}")
        
        status_msg = await m.reply_text("অডিও ট্র্যাক পরিবর্তন করা হচ্ছে (Remuxing)...", reply_markup=progress_keyboard())
        messages_to_delete.append(status_msg.id)

        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Simple wait for the process to finish
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_details = stderr.decode().strip()
            await status_msg.edit(f"অডিও ট্র্যাক পরিবর্তন ব্যর্থ: FFmpeg ত্রুটি।\nবিস্তারিত: {error_details}", reply_markup=None)
            return

        await status_msg.edit("ট্র্যাক পরিবর্তন সম্পন্ন। এখন আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())

        # Start upload process with the remuxed file
        final_name = Path(original_name).stem + out_path.suffix
        await process_file_and_upload(c, m, out_path, original_name=final_name, messages_to_delete=messages_to_delete)
        
    except asyncio.CancelledError:
        try:
            if status_msg:
                await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Audio remux handler error: {e}")
        error_msg = f"অডিও ট্র্যাক পরিবর্তন ও আপলোডে ত্রুটি: {e}"
        try:
            if status_msg:
                await status_msg.edit(error_msg, reply_markup=None)
            else:
                await m.reply_text(error_msg)
        except Exception:
            await m.reply_text(error_msg)
            
    finally:
        try:
            delete_file(in_path)
            delete_file(out_path)
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass


@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    
    if not m.reply_to_message:
        await m.reply_text("একটি ফাইল বা ভিডিওতে রিপ্লাই করে কমান্ডটি ব্যবহার করুন।")
        return

    if not m.command or len(m.command) < 2:
        await m.reply_text("ব্যবহার: /rename <newname.ext>")
        return
        
    new_name = m.text.split(None, 1)[1].strip()
    
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    status_msg = None
    in_path = None
    
    try:
        status_msg = await m.reply_text("রিনেম করার জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
        
        file_info = m.reply_to_message.video or m.reply_to_message.document
        if not file_info:
            await status_msg.edit("এটি একটি ভিডিও বা ডকুমেন্ট ফাইল নয়।")
            return

        in_path = await c.download_media(
            message=m.reply_to_message,
            file_name=str(TMP / f"{uid}_{datetime.now().timestamp()}_{file_info.file_name or 'file'}"),
            progress=progress_callback,
            progress_args=(status_msg, cancel_event)
        )

        if cancel_event.is_set():
            await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
            return

        await status_msg.edit("ডাউনলোড সম্পন্ন। এখন আপলোড প্রক্রিয়া শুরু হচ্ছে...", reply_markup=progress_keyboard())

        # Process and Upload
        await process_file_and_upload(c, m, Path(in_path), original_name=new_name, messages_to_delete=[status_msg.id])

    except asyncio.CancelledError:
        await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
    except Exception as e:
        logger.error(f"Rename command error: {e}")
        error_msg = f"রিনেম প্রক্রিয়াকরণে অপ্রত্যাশিত ত্রুটি: {e}"
        try:
            if status_msg:
                await status_msg.edit(error_msg, reply_markup=None)
            else:
                await m.reply_text(error_msg)
        except Exception:
            await m.reply_text(error_msg)
    finally:
        try:
            delete_file(in_path)
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
        
        # New: Clean up audio change state if in progress
        if await is_audio_change_mode(uid):
            # We don't clear the mode (DB), but clear the waiting file state if it exists (in-memory)
            if uid in AUDIO_CHANGE_FILE:
                try:
                    delete_file(AUDIO_CHANGE_FILE[uid]['path'])
                    if 'message_id' in AUDIO_CHANGE_FILE[uid]:
                        try:
                            await c.delete_messages(cb.message.chat.id, AUDIO_CHANGE_FILE[uid]['message_id'])
                        except Exception:
                            pass
                except Exception:
                    pass
                AUDIO_CHANGE_FILE.pop(uid, None)
            
        await cb.answer("অপারেশন বাতিল করা হয়েছে।", show_alert=True)
        try:
            await cb.message.delete()
        except Exception:
            pass
    else:
        await cb.answer("কোনো অপারেশন চলছে না।", show_alert=True)

# ---- main processing and upload (functions simplified for brevity, assuming they work) ----

# Helper function for thumbnail generation
async def generate_video_thumbnail(video_path: Path, thumb_path: Path, timestamp_sec: int) -> bool:
    try:
        # Use ffmpeg to extract a frame at the specified timestamp
        cmd = [
            'ffmpeg',
            '-i', str(video_path),
            '-ss', str(timestamp_sec),
            '-vframes', '1',
            '-f', 'mjpeg',
            '-vcodec', 'mjpeg',
            '-y', str(thumb_path)
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.wait()

        if thumb_path.exists():
            img = Image.open(thumb_path)
            img.thumbnail((320, 320))
            img = img.convert("RGB")
            img.save(thumb_path, "JPEG")
            return True
        return False
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}")
        delete_file(thumb_path)
        return False

# Helper function to convert to mkv
async def convert_to_mkv(in_path: Path, out_path: Path, status_msg: Message) -> tuple[bool, str]:
    try:
        cmd = [
            'ffmpeg',
            '-i', str(in_path),
            '-c', 'copy', # Stream copy
            '-map', '0', # Map all streams
            '-f', 'matroska', # Force mkv format
            '-y', str(out_path)
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        # Use communicate for simple, non-progress-tracked processes
        _, stderr = await process.communicate()
        
        if process.returncode == 0 and out_path.exists():
            return True, None
        else:
            delete_file(out_path)
            return False, stderr.decode().strip()
            
    except Exception as e:
        logger.error(f"MKV conversion failed: {e}")
        delete_file(out_path)
        return False, str(e)


# DB CHANGE: process_dynamic_caption is now asynchronous and accepts user_data
async def process_dynamic_caption(uid, caption_template, user_data):
    if not caption_template:
        return ""
        
    # DB CHANGE: Load user state from the fetched user_data
    db_counters = user_data.get('counters', {'uploads': 0, 'dynamic_counters': {}, 're_options_count': 0})
    
    # Initialize/Reset the structure if needed
    if 'uploads' not in db_counters:
        db_counters = {'uploads': 0, 'dynamic_counters': {}, 're_options_count': 0}

    # Increment upload counter for the current user
    db_counters['uploads'] += 1

    # --- 1. Quality Cycle Logic (e.g., [re (480p, 720p, 1080p)]) ---
    quality_match = re.search(r"\[re\s*\((.*?)\)\]", caption_template)
    quality_placeholder_replaced = False

    if quality_match:
        options_str = quality_match.group(1)
        options = [opt.strip() for opt in options_str.split(',')]
        
        # Store the number of options if not already stored or if options changed
        if db_counters.get('re_options_count') != len(options):
            db_counters['re_options_count'] = len(options)
            # Reset uploads count to start cycle fresh if options change
            db_counters['uploads'] = 1 
            
        # Calculate the current index in the cycle
        current_index = (db_counters['uploads'] - 1) % len(options)
        current_quality = options[current_index]
        
        # Replace the placeholder with the current quality
        caption_template = caption_template.replace(quality_match.group(0), current_quality)
        quality_placeholder_replaced = True

        # Check if a full cycle has completed and increment counters (This logic should be applied only if a cycle has truly completed)
        if (db_counters['uploads'] - 1) % db_counters['re_options_count'] == 0 and db_counters['uploads'] > 1:
            # Increment all dynamic counters
            for key in db_counters['dynamic_counters']:
                db_counters['dynamic_counters'][key]['value'] += 1
    
    
    # --- 2. Main counter logic (e.g., [12], [(21)]) ---
    # Find all number-based placeholders
    counter_matches = re.findall(r"\[\s*(\(?\d+\)?)\s*\]", caption_template)
    
    # Initialize counters on the first upload or if the structure is empty/mismatched
    if db_counters.get('uploads', 0) <= 1 or not db_counters.get('dynamic_counters'):
        # Only initialize if it's the very first upload or no counters were saved
        db_counters['dynamic_counters'] = {} 
        for match in counter_matches:
            has_paren = match.startswith('(') and match.endswith(')')
            clean_match = re.sub(r'[()]', '', match)
            # Store the original format and the starting value
            db_counters['dynamic_counters'][match] = {'value': int(clean_match), 'has_paren': has_paren}
            
    # If no quality cycle was used, auto-increment the dynamic counters (except on first upload)
    elif db_counters.get('uploads', 0) > 1 and not quality_placeholder_replaced:
        for key in db_counters.get('dynamic_counters', {}):
             db_counters['dynamic_counters'][key]['value'] += 1


    # Replace placeholders with their current values
    for match, data in db_counters.get('dynamic_counters', {}).items():
        value = data['value']
        has_paren = data['has_paren']
        
        # Format the number with leading zeros if necessary (02, 03, etc.)
        original_num_len = len(re.sub(r'[()]', '', match))
        formatted_value = f"{value:0{original_num_len}d}"

        # Add parentheses back if they existed
        final_value = f"({formatted_value})" if has_paren else formatted_value
        
        # This regex will replace all occurrences of the specific placeholder, e.g., '[12]' or '[(21)]'
        caption_template = re.sub(re.escape(f"[{match}]"), final_value, caption_template)


    # --- 3. New Conditional Text Logic (e.g., [End (02)], [hi (05)]) ---
    
    current_episode_num = 0
    # Determine the current episode number based on the dynamic counter with the smallest value (assuming it's the episode counter)
    if db_counters.get('dynamic_counters'):
        current_episode_num = min(data['value'] for data in db_counters['dynamic_counters'].values())

    conditional_matches = re.findall(r"\[([a-zA-Z0-9\s]+)\s*\((.*?)\)\]", caption_template)

    for match in conditional_matches:
        text_to_add = match[0].strip() # e.g., "End", "hi"
        target_num_str = re.sub(r'[^0-9]', '', match[1]).strip() # e.g., "02", "05"

        placeholder = re.escape(f"[{match[0].strip()} ({match[1].strip()})]")
        
        try:
            target_num = int(target_num_str)
        except ValueError:
            caption_template = re.sub(placeholder, "", caption_template)
            continue
        
        if current_episode_num == target_num:
            # Replace placeholder with the actual TEXT
            caption_template = re.sub(placeholder, text_to_add, caption_template)
        else:
            # Replace placeholder with an empty string
            caption_template = re.sub(placeholder, "", caption_template)

    # DB CHANGE: Save the updated counter state back to MongoDB
    await save_user_data(uid, {'counters': db_counters})

    # Final formatting
    return "**" + "\n".join(caption_template.splitlines()) + "**"


async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    upload_path = in_path
    temp_thumb_path = None
    messages_to_delete = messages_to_delete or []
    
    # DB CHANGE: Get user data
    user_data = await get_user_data(uid)
    
    final_caption_template = user_data.get('caption')

    try:
        final_name = original_name or in_path.name
        
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        is_video = bool(m.video) or any(in_path.suffix.lower() == ext for ext in video_exts)
        
        if is_video:
            if in_path.suffix.lower() not in {".mp4", ".mkv"}:
                mkv_path = TMP / f"{in_path.stem}.mkv"
                try:
                    status_msg = await m.reply_text(f"ভিডিওটি {in_path.suffix} ফরম্যাটে আছে। MKV এ কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
                except Exception:
                    status_msg = await m.reply_text(f"ভিডিওটি {in_path.suffix} ফরম্যাটে আছে। MKV এ কনভার্ট করা হচ্ছে...", reply_markup=progress_keyboard())
                if messages_to_delete:
                    messages_to_delete.append(status_msg.id)
                ok, err = await convert_to_mkv(in_path, mkv_path, status_msg)
                if not ok:
                    try:
                        await status_msg.edit(f"কনভার্সন ব্যর্থ: {err}\nমূল ফাইলটি আপলোড করা হচ্ছে...", reply_markup=None)
                    except Exception:
                        await m.reply_text(f"কনভার্সন ব্যর্থ: {err}\nমূল ফাইলটি আপলোড করা হচ্ছে...", reply_markup=None)
                else:
                    upload_path = mkv_path
                    final_name = Path(final_name).stem + ".mkv" 
        
        # DB CHANGE: Get thumb path and time
        thumb_path = user_data.get('thumb_path')
        
        if is_video and not thumb_path:
            temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            thumb_time_sec = user_data.get('thumb_time', 1) # Default to 1 second
            ok = await generate_video_thumbnail(upload_path, temp_thumb_path, timestamp_sec=thumb_time_sec)
            if ok:
                thumb_path = str(temp_thumb_path)

        try:
            status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        except Exception:
            status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        if messages_to_delete:
            messages_to_delete.append(status_msg.id)

        if cancel_event.is_set():
            if messages_to_delete:
                try:
                    await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                except Exception:
                    pass
            try:
                await status_msg.edit("অপারেশন বাতিল করা হয়েছে, আপলোড শুরু করা হয়নি।", reply_markup=None)
            except Exception:
                await m.reply_text("অপারেশন বাতিল করা হয়েছে, আপলোড শুরু করা হয়নি।", reply_markup=None)
            TASKS[uid].remove(cancel_event)
            return
        
        duration_sec = get_video_duration(upload_path) if upload_path.exists() else 0
        
        caption_to_use = final_name
        if final_caption_template:
            # DB CHANGE: Call async dynamic caption processor
            caption_to_use = await process_dynamic_caption(uid, final_caption_template, user_data)

        upload_attempts = 3
        last_exc = None
        for attempt in range(1, upload_attempts + 1):
            try:
                if is_video:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=str(upload_path),
                        caption=caption_to_use,
                        thumb=thumb_path,
                        duration=duration_sec,
                        supports_streaming=True,
                        file_name=final_name, 
                        parse_mode=ParseMode.MARKDOWN,
                        progress=progress_callback,
                        progress_args=(status_msg, cancel_event)
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=str(upload_path),
                        file_name=final_name,
                        caption=caption_to_use,
                        parse_mode=ParseMode.MARKDOWN,
                        progress=progress_callback,
                        progress_args=(status_msg, cancel_event)
                    )
                
                if messages_to_delete:
                    try:
                        await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
                    except Exception:
                        pass
                
                last_exc = None
                break
            except asyncio.CancelledError:
                raise # Re-raise CancelledError to handle in outer block
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

    except asyncio.CancelledError:
        if messages_to_delete:
            try:
                await c.delete_messages(chat_id=m.chat.id, message_ids=messages_to_delete)
            except Exception:
                pass
        try:
            if status_msg:
                await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
        except Exception:
            await m.reply_text("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)

    except Exception as e:
        await m.reply_text(f"আপলোডে ত্রুটি: {e}")
    finally:
        try:
            # Clean up files
            if upload_path != in_path:
                delete_file(upload_path)
            delete_file(in_path)
            delete_file(temp_thumb_path)
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

# *** সংশোধিত: ব্রডকাস্ট কমান্ড ***
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd_no_reply(c, m: Message):
    if not is_admin(m.from_user.id):
        return
        
    if not m.command or len(m.command) < 2:
        await m.reply_text("ব্যবহার: /broadcast <text>")
        return
    
    text = m.text.split(None, 1)[1]
    
    success_count = 0
    fail_count = 0
    
    await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে {len(SUBSCRIBERS)} জন সাবস্ক্রাইবারের কাছে...")
    
    for uid in list(SUBSCRIBERS):
        try:
            await c.send_message(uid, text)
            success_count += 1
        except Exception:
            fail_count += 1
            SUBSCRIBERS.discard(uid) # Remove failed subscriber
            
    await m.reply_text(f"ব্রডকাস্ট সম্পন্ন!\nসফল: {success_count}\nব্যর্থ: {fail_count}")

@app.on_message(filters.command("broadcast") & filters.private & filters.reply)
async def broadcast_cmd_reply(c, m: Message):
    if not is_admin(m.from_user.id):
        return
        
    reply_msg = m.reply_to_message
    if not reply_msg:
        await m.reply_text("একটি মেসেজে রিপ্লাই করে কমান্ডটি ব্যবহার করুন।")
        return
    
    success_count = 0
    fail_count = 0
    
    await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে {len(SUBSCRIBERS)} জন সাবস্ক্রাইবারের কাছে...")
    
    for uid in list(SUBSCRIBERS):
        try:
            await reply_msg.copy(chat_id=uid)
            success_count += 1
        except Exception:
            fail_count += 1
            SUBSCRIBERS.discard(uid) # Remove failed subscriber
            
    await m.reply_text(f"ব্রডকাস্ট সম্পন্ন!\nসফল: {success_count}\nব্যর্থ: {fail_count}")

# --- Flask Web Server ---
@flask_app.route('/')
def home():
    # Simple check to confirm the service is running
    return render_template_string("Bot is running! Hello from Flask.")

# Ping service to keep the bot alive
def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        # Assuming that RENDER_EXTERNAL_HOSTNAME is set via env var
        logger.error("Render URL is not set. Ping service is disabled.")
        return

    url = f"http://{RENDER_EXTERNAL_HOSTNAME}"
    while True:
        try:
            response = requests.get(url, timeout=10)
            logger.info(f"Pinged {url} | Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error pinging {url}: {e}")
        time.sleep(600) # Ping every 10 minutes (600 seconds)

def run_flask_and_ping():
    # Start Flask app
    flask_thread = threading.Thread(target=lambda: flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False))
    flask_thread.start()
    
    # Start Ping service
    ping_thread = threading.Thread(target=ping_service)
    ping_thread.start()
    logger.info("Flask and Ping services started.")

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
        await asyncio.sleep(3600) # Check every hour (3600 seconds)

if __name__ == "__main__":
    print("Bot চালু হচ্ছে... Flask and Ping threads start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask_and_ping, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
    except RuntimeError:
        pass
    app.run()
