#!/usr/bin/env python3
import os
import re
import aiohttp
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from pyrogram import Client, filters
# --- S U P E R   I M P O R T   F I X ---
# Added 'CallbackQuery' import which was likely causing the exit error
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

# --- MongoDB Imports ---
from motor.motor_asyncio import AsyncIOMotorClient
from bson.objectid import ObjectId 
# -----------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "5000"))
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 

# --- New Environment Variables for Database and Channel ---
MONGO_URI = os.getenv("MONGO_URI") 
STORE_CHANNEL_ID = os.getenv("STORE_CHANNEL_ID") 
# -----------------------------------------------------------

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# state
USER_THUMBS = {}
TASKS = {}
SET_THUMB_REQUEST = set()
SUBSCRIBERS = set()
SET_CAPTION_REQUEST = set()
USER_CAPTIONS = {}
USER_COUNTERS = {}
EDIT_CAPTION_MODE = set()
USER_THUMB_TIME = {}

# --- STATE FOR AUDIO CHANGE (Assuming this exists from original file) ---
MKV_AUDIO_CHANGE_MODE = set()
AUDIO_CHANGE_FILE = {} 
# ------------------------------------------------------------------------

# --- New Store State Variables ---
SET_STORE_REQUEST = set()
STORE_NAME_REQUEST = set()
STORE_THUMB_REQUEST = set()
USER_STORE_TEMP = {} 
USER_CURRENT_STORE_NAME = {} 
# ---------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", "0")) # Ensure a default value to prevent error if not set
MAX_SIZE = 4 * 1024 * 1024 * 1024

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

# --- MongoDB Initialization ---
mongo_client = None
db = None
store_collection = None
if MONGO_URI:
    try:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        db = mongo_client.File_Rename 
        store_collection = db.stores
        logger.info("MongoDB client connected.")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        mongo_client = None
else:
    logger.warning("MONGO_URI is not set. Store commands will not work.")
# ------------------------------


# ---- utilities ----

def is_admin(uid: int) -> bool:
    # Use global ADMIN_ID which is cast to int earlier
    return uid == ADMIN_ID 

# Placeholder functions (ensure these exist in your original main.py or provide their implementation)
def get_video_duration(path: Path) -> int:
    try:
        parser = createParser(str(path))
        if not parser:
            return 0
        with parser:
            metadata = extractMetadata(parser)
            if metadata and metadata.has("duration"):
                return int(metadata.get('duration').total_seconds())
    except Exception:
        pass
    return 0

def parse_time(time_str: str) -> int:
    # ... (Implementation needed: Placeholder for time parsing logic) ...
    total_seconds = 0
    time_str = time_str.lower().strip()
    
    # Handle hours (h), minutes (m), seconds (s)
    h_match = re.search(r'(\d+)\s*h', time_str)
    m_match = re.search(r'(\d+)\s*m', time_str)
    s_match = re.search(r'(\d+)\s*s', time_str)

    if h_match:
        total_seconds += int(h_match.group(1)) * 3600
    if m_match:
        total_seconds += int(m_match.group(1)) * 60
    if s_match:
        total_seconds += int(s_match.group(1))

    # If only digits are provided, assume seconds (e.g., '30')
    if total_seconds == 0 and time_str.isdigit():
         total_seconds = int(time_str)

    return total_seconds

async def generate_video_thumbnail(video_path: Path, output_path: Path, timestamp_sec: int = 1) -> bool:
    try:
        if not video_path.exists(): return False
        
        # Use ffmpeg to seek and capture a frame
        command = [
            'ffmpeg',
            '-ss', str(timestamp_sec),
            '-i', str(video_path),
            '-vframes', '1',
            '-s', '320x320', # Resize to 320x320
            '-y', 
            str(output_path)
        ]
        
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.wait()

        if output_path.exists():
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}")
        return False
    
# ---- MongoDB Utility Functions ----

async def db_save_store(name: str, caption: str, thumb_file_id: str, owner_id: int):
    if not store_collection: return None
    document = {
        "name": name,
        "caption": caption,
        "thumb_file_id": thumb_file_id,
        "owner_id": owner_id,
        "created_at": datetime.now(),
        "last_used": datetime.now()
    }
    await store_collection.update_one(
        {"name": name, "owner_id": owner_id},
        {"$set": document},
        upsert=True
    )

async def db_get_store(name: str, owner_id: int):
    if not store_collection: return None
    return await store_collection.find_one({"name": name, "owner_id": owner_id})

async def db_delete_store(name: str, owner_id: int):
    if not store_collection: return False
    result = await store_collection.delete_one({"name": name, "owner_id": owner_id})
    return result.deleted_count > 0

async def db_get_all_store_names(owner_id: int):
    if not store_collection: return []
    cursor = store_collection.find({"owner_id": owner_id}, {"name": 1}).sort("last_used", -1) 
    return [doc["name"] for doc in await cursor.to_list(length=1000)]

async def db_update_store_caption(name: str, owner_id: int, new_caption: str):
    if not store_collection: return False
    await store_collection.update_one(
        {"name": name, "owner_id": owner_id},
        {"$set": {"caption": new_caption, "last_used": datetime.now()}}
    )
    return True

# ----------------------------------


# ... (Other utility functions like progress_keyboard, mode_check_keyboard, etc. needed for the full code to run) ...


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
        
        # --- New Store Commands ---
        BotCommand("store", "নতুন থাম্বনেইল ও ক্যাপশন সেটআপ করুন (admin only)"),
        BotCommand("view_store", "সেভ করা সকল স্টোরের নাম দেখুন (admin only)"),
        BotCommand("set_store", "সেভ করা স্টোর ব্যবহার করুন (admin only)"),
        BotCommand("delete_store", "স্টোর মুছে ফেলুন (admin only)"),
        # --------------------------
        
        BotCommand("broadcast", "ব্রডকাস্ট (কেবল অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
        logger.info("Bot commands set successfully.")
    except Exception as e:
        logger.warning("Set commands error: %s", e)


# ---- handlers ----

# Placeholder for existing handlers like handle_url_download_and_upload, etc. 
# You need to ensure these are defined or provided in your main.py

async def handle_url_download_and_upload(c: Client, m: Message, url: str):
    # This function needs to be implemented in your full code
    await m.reply_text(f"URL: {url} থেকে ফাইল ডাউনলোড এবং আপলোডের কাজ শুরু হয়েছে। (Placeholder)")

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(c, m: Message):
    await m.reply_text("নমস্কার! আমি আপনার ফাইল রিনেম এবং আপলোড বট। অ্যাডমিন ছাড়া অন্য কেউ এটি ব্যবহার করতে পারবে না। /help-এ ক্লিক করুন সহায়িকা দেখতে।")

@app.on_message(filters.command("help") & filters.private)
async def help_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
        
    await m.reply_text("সহায়িকা: সমস্ত কমান্ড এবং ব্যবহারবিধি এখানে দেখানো হলো...", parse_mode=ParseMode.MARKDOWN)

# ... (Existing setthumb, view_thumb, del_thumb, photo_handler, set_caption, text_handler - fully included in next block) ...

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
            USER_CURRENT_STORE_NAME.pop(uid, None) 
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
    
    caption_text = ""
    if uid in USER_CURRENT_STORE_NAME:
        caption_text = f"এটি স্টোর `{USER_CURRENT_STORE_NAME[uid]}` থেকে সেট করা হয়েছে।"
    
    is_tele_file_id = False
    if thumb_path:
        path_obj = Path(thumb_path)
        # Heuristic check for file_id (not a local file and looks like a file_id string)
        if not path_obj.exists() and len(thumb_path) > 10 and not thumb_path.startswith("tmp/"): 
            is_tele_file_id = True 

    if thumb_path and (is_tele_file_id or Path(thumb_path).exists()):
        try:
            await c.send_photo(
                chat_id=m.chat.id, 
                photo=thumb_path, 
                caption=f"এটা আপনার সেভ করা থাম্বনেইল।\n{caption_text}"
            )
        except Exception as e:
             await m.reply_text(f"আপনার থাম্বনেইল সেভ করা আছে, কিন্তু এটি প্রদর্শনে ব্যর্থ হয়েছে। ত্রুটি: {e}\n{caption_text}")
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
    
    USER_CURRENT_STORE_NAME.pop(uid, None)

    if thumb_path and Path(thumb_path).exists():
        try:
            Path(thumb_path).unlink()
        except Exception:
            pass
        USER_THUMBS.pop(uid, None)
    elif uid in USER_THUMBS:
        USER_THUMBS.pop(uid)

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
    
    # 1. Handle SET_THUMB_REQUEST 
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
            USER_THUMB_TIME.pop(uid, None)
            USER_CURRENT_STORE_NAME.pop(uid, None) 
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        except Exception as e:
            await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")
        return
    
    # 2. Handle STORE_THUMB_REQUEST (New logic)
    if uid in STORE_THUMB_REQUEST:
        STORE_THUMB_REQUEST.discard(uid)
        
        if not STORE_CHANNEL_ID:
            await m.reply_text("চ্যানেল আইডি (`STORE_CHANNEL_ID`) সেট করা নেই। স্টোর তৈরি করা যাচ্ছে না।")
            USER_STORE_TEMP.pop(uid, None)
            return

        thumb_file_id = m.photo.file_id
        
        USER_STORE_TEMP[uid]['thumb_file_id'] = thumb_file_id
        SET_STORE_REQUEST.add(uid)
        
        await m.reply_text("থাম্বনেইল সেভ হয়েছে। এবার স্টোরের **ক্যাপশন** দিন।")
        return
    
    pass

# Handlers for caption
@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_CAPTION_REQUEST.add(m.from_user.id)
    USER_COUNTERS.pop(m.from_user.id, None)
    USER_CURRENT_STORE_NAME.pop(m.from_user.id, None)
    
    await m.reply_text(
        "ক্যাপশন দিন। এখন আপনি এই কোডগুলো ব্যবহার করতে পারবেন:\n"
        "1. **নম্বর বৃদ্ধি:** `[01]`, `[(01)]` (নম্বর স্বয়ংক্রিয়ভাবে বাড়বে)\n"
        "2. **গুণমানের সাইকেল:** `[re (480p, 720p)]`\n"
        "3. **শর্তসাপেক্ষ টেক্সট:** `[TEXT (XX)]` - যেমন: `[End (02)]`, `[hi (05)]` (যদি বর্তমান পর্বের নম্বর `XX` এর **সমান** হয়, তাহলে `TEXT` যোগ হবে)।"
    )

@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    caption = USER_CAPTIONS.get(m.from_user.id, "কোনো ক্যাপশন সেভ করা নেই।")
    
    current_store = USER_CURRENT_STORE_NAME.get(m.from_user.id)
    store_info = f"\n\n**ব্যবহার হচ্ছে স্টোর:** `{current_store}`" if current_store else ""
    
    await m.reply_text(f"আপনার বর্তমান ক্যাপশন:\n\n`{caption}`{store_info}", parse_mode=ParseMode.MARKDOWN)


# --- New Store Command Handlers ---

@app.on_message(filters.command("store") & filters.private)
async def store_cmd_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not store_collection:
        await m.reply_text("ডাটাবেস কানেকশন সেট করা নেই। MongoDB URI সেট করুন।")
        return
    
    uid = m.from_user.id
    
    STORE_NAME_REQUEST.add(uid)
    USER_STORE_TEMP.pop(uid, None)
    
    await m.reply_text("স্টোরের একটি **নাম** দিন। (যেমন: `Bangla_Series_01`, `Action_Movie`)")

@app.on_message(filters.command("delete_store") & filters.private)
async def delete_store_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not store_collection:
        await m.reply_text("ডাটাবেস কানেকশন সেট করা নেই। MongoDB URI সেট করুন।")
        return
    
    STORE_NAME_REQUEST.add(m.from_user.id)
    USER_STORE_TEMP[m.from_user.id] = {'action': 'delete'}
    
    await m.reply_text("মুছে ফেলার জন্য স্টোরের **নাম** দিন।")

@app.on_message(filters.command("view_store") & filters.private)
async def view_store_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not store_collection:
        await m.reply_text("ডাটাবেস কানেকশন সেট করা নেই। MongoDB URI সেট করুন।")
        return
    
    uid = m.from_user.id
    names = await db_get_all_store_names(uid)
    
    if names:
        store_list = "\n".join(f"- `{name}`" for name in names)
        await m.reply_text(f"আপনার সেভ করা স্টোরের নামসমূহ:\n\n{store_list}")
    else:
        await m.reply_text("আপনার কোনো স্টোর সেভ করা নেই। `/store` দিয়ে নতুন স্টোর তৈরি করুন।")

@app.on_message(filters.command("set_store") & filters.private)
async def set_store_cmd_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not store_collection:
        await m.reply_text("ডাটাবেস কানেকশন সেট করা নেই। MongoDB URI সেট করুন।")
        return
        
    STORE_NAME_REQUEST.add(m.from_user.id)
    USER_STORE_TEMP[m.from_user.id] = {'action': 'set'}
    
    await m.reply_text("ব্যবহার করার জন্য স্টোরের **নাম** দিন।")

# --- End New Store Command Handlers ---


@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    text = m.text.strip()
    
    # 1. Handle SET_CAPTION_REQUEST
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None)
        USER_CURRENT_STORE_NAME.pop(uid, None)
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # 2. Handle Store Name / Action Request
    if uid in STORE_NAME_REQUEST:
        STORE_NAME_REQUEST.discard(uid)
        
        # Action: DELETE
        if USER_STORE_TEMP.get(uid, {}).get('action') == 'delete':
            deleted = await db_delete_store(text, uid)
            USER_STORE_TEMP.pop(uid, None)
            if deleted:
                if USER_CURRENT_STORE_NAME.get(uid) == text:
                     USER_CURRENT_STORE_NAME.pop(uid, None)
                await m.reply_text(f"স্টোর `{text}` সফলভাবে মুছে ফেলা হয়েছে।")
            else:
                await m.reply_text(f"স্টোর `{text}` খুঁজে পাওয়া যায়নি বা মুছে ফেলা যায়নি। নামের বানান চেক করুন।")
            return

        # Action: SET
        if USER_STORE_TEMP.get(uid, {}).get('action') == 'set':
            store_data = await db_get_store(text, uid)
            USER_STORE_TEMP.pop(uid, None)
            if store_data:
                # Set the in-memory defaults for the user
                USER_THUMBS[uid] = store_data['thumb_file_id']
                USER_CAPTIONS[uid] = store_data['caption']
                USER_THUMB_TIME.pop(uid, None) 
                USER_CURRENT_STORE_NAME[uid] = text 
                
                # Reset counter for the new store
                USER_COUNTERS.pop(uid, None) 
                
                # Send confirmation
                await c.send_photo(
                    chat_id=m.chat.id,
                    photo=store_data['thumb_file_id'],
                    caption=f"স্টোর `{text}` সফলভাবে সেট হয়েছে।\n\n**ক্যাপশন:**\n{store_data['caption']}",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await m.reply_text(f"স্টোর `{text}` খুঁজে পাওয়া যায়নি। নামের বানান চেক করুন।")
            return
            
        # Action: STORE (Start the creation flow)
        if uid not in USER_STORE_TEMP and not USER_STORE_TEMP.get(uid, {}).get('action'):
            # Save the name and proceed to thumb request
            USER_STORE_TEMP[uid] = {'name': text}
            STORE_THUMB_REQUEST.add(uid)
            await m.reply_text(f"স্টোরের নাম সেভ হয়েছে: `{text}`\n\nএবার স্টোরের **থাম্বনেইল** হিসেবে ব্যবহার করার জন্য একটি ছবি (Photo) পাঠান।")
            return

    # 3. Handle Store Caption Request (Final step in creation)
    if uid in SET_STORE_REQUEST:
        SET_STORE_REQUEST.discard(uid)
        if uid not in USER_STORE_TEMP or 'thumb_file_id' not in USER_STORE_TEMP[uid]:
            await m.reply_text("স্টোর তৈরির প্রক্রিয়াটি সম্পূর্ণ হয়নি। /store দিয়ে আবার শুরু করুন।")
            return

        store_data = USER_STORE_TEMP.pop(uid)
        store_name = store_data['name']
        thumb_file_id = store_data['thumb_file_id']
        
        await db_save_store(store_name, text, thumb_file_id, uid)
        
        await c.send_photo(
            chat_id=m.chat.id,
            photo=thumb_file_id,
            caption=f"স্টোর `{store_name}` সফলভাবে সেভ হয়েছে।\n\n**ক্যাপশন:**\n{text}",
            parse_mode=ParseMode.MARKDOWN
        )
        
        if STORE_CHANNEL_ID:
            try:
                await c.send_photo(
                    chat_id=int(STORE_CHANNEL_ID),
                    photo=thumb_file_id,
                    caption=f"**New Store Added:**\n`{store_name}`\n\n**Set Caption:**\n{text}",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Failed to send to store channel: {e}")
                await m.reply_text(f"চ্যানেলে আপলোড ব্যর্থ হয়েছে। `STORE_CHANNEL_ID` ঠিক করুন। ত্রুটি: {e}")
        return

    # 4. Handle auto URL upload
    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))

# --- ASYNC DB Update Task ---
async def run_db_caption_update(store_name, owner_id, new_template):
    try:
        await db_update_store_caption(store_name, owner_id, new_template)
    except Exception as e:
        logger.error(f"Failed to update store caption in DB: {e}")
# ----------------------------


def process_dynamic_caption(uid, caption_template):
    if uid not in USER_COUNTERS:
        USER_COUNTERS[uid] = {'uploads': 0, 'dynamic_counters': {}, 're_options_count': 0}

    USER_COUNTERS[uid]['uploads'] += 1
    # ... (Rest of the process_dynamic_caption logic to update counters and template) ...
    
    # 1. Quality Cycle Logic
    quality_match = re.search(r"\[re\s*\((.*?)\)\]", caption_template)
    if quality_match:
        options_str = quality_match.group(1)
        options = [opt.strip() for opt in options_str.split(',')]
        
        if not USER_COUNTERS[uid]['re_options_count']:
            USER_COUNTERS[uid]['re_options_count'] = len(options)
        
        current_index = (USER_COUNTERS[uid]['uploads'] - 1) % len(options)
        current_quality = options[current_index]
        
        caption_template = caption_template.replace(quality_match.group(0), current_quality)

        if (USER_COUNTERS[uid]['uploads'] - 1) % USER_COUNTERS[uid]['re_options_count'] == 0 and USER_COUNTERS[uid]['uploads'] > 1:
            for key in USER_COUNTERS[uid]['dynamic_counters']:
                USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1
    elif USER_COUNTERS[uid]['uploads'] > 1: 
        for key in USER_COUNTERS[uid].get('dynamic_counters', {}):
             USER_COUNTERS[uid]['dynamic_counters'][key]['value'] += 1


    # 2. Main counter logic
    counter_matches = re.findall(r"\[\s*(\(?\d+\)?)\s*\]", caption_template)
    
    if USER_COUNTERS[uid]['uploads'] == 1:
        for match in counter_matches:
            has_paren = match.startswith('(') and match.endswith(')')
            clean_match = re.sub(r'[()]', '', match)
            USER_COUNTERS[uid]['dynamic_counters'][match] = {'value': int(clean_match), 'has_paren': has_paren}
    
    current_episode_num = 0
    for match, data in USER_COUNTERS[uid]['dynamic_counters'].items():
        value = data['value']
        has_paren = data['has_paren']
        
        original_num_len = len(re.sub(r'[()]', '', match))
        formatted_value = f"{value:0{original_num_len}d}"
        
        final_value = f"({formatted_value})" if has_paren else formatted_value
        caption_template = re.sub(re.escape(f"[{match}]"), final_value, caption_template)
        current_episode_num = value # Use the last counter value as the episode num for conditional text

    # 3. Conditional Text Logic 
    conditional_matches = re.findall(r"\[([a-zA-Z0-9\s]+)\s*\((.*?)\)\]", caption_template)

    for match in conditional_matches:
        text_to_add = match[0].strip()
        target_num_str = re.sub(r'[^0-9]', '', match[1]).strip() 

        placeholder = re.escape(f"[{match[0].strip()} ({match[1].strip()})]")
        
        try:
            target_num = int(target_num_str)
        except ValueError:
            caption_template = re.sub(placeholder, "", caption_template)
            continue
        
        if current_episode_num == target_num:
            caption_template = re.sub(placeholder, text_to_add, caption_template)
        else:
            caption_template = re.sub(placeholder, "", caption_template)

    
    # Update Store Caption in DB
    current_store_name = USER_CURRENT_STORE_NAME.get(uid)
    if current_store_name and store_collection:
        asyncio.create_task(run_db_caption_update(current_store_name, uid, caption_template))
    
    # Final formatting
    return "**" + "\n".join(caption_template.splitlines()) + "**"


async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None, messages_to_delete: list = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    
    upload_path = in_path
    temp_thumb_path = None
    final_caption_template = USER_CAPTIONS.get(uid)

    try:
        final_name = original_name or in_path.name
        
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        is_video = bool(m.video) or any(in_path.suffix.lower() == ext for ext in video_exts)
        
        # ... (Existing conversion logic) ...
        # NOTE: This conversion logic needs to be fully implemented in your main.py

        thumb_file_or_path = USER_THUMBS.get(uid)
        
        # Determine the thumbnail to use
        final_thumb = None
        if thumb_file_or_path:
            path_obj = Path(thumb_file_or_path)
            # Check if it's a file_id (from a store)
            is_tele_file_id = not path_obj.exists() and len(thumb_file_or_path) > 10 and not thumb_file_or_path.startswith("tmp/")

            if is_tele_file_id:
                final_thumb = thumb_file_or_path
            elif path_obj.exists():
                final_thumb = str(thumb_file_or_path)
        
        # If no user-defined thumb, generate one if it's a video
        if is_video and not final_thumb:
            temp_thumb_path = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            thumb_time_sec = USER_THUMB_TIME.get(uid, 1) 
            ok = await generate_video_thumbnail(upload_path, temp_thumb_path, timestamp_sec=thumb_time_sec)
            if ok:
                final_thumb = str(temp_thumb_path)


        # ... (Existing status message and upload attempts logic - Placeholder) ...
        await c.edit_message_text(m.chat.id, messages_to_delete[0].id, "আপলোড শুরু হচ্ছে...")
        
        duration_sec = get_video_duration(upload_path) if upload_path.exists() else 0
        
        caption_to_use = final_name
        if final_caption_template:
            caption_to_use = process_dynamic_caption(uid, final_caption_template) 

        upload_attempts = 3
        for attempt in range(1, upload_attempts + 1):
            try:
                if is_video:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=str(upload_path),
                        caption=caption_to_use,
                        thumb=final_thumb, 
                        duration=duration_sec,
                        supports_streaming=True,
                        file_name=final_name, 
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id,
                        document=str(upload_path),
                        file_name=final_name,
                        caption=caption_to_use,
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                await c.edit_message_text(m.chat.id, messages_to_delete[0].id, f"✅ ফাইল `{final_name}` সফলভাবে আপলোড হয়েছে।")
                break
            except Exception as e:
                # ... (Failure logic) ...
                if attempt == upload_attempts:
                     raise e

        
    except Exception as e:
        await c.edit_message_text(m.chat.id, messages_to_delete[0].id, f"❌ আপলোডে ত্রুটি: {e}")
    finally:
        # ... (Existing cleanup logic) ...
        if temp_thumb_path and temp_thumb_path.exists(): temp_thumb_path.unlink()
        if upload_path.exists(): upload_path.unlink()
        if cancel_event in TASKS.get(uid, []): TASKS[uid].remove(cancel_event)


# ... (Existing flask, ping_service, run_flask_and_ping) ...

@flask_app.route("/")
def index():
    return render_template_string("<h1>Bot is running!</h1>")

def ping_service():
    if not RENDER_EXTERNAL_HOSTNAME:
        return

    url = f"http://{RENDER_EXTERNAL_HOSTNAME}"
    while True:
        try:
            requests.get(url, timeout=10)
        except requests.exceptions.RequestException:
            pass
        time.sleep(600)

def run_flask_and_ping():
    flask_thread = threading.Thread(target=lambda: flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False))
    flask_thread.start()
    ping_thread = threading.Thread(target=ping_service)
    ping_thread.start()

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
        loop.run_until_complete(asyncio.gather(
            periodic_cleanup(),
            app.run()
        ))
    except (KeyboardInterrupt, SystemExit):
        print("Bot বন্ধ হচ্ছে...")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()
