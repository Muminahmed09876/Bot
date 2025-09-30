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

# --- MongoDB Imports ---
from motor.motor_asyncio import AsyncIOMotorClient
from bson.objectid import ObjectId # Although not strictly used, it's good practice
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
MONGO_URI = os.getenv("MONGO_URI") # MongoDB Connection String
# Ensure this channel ID is correct and the bot is admin there
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

# --- STATE FOR AUDIO CHANGE ---
MKV_AUDIO_CHANGE_MODE = set()
AUDIO_CHANGE_FILE = {} 
# ------------------------------

# --- New Store State Variables ---
SET_STORE_REQUEST = set()
STORE_NAME_REQUEST = set()
STORE_THUMB_REQUEST = set()
USER_STORE_TEMP = {} # {uid: {'name': 'store_name', 'thumb_file_id': '...'}}
USER_CURRENT_STORE_NAME = {} # {uid: 'active_store_name'}
# ---------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
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
        db = mongo_client.File_Rename # Database Name - File_Rename
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
    return uid == ADMIN_ID

# ... (Existing utility functions) ...

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
    # Sort by last used for better UX
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

# ... (Existing utility functions like is_drive_url, get_video_duration, parse_time, progress_keyboard, etc.) ...

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

# ... (Existing download and progress utilities) ...


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
    except Exception as e:
        logger.warning("Set commands error: %s", e)

# ---- handlers ----

# ... (Existing start and help handlers) ...

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
            # Clear active store when setting custom time
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
    
    # Check if thumb_path is a file_id (from a store) or a local path (from /setthumb)
    is_tele_file_id = False
    if thumb_path:
        path_obj = Path(thumb_path)
        # Check if it's not a local file path (a heuristic check for a file_id)
        if not path_obj.exists() and len(thumb_path) > 10: 
            is_tele_file_id = True 

    if thumb_path and (is_tele_file_id or Path(thumb_path).exists()):
        try:
            await c.send_photo(
                chat_id=m.chat.id, 
                photo=thumb_path, 
                caption=f"এটা আপনার সেভ করা থাম্বনেইল।\n{caption_text}"
            )
        except Exception:
             await m.reply_text(f"আপনার থাম্বনেইল সেভ করা আছে, কিন্তু এটি প্রদর্শনে ব্যর্থ হয়েছে।\n{caption_text}")
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
    
    # Clear store state
    USER_CURRENT_STORE_NAME.pop(uid, None)

    # Delete local thumb
    if thumb_path and Path(thumb_path).exists():
        try:
            Path(thumb_path).unlink()
        except Exception:
            pass
        USER_THUMBS.pop(uid, None)
    # Clear file_id from store (if thumb was set from a store)
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
    
    # 1. Handle SET_THUMB_REQUEST (Existing logic)
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
            USER_CURRENT_STORE_NAME.pop(uid, None) # Clear active store
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        except Exception as e:
            await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")
        return
    
    # 2. Handle STORE_THUMB_REQUEST (New logic)
    if uid in STORE_THUMB_REQUEST:
        STORE_THUMB_REQUEST.discard(uid)
        
        # Get permanent file_id
        thumb_file_id = m.photo.file_id
        
        USER_STORE_TEMP[uid]['thumb_file_id'] = thumb_file_id
        SET_STORE_REQUEST.add(uid)
        
        await m.reply_text("থাম্বনেইল সেভ হয়েছে। এবার স্টোরের **ক্যাপশন** দিন।")
        return
    
    pass

# ... (Existing set_caption, view_caption, delete_caption_cb, toggle_edit_caption_mode, toggle_audio_change_mode, mode_check_cmd, mode_toggle_callback, handle_audio_change_file, handle_audio_remux handlers) ...

# Handlers for caption
@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_prompt(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_CAPTION_REQUEST.add(m.from_user.id)
    # Reset counter data and clear active store when a new caption is about to be set
    USER_COUNTERS.pop(m.from_user.id, None)
    USER_CURRENT_STORE_NAME.pop(m.from_user.id, None)
    
    await m.reply_text(
        "ক্যাপশন দিন। এখন আপনি এই কোডগুলো ব্যবহার করতে পারবেন:\n"
        "1. **নম্বর বৃদ্ধি:** `[01]`, `[(01)]` (নম্বর স্বয়ংক্রিয়ভাবে বাড়বে)\n"
        "2. **গুণমানের সাইকেল:** `[re (480p, 720p)]`\n"
        "3. **শর্তসাপেক্ষ টেক্সট (নতুন):** `[TEXT (XX)]` - যেমন: `[End (02)]`, `[hi (05)]` (যদি বর্তমান পর্বের নম্বর `XX` এর **সমান** হয়, তাহলে `TEXT` যোগ হবে)।"
    )


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
    
    # Reset any previous state and start the store creation flow
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
    
    # --- 1. Handle SET_CAPTION_REQUEST (Existing logic) ---
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        USER_CAPTIONS[uid] = text
        USER_COUNTERS.pop(uid, None)
        USER_CURRENT_STORE_NAME.pop(uid, None) # Clear active store
        await m.reply_text("আপনার ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # --- 2. Handle Store Name / Action Request (New logic) ---
    if uid in STORE_NAME_REQUEST:
        STORE_NAME_REQUEST.discard(uid)
        
        # Action: DELETE
        if USER_STORE_TEMP.get(uid, {}).get('action') == 'delete':
            deleted = await db_delete_store(text, uid)
            USER_STORE_TEMP.pop(uid, None)
            if deleted:
                if USER_CURRENT_STORE_NAME.get(uid) == text:
                     USER_CURRENT_STORE_NAME.pop(uid, None) # Clear active store if it was the one deleted
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
                USER_THUMBS[uid] = store_data['thumb_file_id'] # Store file_id instead of path
                USER_CAPTIONS[uid] = store_data['caption']
                USER_THUMB_TIME.pop(uid, None) 
                USER_CURRENT_STORE_NAME[uid] = text # Set the active store name
                
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

    # --- 3. Handle Store Caption Request (Final step in creation) ---
    if uid in SET_STORE_REQUEST:
        SET_STORE_REQUEST.discard(uid)
        if uid not in USER_STORE_TEMP or 'thumb_file_id' not in USER_STORE_TEMP[uid]:
            await m.reply_text("স্টোর তৈরির প্রক্রিয়াটি সম্পূর্ণ হয়নি। /store দিয়ে আবার শুরু করুন।")
            return

        store_data = USER_STORE_TEMP.pop(uid)
        store_name = store_data['name']
        thumb_file_id = store_data['thumb_file_id']
        
        # Save to MongoDB
        await db_save_store(store_name, text, thumb_file_id, uid)
        
        # Send confirmation to user
        await c.send_photo(
            chat_id=m.chat.id,
            photo=thumb_file_id,
            caption=f"স্টোর `{store_name}` সফলভাবে সেভ হয়েছে।\n\n**ক্যাপশন:**\n{text}",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Send to channel (as per requirement)
        if STORE_CHANNEL_ID:
            try:
                # Send the thumbnail and name to the output channel
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

    # --- 4. Handle audio order input (Existing logic) ---
    if uid in MKV_AUDIO_CHANGE_MODE and uid in AUDIO_CHANGE_FILE:
        # ... (Existing logic for audio order handling) ...
        # (Omitted for brevity in this final response, assuming the previous state of the code is complete)
        pass


    # --- 5. Handle auto URL upload (Existing logic) ---
    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))
    
# ... (Existing upload_url_cmd handler) ...
# ... (Existing handle_url_download_and_upload handler) ...
# ... (Existing handle_caption_only_upload handler) ...
# ... (Existing forwarded_file_or_direct_file handler) ...
# ... (Existing handle_audio_change_file handler) ...
# ... (Existing handle_audio_remux handler) ...
# ... (Existing rename_cmd handler) ...
# ... (Existing cancel_task_cb handler) ...
# ... (Existing generate_video_thumbnail, convert_to_mkv handlers) ...


# --- ASYNC DB Update Task ---
async def run_db_caption_update(store_name, owner_id, new_template):
    try:
        await db_update_store_caption(store_name, owner_id, new_template)
    except Exception as e:
        logger.error(f"Failed to update store caption in DB: {e}")
# ----------------------------


def process_dynamic_caption(uid, caption_template):
    # Initialize user state if it doesn't exist
    if uid not in USER_COUNTERS:
        USER_COUNTERS[uid] = {'uploads': 0, 'episode_numbers': {}, 'dynamic_counters': {}, 're_options_count': 0}

    # Increment upload counter for the current user
    USER_COUNTERS[uid]['uploads'] += 1

    # --- 1. Quality Cycle Logic (e.g., [re (480p, 720p, 1080p)]) ---
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


    # --- 2. Main counter logic (e.g., [12], [(21)]) ---
    counter_matches = re.findall(r"\[\s*(\(?\d+\)?)\s*\]", caption_template)
    
    if USER_COUNTERS[uid]['uploads'] == 1:
        for match in counter_matches:
            has_paren = match.startswith('(') and match.endswith(')')
            clean_match = re.sub(r'[()]', '', match)
            USER_COUNTERS[uid]['dynamic_counters'][match] = {'value': int(clean_match), 'has_paren': has_paren}
    
    for match, data in USER_COUNTERS[uid]['dynamic_counters'].items():
        value = data['value']
        has_paren = data['has_paren']
        
        original_num_len = len(re.sub(r'[()]', '', match))
        formatted_value = f"{value:0{original_num_len}d}"

        final_value = f"({formatted_value})" if has_paren else formatted_value
        
        caption_template = re.sub(re.escape(f"[{match}]"), final_value, caption_template)


    # --- 3. Conditional Text Logic (e.g., [End (02)], [hi (05)]) ---
    current_episode_num = 0
    if USER_COUNTERS[uid].get('dynamic_counters'):
        current_episode_num = min(data['value'] for data in USER_COUNTERS[uid]['dynamic_counters'].values())

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

    
    # --- New: Update Store Caption in DB ---
    current_store_name = USER_CURRENT_STORE_NAME.get(uid)
    if current_store_name and store_collection:
        # The 'caption_template' now contains the new, incremented counter values.
        # Save this template back to the DB for the next run.
        asyncio.create_task(run_db_caption_update(current_store_name, uid, caption_template))
    # ------------------------------------------
    
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
        
        if is_video:
            if in_path.suffix.lower() not in {".mp4", ".mkv"}:
                # ... (Existing conversion logic) ...
                pass
        
        thumb_file_or_path = USER_THUMBS.get(uid)
        
        # Determine the thumbnail to use
        final_thumb = None
        if thumb_file_or_path:
            path_obj = Path(thumb_file_or_path)
            # Check if it's a file_id (from a store)
            is_tele_file_id = not path_obj.exists() and len(thumb_file_or_path) > 10

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


        # ... (Existing status message and upload attempts logic) ...

        duration_sec = get_video_duration(upload_path) if upload_path.exists() else 0
        
        caption_to_use = final_name
        if final_caption_template:
            caption_to_use = process_dynamic_caption(uid, final_caption_template) # Call the updated function

        upload_attempts = 3
        last_exc = None
        for attempt in range(1, upload_attempts + 1):
            try:
                if is_video:
                    await c.send_video(
                        chat_id=m.chat.id,
                        video=str(upload_path),
                        caption=caption_to_use,
                        thumb=final_thumb, # Use final_thumb which can be a path or file_id
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
                
                # ... (Existing success logic) ...
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                # ... (Existing failure and retry logic) ...

        # ... (Existing final error message) ...
        
    except Exception as e:
        await m.reply_text(f"আপলোডে ত্রুটি: {e}")
    finally:
        # ... (Existing cleanup logic) ...
        pass
        
# ... (Remaining existing functions like broadcast, flask, ping_service, periodic_cleanup, if __name__ == "__main__") ...
