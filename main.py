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
# --- NEW IMPORTS ---
import motor.motor_asyncio
from typing import Optional, Dict, Any, List
# -------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "5000"))
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME") 
# --- NEW ENV VARS ---
MONGO_URI = os.getenv("MONGO_URI")
STORE_CHANNEL_ID = int(os.getenv("STORE_CHANNEL_ID", "0")) 
# --------------------

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# --- MONGODB SETUP ---
db: Optional[motor.motor_asyncio.AsyncIOMotorDatabase] = None
stores_collection: Optional[motor.motor_asyncio.AsyncIOMotorCollection] = None

if MONGO_URI:
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        db = client.File_Rename
        stores_collection = db.stores
        logger.info("MongoDB connection established.")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
else:
    logger.warning("MONGO_URI not set. Store features will not work.")
# ---------------------

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

# --- NEW STATE FOR STORE MANAGEMENT ---
SET_STORE_REQUEST = set() # Waiting for a new store name
STORE_NAME_REQUEST = set() # Waiting for store name input for /store command
STORE_THUMB_REQUEST = set() # Waiting for a photo for store thumbnail
USER_STORE_TEMP: Dict[int, Dict[str, Any]] = {} # Temp store data for creation
USER_CURRENT_STORE_NAME: Dict[int, str] = {} # Active store name for the user
# --------------------------------------

# --- STATE FOR AUDIO CHANGE ---
MKV_AUDIO_CHANGE_MODE = set()
# Stores the path of the downloaded file waiting for audio order
AUDIO_CHANGE_FILE = {} 
# ------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))
MAX_SIZE = 4 * 1024 * 1024 * 1024

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

# ---- MONGODB UTILITIES ----

async def db_save_store(store_data: Dict[str, Any]) -> bool:
    """Saves a new store document to the database."""
    if not stores_collection: return False
    try:
        store_name = store_data['store_name']
        result = await stores_collection.update_one(
            {'store_name': store_name},
            {'$set': store_data},
            upsert=True
        )
        return result.acknowledged
    except Exception as e:
        logger.error(f"Error saving store {store_data.get('store_name')}: {e}")
        return False

async def db_get_store(store_name: str) -> Optional[Dict[str, Any]]:
    """Retrieves a store document by name."""
    if not stores_collection: return None
    try:
        return await stores_collection.find_one({'store_name': store_name})
    except Exception as e:
        logger.error(f"Error getting store {store_name}: {e}")
        return None

async def db_delete_store(store_name: str) -> bool:
    """Deletes a store document by name."""
    if not stores_collection: return False
    try:
        result = await stores_collection.delete_one({'store_name': store_name})
        return result.deleted_count > 0
    except Exception as e:
        logger.error(f"Error deleting store {store_name}: {e}")
        return False

async def db_get_all_store_names() -> List[str]:
    """Retrieves all store names for display."""
    if not stores_collection: return []
    try:
        names = await stores_collection.find({}, {'store_name': 1}).to_list(length=None)
        return [doc['store_name'] for doc in names]
    except Exception as e:
        logger.error(f"Error fetching all store names: {e}")
        return []

async def db_update_store_caption(store_name: str, new_caption: str, new_counters: Dict[str, int]) -> bool:
    """Updates the caption and counters for an existing store."""
    if not stores_collection: return False
    try:
        result = await stores_collection.update_one(
            {'store_name': store_name},
            {'$set': {
                'caption_template': new_caption,
                'caption_counters': new_counters,
                'last_modified': datetime.now()
            }}
        )
        return result.acknowledged
    except Exception as e:
        logger.error(f"Error updating store caption for {store_name}: {e}")
        return False
        
# ---------------------------

# ---- utilities ----
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

# ... [Existing utility functions like is_drive_url, extract_drive_id, generate_new_filename, get_video_duration, parse_time remain the same] ...

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

# --- NEW UTILITY: Store Selection Keyboard ---
async def store_selection_keyboard(uid: int, view_mode: bool = True) -> InlineKeyboardMarkup:
    """Generates a keyboard with all saved store names."""
    store_names = await db_get_all_store_names()
    if not store_names:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("No Stores Available", callback_data="none")]
        ])

    keyboard = []
    current_store = USER_CURRENT_STORE_NAME.get(uid)
    for name in store_names:
        # Checkmark if it's the current active store
        indicator = " (✅ Active)" if name == current_store else ""
        
        if view_mode:
            # View mode: buttons for selecting an action on the store
            keyboard.append([
                InlineKeyboardButton(f"{name}{indicator}", callback_data=f"store_detail_{name}"),
                InlineKeyboardButton("Set Active", callback_data=f"store_select_{name}"),
                InlineKeyboardButton("Delete 🗑️", callback_data=f"store_delete_{name}")
            ])
        else:
            # Simple select/set mode
            keyboard.append([
                InlineKeyboardButton(f"{name}{indicator}", callback_data=f"store_select_{name}")
            ])

    # Add a button to manage stores if in view mode
    if view_mode:
        keyboard.append([InlineKeyboardButton("Done ✅", callback_data="done_viewing_stores")])
        
    return InlineKeyboardMarkup(keyboard)
# ---------------------------------------------


# --- NEW UTILITY: Dynamic Caption Processing ---

def process_dynamic_caption(uid: int, caption_template: str, store_name: Optional[str] = None) -> str:
    """
    Processes the dynamic caption template, updates the counter, and returns the final caption.
    If store_name is provided, it uses/updates the MongoDB counter.
    Otherwise, it uses/updates the local USER_COUNTERS.
    """
    
    # 1. Determine which counter dictionary to use (local or store's)
    counters: Dict[str, int] = {}
    if store_name:
        # Counters are retrieved from DB later or assumed to be in the store object passed to it.
        # Since this function is used BEFORE sending, we must use the counter data from DB
        # or rely on the caller to update the DB later.
        # For simplicity in this function, we will return the updated counter dict as well.
        # The calling function (process_file_and_upload/handle_caption_only_upload) will handle DB update.
        # For now, we use the local counter as fallback if not in store mode and assume caller handles DB.
        pass # The actual counter value will be passed by the caller function if in store mode.
    else:
        counters = USER_COUNTERS.get(uid, {})


    def get_counter_value(key: str) -> int:
        return counters.get(key, 0)
    
    def increment_counter(key: str) -> None:
        counters[key] = get_counter_value(key) + 1
    
    final_caption = caption_template
    new_counters = dict(counters) # Create a copy for modification

    # --- 1. Dynamic Counter Increment ([XX] and [(XX)]) ---
    def counter_replacer(match):
        full_match = match.group(0)
        
        # Check if it's the incrementing format: [01] or [(01)]
        if re.search(r"^\(?\d{2}\)?$", match.group(1)):
            key = 'main' # Fixed key for the main counter
            current_value = new_counters.get(key, int(match.group(1)) - 1)
            new_value = current_value + 1
            new_counters[key] = new_value
            
            # Format the output with leading zero (e.g., 01, 10, 100)
            # Find the required padding based on the original template
            padding = len(match.group(1).strip('()'))
            
            # Reconstruct the string with the new value
            new_value_str = str(new_value).zfill(padding)
            
            # Check for parentheses: [(XX)] -> (new_value)
            if full_match.startswith('(') and full_match.endswith(')'):
                return f"({new_value_str})"
            # Check for simple brackets: [XX] -> new_value
            else:
                return new_value_str
        
        # This shouldn't happen with the current regex pattern, but for safety
        return full_match 

    # Regex to capture [XX], [(XX)], [re (A, B)], [TEXT (XX)]
    # We will process in order: 1. Counter, 2. Conditional, 3. Cycle (re)
    
    # 1. Dynamic Counter (The main one to increment)
    # Pattern for [01], [(01)] etc. - captured by group 1 which will be used as a flag
    # This must be processed first to determine the current episode number
    
    # Regex for dynamic counter: [01], [(01)], [10], [(10)]
    counter_pattern = r'\[(\(?\d{1,}\)?)]' 
    
    # Find all potential dynamic counters. We only support *one* main counter per caption.
    counter_matches = re.findall(counter_pattern, final_caption)
    
    current_episode_number = None
    
    for match_text in counter_matches:
        # Check if it's the incrementing type (digits only)
        if re.match(r'^\(?\d{1,}\)?$', match_text):
            key = 'main' 
            # If a store is active, get the last saved counter value from the store's data
            if store_name:
                store_data = asyncio.run(db_get_store(store_name))
                last_value = store_data['caption_counters'].get(key, int(match_text.strip('()')) - 1)
            else:
                last_value = new_counters.get(key, int(match_text.strip('()')) - 1)
                
            new_value = last_value + 1
            new_counters[key] = new_value
            current_episode_number = new_value # Set the current episode number
            
            padding = len(match_text.strip('()'))
            new_value_str = str(new_value).zfill(padding)
            
            replacement = f"({new_value_str})" if match_text.startswith('(') else new_value_str
            
            # Replace only the first instance (or all if desired, but typically there is only one main counter)
            final_caption = final_caption.replace(f"[{match_text}]", replacement, 1) 
            break # Assume only one main counter for incrementing

    # --- 2. Conditional Text ([TEXT (XX)]) ---
    # Pattern: [TEXT (XX)] - where XX is the condition (episode number)
    if current_episode_number is not None:
        conditional_pattern = r"\[(.+?)\s*\((?P<condition>\d+)\)\]"
        
        def conditional_replacer(match):
            text_to_use = match.group(1).strip()
            condition_num = int(match.group('condition'))
            
            if current_episode_number == condition_num:
                return text_to_use
            else:
                return ""
                
        final_caption = re.sub(conditional_pattern, conditional_replacer, final_caption)
    
    # --- 3. Cycle Replacement ([re (A, B, ...)]) ---
    # Pattern: [re (item1, item2)]
    cycle_pattern = r'\[re\s*\((.*?)\)\]'

    def cycle_replacer(match):
        key = 'cycle' # Fixed key for cycle counter
        
        # Get the list of items
        items_str = match.group(1)
        items = [item.strip() for item in items_str.split(',')]
        if not items or items == ['']:
            return match.group(0) # Return original if empty
            
        # Determine the current index (local or store-based)
        if store_name:
            # Must retrieve from DB as this function only processes one instance
            store_data = asyncio.run(db_get_store(store_name))
            current_index = store_data['caption_counters'].get(key, 0)
        else:
            current_index = new_counters.get(key, 0)
            
        selected_item = items[current_index % len(items)]
        
        # Update the index for the next run
        new_counters[key] = current_index + 1
        
        return selected_item

    final_caption = re.sub(cycle_pattern, cycle_replacer, final_caption)

    # If the user is not in a store, update the local counter
    if not store_name:
        USER_COUNTERS[uid] = new_counters
        
    return final_caption, new_counters

# ---------------------------------------------


# ... [Existing utility functions like mode_check_keyboard, get_audio_tracks_ffprobe, download functions remain the same] ...

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


# ... [Existing utility functions like get_audio_tracks_ffprobe, download functions remain the same] ...

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
        # --- NEW STORE COMMANDS ---
        BotCommand("store", "নতুন স্টোর তৈরি করুন (admin only)"),
        BotCommand("set_store", "আপলোডের জন্য বর্তমান স্টোর সেট করুন (admin only)"),
        BotCommand("view_store", "স্টোরগুলো দেখুন এবং ম্যানেজ করুন (admin only)"),
        BotCommand("delete_store", "একটি স্টোর ডিলিট করুন (admin only)"),
        # --------------------------
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
        # --- NEW STORE COMMANDS IN HELP ---
        "/store - নতুন স্টোর তৈরি করুন (admin only)\n"
        "/set_store <store_name> - আপলোডের জন্য বর্তমান স্টোর সেট করুন (admin only)\n"
        "/view_store - স্টোরগুলো দেখুন এবং ম্যানেজ করুন (admin only)\n"
        "/delete_store <store_name> - একটি স্টোর ডিলিট করুন (admin only)\n"
        # ----------------------------------
        "/broadcast <text> - ব্রডকাস্ট (শুধুমাত্র অ্যাডমিন)\n"
        "/help - সাহায্য"
    )
    
    current_store = USER_CURRENT_STORE_NAME.get(m.from_user.id)
    if current_store:
        text += f"\n\n**🎯 বর্তমানে নির্বাচিত স্টোর:** `{current_store}`"
        
    await m.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("help") & filters.private)
async def help_handler(c, m):
    await start_handler(c, m)

# ... [Existing setthumb, view_thumb, del_thumb, photo_handler, set_caption, view_caption, delete_caption_cb, toggle_edit_caption_mode, toggle_audio_change_mode, mode_check_cmd, mode_toggle_callback handlers remain the same] ...

# --- STORE COMMANDS ---

@app.on_message(filters.command("store") & filters.private)
async def create_store_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not MONGO_URI:
        await m.reply_text("MongoDB কানেকশন সেটআপ করা নেই। স্টোর তৈরি সম্ভব নয়।")
        return
        
    uid = m.from_user.id
    STORE_NAME_REQUEST.add(uid)
    USER_STORE_TEMP[uid] = {} # Initialize temp store data
    
    await m.reply_text("দয়া করে নতুন **স্টোরের নাম** দিন (যেমন: 'Dragon Ball Z', 'One Piece')।")

@app.on_message(filters.command("set_store") & filters.private)
async def set_store_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not MONGO_URI:
        await m.reply_text("MongoDB কানেকশন সেটআপ করা নেই। স্টোর সেট করা সম্ভব নয়।")
        return
        
    uid = m.from_user.id
    
    if len(m.command) > 1:
        store_name = m.text.split(None, 1)[1].strip()
        store_data = await db_get_store(store_name)
        
        if store_data:
            USER_CURRENT_STORE_NAME[uid] = store_name
            await m.reply_text(f"**✅ সফল!**\nএখন থেকে আপলোডের জন্য `{store_name}` স্টোরটি সক্রিয়। এই স্টোরের ক্যাপশন এবং থাম্বনেইল ব্যবহার হবে।")
        else:
            await m.reply_text(f"স্টোর `{store_name}` খুঁজে পাওয়া যায়নি। `/view_store` দিয়ে স্টোরগুলো দেখুন।")
    else:
        # Prompt for store selection if no name is provided
        keyboard = await store_selection_keyboard(uid, view_mode=False)
        await m.reply_text("অনুগ্রহ করে নিচের তালিকা থেকে একটি স্টোর নির্বাচন করুন, অথবা `/set_store <store_name>` ব্যবহার করুন:", reply_markup=keyboard)

@app.on_message(filters.command("view_store") & filters.private)
async def view_store_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not MONGO_URI:
        await m.reply_text("MongoDB কানেকশন সেটআপ করা নেই। স্টোর দেখা সম্ভব নয়।")
        return

    uid = m.from_user.id
    keyboard = await store_selection_keyboard(uid, view_mode=True)
    
    if keyboard.inline_keyboard[0][0].text == "No Stores Available":
        await m.reply_text("কোনো সেভ করা স্টোর নেই। `/store` দিয়ে তৈরি করুন।")
    else:
        await m.reply_text("স্টোরগুলো ম্যানেজ করুন (Set Active, Delete):", reply_markup=keyboard)

@app.on_message(filters.command("delete_store") & filters.private)
async def delete_store_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not MONGO_URI:
        await m.reply_text("MongoDB কানেকশন সেটআপ করা নেই। ডিলিট করা সম্ভব নয়।")
        return
        
    uid = m.from_user.id
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: `/delete_store <store_name>`")
        return

    store_name = m.text.split(None, 1)[1].strip()
    
    if await db_delete_store(store_name):
        # Clear the active store if it was the one deleted
        if USER_CURRENT_STORE_NAME.get(uid) == store_name:
            USER_CURRENT_STORE_NAME.pop(uid)
            await m.reply_text(f"**✅ সফল!** স্টোর `{store_name}` ডিলিট করা হয়েছে এবং আপনার বর্তমান সক্রিয় স্টোর বাতিল করা হয়েছে।")
        else:
            await m.reply_text(f"**✅ সফল!** স্টোর `{store_name}` ডিলিট করা হয়েছে।")
    else:
        await m.reply_text(f"স্টোর `{store_name}` খুঁজে পাওয়া যায়নি বা ডিলিট করা সম্ভব হয়নি।")

# --- STORE CALLBACKS ---
@app.on_callback_query(filters.regex("^store_(select|delete|detail)_"))
async def store_callback_handler(c: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return
        
    action, store_name = cb.data.split('_', 2)

    if action == 'select':
        USER_CURRENT_STORE_NAME[uid] = store_name
        await cb.answer(f"'{store_name}' স্টোরটি সক্রিয় করা হলো।", show_alert=True)
        # Try to update the message if it was from /view_store
        try:
            await cb.message.edit_reply_markup(reply_markup=await store_selection_keyboard(uid, view_mode=True))
        except Exception:
            pass # Ignore if edit fails
            
    elif action == 'delete':
        if await db_delete_store(store_name):
            if USER_CURRENT_STORE_NAME.get(uid) == store_name:
                USER_CURRENT_STORE_NAME.pop(uid)
            await cb.answer(f"'{store_name}' স্টোরটি ডিলিট করা হয়েছে।", show_alert=True)
            try:
                await cb.message.edit_reply_markup(reply_markup=await store_selection_keyboard(uid, view_mode=True))
            except Exception:
                pass
        else:
            await cb.answer(f"'{store_name}' ডিলিট করা সম্ভব হয়নি।", show_alert=True)
            
    elif action == 'detail':
        store_data = await db_get_store(store_name)
        if store_data:
            caption_info = store_data.get('caption_template', 'None')
            counter_info = store_data.get('caption_counters', {'main': 0, 'cycle': 0})
            
            detail_text = (
                f"** স্টোর বিস্তারিত: `{store_name}`**\n\n"
                f"**ক্যাপশন টেমপ্লেট:**\n`{caption_info}`\n\n"
                f"**বর্তমান কাউন্টার:**\n"
                f"- পর্বের নম্বর (main): `{counter_info.get('main', '0')}`\n"
                f"- সাইকেল কাউন্টার (cycle): `{counter_info.get('cycle', '0')}`\n\n"
                f"**থাম্বনেইল:** {'সেভ করা আছে' if store_data.get('thumb_file_id') else 'নেই'}"
            )
            await cb.message.reply_text(detail_text, parse_mode=ParseMode.MARKDOWN)
            await cb.answer("বিস্তারিত তথ্য দেখুন।")
        else:
            await cb.answer("স্টোর খুঁজে পাওয়া যায়নি।", show_alert=True)

@app.on_callback_query(filters.regex("done_viewing_stores"))
async def done_viewing_stores_cb(c, cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("আপনার অনুমতি নেই।", show_alert=True)
        return
    await cb.message.edit_text("স্টোর ম্যানেজমেন্ট সম্পন্ন হয়েছে।")


@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id
    
    # --- NEW: Handle store thumbnail request ---
    if uid in STORE_THUMB_REQUEST:
        STORE_THUMB_REQUEST.discard(uid)
        store_name = USER_STORE_TEMP.get(uid, {}).get('store_name')
        if not store_name:
            await m.reply_text("স্টোরের নাম সেট করা হয়নি। অনুগ্রহ করে আবার `/store` কমান্ডটি চালান।")
            USER_STORE_TEMP.pop(uid, None)
            return

        try:
            # Store the file_id instead of downloading the image for store thumbnail
            thumb_file_id = m.photo.file_id
            
            # Finalize store data
            store_data = USER_STORE_TEMP.pop(uid)
            store_data.update({
                'thumb_file_id': thumb_file_id,
                'caption_template': None,
                'caption_counters': {'main': 0, 'cycle': 0},
                'created_at': datetime.now()
            })
            
            if await db_save_store(store_data):
                USER_CURRENT_STORE_NAME[uid] = store_name # Set as active immediately
                await m.reply_text(
                    f"**✅ সফল!** স্টোর `{store_name}` তৈরি এবং সক্রিয় করা হয়েছে।\n"
                    f"থাম্বনেইল সেভ হয়েছে। এখন `/set_caption` ব্যবহার করে এই স্টোরের জন্য ক্যাপশন সেট করুন।",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await m.reply_text("স্টোর সেভ করতে সমস্যা হয়েছে।")
        except Exception as e:
            await m.reply_text(f"স্টোর থাম্বনেইল সেভ করতে সমস্যা: {e}")
        return
    # -------------------------------------------
    
    # Existing setthumb logic (for user's general thumb)
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


@app.on_message(filters.text & filters.private)
async def text_handler(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    text = m.text.strip()
    
    # --- NEW: Handle store name request ---
    if uid in STORE_NAME_REQUEST:
        STORE_NAME_REQUEST.discard(uid)
        store_name = text
        if await db_get_store(store_name):
            await m.reply_text(f"স্টোর `{store_name}` ইতিমধ্যেই আছে। অন্য নাম দিন অথবা `/set_store {store_name}` ব্যবহার করুন।")
            STORE_NAME_REQUEST.add(uid) # Re-add the request state
            return
            
        USER_STORE_TEMP.setdefault(uid, {})['store_name'] = store_name
        STORE_THUMB_REQUEST.add(uid)
        await m.reply_text(f"স্টোরের নাম (`{store_name}`) সেভ হয়েছে। এখন স্টোরের জন্য **একটি ছবি** পাঠান — সেট হবে আপনার স্টোরের থাম্বনেইল।")
        return
    # ----------------------------------------
    
    # Handle set caption request
    if uid in SET_CAPTION_REQUEST:
        SET_CAPTION_REQUEST.discard(uid)
        
        current_store = USER_CURRENT_STORE_NAME.get(uid)
        if current_store:
            # Save to MongoDB store document
            if await stores_collection.update_one(
                {'store_name': current_store},
                {'$set': {'caption_template': text}}
            ):
                # Reset counters when a new caption template is set
                await stores_collection.update_one(
                    {'store_name': current_store},
                    {'$set': {'caption_counters': {'main': 0, 'cycle': 0}}}
                )
                await m.reply_text(f"স্টোর `{current_store}` এর জন্য আপনার ক্যাপশন এবং কাউন্টার রিসেট করে সেভ করা হয়েছে।")
            else:
                await m.reply_text(f"স্টোর `{current_store}` এর ক্যাপশন সেভ করা সম্ভব হয়নি।")
        else:
            # Save to local memory (existing logic)
            USER_CAPTIONS[uid] = text
            USER_COUNTERS.pop(uid, None) # New: reset counter on new caption set
            await m.reply_text("আপনার লোকাল ক্যাপশন সেভ হয়েছে। এখন থেকে আপলোড করা ভিডিওতে এই ক্যাপশন ব্যবহার হবে।")
        return

    # ... [Existing audio remux input handler remains the same] ...
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

    # Handle auto URL upload
    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))

# ... [Existing upload_url_cmd, handle_url_download_and_upload, handle_caption_only_upload, forwarded_file_or_direct_file, handle_audio_change_file handlers need full implementation but will be placeholders here] ...


# --- PLACEHOLDER FUNCTIONS ---
# NOTE: The actual implementation of these functions would contain the file processing and Pyrogram upload logic.
# They are only included as placeholders to ensure the new state and utility functions are correctly called.
# The `process_file_and_upload` and `handle_caption_only_upload` are CRITICAL for the new store logic.

async def process_file_and_upload(c: Client, m: Message, tmp_path: Path, original_name: str, messages_to_delete: List[int] = []):
    """
    Handles file renaming, thumbnail selection, dynamic caption processing, and final upload.
    This function must be updated to check for active store and use its settings.
    """
    uid = m.from_user.id
    current_store_name = USER_CURRENT_STORE_NAME.get(uid)
    final_caption_template = None
    
    # 1. Determine Caption & Counter Source
    if current_store_name:
        store_data = await db_get_store(current_store_name)
        if store_data:
            final_caption_template = store_data.get('caption_template')
            counter_data = store_data.get('caption_counters', {'main': 0, 'cycle': 0})
        else:
            final_caption_template = USER_CAPTIONS.get(uid)
            counter_data = USER_COUNTERS.get(uid, {})
    else:
        final_caption_template = USER_CAPTIONS.get(uid)
        counter_data = USER_COUNTERS.get(uid, {})

    # 2. Process Dynamic Caption
    final_caption, new_counters = final_caption_template, None
    if final_caption_template:
        final_caption, new_counters = process_dynamic_caption(uid, final_caption_template, store_name=current_store_name)
    
    # 3. Update DB/Local Counter
    if current_store_name and new_counters:
        await db_update_store_caption(current_store_name, final_caption_template, new_counters)
    elif new_counters:
        USER_COUNTERS[uid] = new_counters

    # 4. Determine Thumbnail
    thumb_path = None
    if current_store_name and store_data and store_data.get('thumb_file_id'):
        thumb_path = store_data['thumb_file_id']
    elif uid in USER_THUMBS and Path(USER_THUMBS[uid]).exists():
        thumb_path = USER_THUMBS[uid]
    
    # ... [Remaining file processing and Pyrogram upload logic here] ...
    try:
        # Example of final upload using Pyrogram
        await c.send_document(
            chat_id=m.chat.id if not STORE_CHANNEL_ID else STORE_CHANNEL_ID, # Use store channel if set
            document=str(tmp_path),
            file_name=generate_new_filename(original_name) if uid not in EDIT_CAPTION_MODE else original_name,
            caption=final_caption,
            thumb=thumb_path,
            parse_mode=ParseMode.MARKDOWN
        )
        await m.reply_text("ফাইল সফলভাবে আপলোড করা হয়েছে।")
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await m.reply_text(f"আপলোডে সমস্যা: {e}")
    finally:
        try:
            tmp_path.unlink()
            for msg_id in messages_to_delete:
                await c.delete_messages(m.chat.id, msg_id)
        except Exception:
            pass


async def handle_caption_only_upload(c: Client, m: Message):
    uid = m.from_user.id
    current_store_name = USER_CURRENT_STORE_NAME.get(uid)
    
    final_caption_template = None
    counter_data = None
    thumb_path = None

    if current_store_name:
        store_data = await db_get_store(current_store_name)
        if store_data:
            final_caption_template = store_data.get('caption_template')
            counter_data = store_data.get('caption_counters', {'main': 0, 'cycle': 0})
            thumb_path = store_data.get('thumb_file_id')
    
    # Fallback to local if no store or no store caption
    if not final_caption_template:
        final_caption_template = USER_CAPTIONS.get(uid)
        counter_data = USER_COUNTERS.get(uid, {})
        thumb_path = USER_THUMBS.get(uid) if uid in USER_THUMBS and Path(USER_THUMBS[uid]).exists() else None

    if not final_caption_template:
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
            
        # Process the dynamic caption
        final_caption, new_counters = process_dynamic_caption(uid, final_caption_template, store_name=current_store_name)
        
        # Update DB/Local Counter
        if current_store_name and new_counters:
            await db_update_store_caption(current_store_name, final_caption_template, new_counters)
        elif new_counters:
            USER_COUNTERS[uid] = new_counters
            
        # Use the file ID directly for re-uploading with new caption/thumb
        if file_info.file_id:
            try:
                if source_message.video:
                    await c.send_video(
                        chat_id=m.chat.id if not STORE_CHANNEL_ID else STORE_CHANNEL_ID,
                        video=file_info.file_id,
                        caption=final_caption,
                        thumb=thumb_path if thumb_path else (file_info.thumbs[0].file_id if file_info.thumbs else None),
                        duration=file_info.duration,
                        supports_streaming=True,
                        parse_mode=ParseMode.MARKDOWN
                    )
                elif source_message.document:
                    await c.send_document(
                        chat_id=m.chat.id if not STORE_CHANNEL_ID else STORE_CHANNEL_ID,
                        document=file_info.file_id,
                        file_name=file_info.file_name,
                        caption=final_caption,
                        thumb=thumb_path if thumb_path else (file_info.thumbs[0].file_id if file_info.thumbs else None),
                        parse_mode=ParseMode.MARKDOWN
                    )
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                try:
                    await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
                except Exception:
                    await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
                return
        else:
            try:
                await status_msg.edit("ফাইলের ফাইল আইডি পাওয়া যায়নি।", reply_markup=None)
            except Exception:
                await m.reply_text("ফাইলের ফাইল আইডি পাওয়া যায়নি।", reply_markup=None)
            return

        # New code to auto-delete the success message
        try:
            success_msg = await status_msg.edit("ক্যাপশন সফলভাবে আপডেট করা হয়েছে।", reply_markup=None)
            await asyncio.sleep(5)
            await success_msg.delete()
        except Exception:
            success_msg = await m.reply_text("ক্যাপশন সফলভাবে আপডেট করা হয়েছে।", reply_markup=None)
            await asyncio.sleep(5)
            try:
                await success_msg.delete()
            except Exception:
                pass
                
    except Exception as e:
        traceback.print_exc()
        try:
            await status_msg.edit(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
        except Exception:
            await m.reply_text(f"ক্যাপশন এডিটে ত্রুটি: {e}", reply_markup=None)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass


async def handle_audio_remux(c: Client, m: Message, in_path: Path, original_name: str, stream_map: List[str], messages_to_delete: List[int] = []):
    """Placeholder for existing MKV audio remux logic."""
    await m.reply_text("অডিও রিমুক্সিং সম্পন্ন, এখন আপলোড শুরু হবে (প্লেসহোল্ডার)।")
    # Call process_file_and_upload after remux is done and the new file is saved.
    await process_file_and_upload(c, m, in_path, original_name, messages_to_delete)


async def handle_audio_change_file(c: Client, m: Message):
    """Placeholder for existing MKV audio change file logic."""
    uid = m.from_user.id
    await m.reply_text("MKV ফাইল ডাউনলোড সম্পন্ন। অডিও ট্র্যাক অর্ডারের জন্য অপেক্ষা করছে (প্লেসহোল্ডার)।")
    # Populate AUDIO_CHANGE_FILE with path and track list here
    # AUDIO_CHANGE_FILE[uid] = {'path': str(tmp_path), 'original_name': original_name, 'tracks': tracks_list, 'message_id': m.id}
    # Then wait for text_handler input

async def run_flask_and_ping():
    flask_thread = threading.Thread(target=lambda: flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False))
    flask_thread.start()
    ping_thread = threading.Thread(target=ping_service)
    ping_thread.start()
    print("Flask and Ping services started.")

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
        time.sleep(600) # Ping every 10 minutes

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

# --- END PLACEHOLDER FUNCTIONS ---


@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c: Client, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return

    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        await m.reply_text("নাম পরিবর্তন করতে একটি ভিডিও বা ডকুমেন্ট ফাইলে রিপ্লাই করুন।")
        return

    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: `/rename <newname.ext>`")
        return

    new_name_full = m.text.split(None, 1)[1].strip()
    
    # We use the existing file ID for quick rename (no re-download needed)
    file_info = m.reply_to_message.video or m.reply_to_message.document
    
    uid = m.from_user.id
    current_store_name = USER_CURRENT_STORE_NAME.get(uid)
    
    final_caption_template = None
    counter_data = None
    thumb_path = None

    if current_store_name:
        store_data = await db_get_store(current_store_name)
        if store_data:
            final_caption_template = store_data.get('caption_template')
            counter_data = store_data.get('caption_counters', {'main': 0, 'cycle': 0})
            thumb_path = store_data.get('thumb_file_id')
    
    if not final_caption_template:
        final_caption_template = USER_CAPTIONS.get(uid)
        counter_data = USER_COUNTERS.get(uid, {})
        thumb_path = USER_THUMBS.get(uid) if uid in USER_THUMBS and Path(USER_THUMBS[uid]).exists() else None
        
    final_caption, new_counters = final_caption_template, None
    if final_caption_template:
        final_caption, new_counters = process_dynamic_caption(uid, final_caption_template, store_name=current_store_name)
        
    # Update DB/Local Counter
    if current_store_name and new_counters:
        await db_update_store_caption(current_store_name, final_caption_template, new_counters)
    elif new_counters:
        USER_COUNTERS[uid] = new_counters

    try:
        if m.reply_to_message.video:
            await c.send_video(
                chat_id=m.chat.id if not STORE_CHANNEL_ID else STORE_CHANNEL_ID,
                video=file_info.file_id,
                file_name=new_name_full,
                caption=final_caption,
                thumb=thumb_path if thumb_path else (file_info.thumbs[0].file_id if file_info.thumbs else None),
                duration=file_info.duration,
                supports_streaming=True,
                parse_mode=ParseMode.MARKDOWN
            )
        elif m.reply_to_message.document:
            await c.send_document(
                chat_id=m.chat.id if not STORE_CHANNEL_ID else STORE_CHANNEL_ID,
                document=file_info.file_id,
                file_name=new_name_full,
                caption=final_caption,
                thumb=thumb_path if thumb_path else (file_info.thumbs[0].file_id if file_info.thumbs else None),
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Delete the original message and the command message
        await m.reply_to_message.delete()
        await m.delete()

    except Exception as e:
        await m.reply_text(f"নাম পরিবর্তন করে আপলোড করতে সমস্যা: {e}")


# ... [Existing broadcast_cmd, web_display handlers remain the same] ...

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /broadcast <text>")
        return

    text = m.text.split(None, 1)[1]
    sent_count = 0
    fail_count = 0
    total = len(SUBSCRIBERS)

    msg = await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে... ({total} জন গ্রাহকের কাছে)")

    for chat_id in list(SUBSCRIBERS):
        try:
            await c.send_message(chat_id, text)
            sent_count += 1
            await asyncio.sleep(0.1)  # Small delay to avoid flood waits
        except Exception:
            fail_count += 1

    await msg.edit_text(f"ব্রডকাস্ট সম্পন্ন!\nসফল: {sent_count}\nব্যর্থ: {fail_count}")

# --- WEB HOOKS (existing logic) ---

@flask_app.route('/')
def index():
    return render_template_string("Bot is running!")

@flask_app.route('/subscribers')
def subscribers():
    return f"Subscribers: {len(SUBSCRIBERS)}"

# -----------------------------------


if __name__ == "__main__":
    print("Bot চালু হচ্ছে... Flask and Ping threads start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask_and_ping, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
        # Use run_until_complete to start pyrogram client synchronously
        # We start the client and then run forever using idle()
        with app:
             loop.run_until_complete(set_bot_commands())
             app.run()
    except KeyboardInterrupt:
        print("Bot বন্ধ হচ্ছে...")
    except Exception as e:
        logger.error(f"Main execution error: {e}")
