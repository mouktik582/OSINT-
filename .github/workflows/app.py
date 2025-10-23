import aiohttp
import asyncio
import logging
import re
import math
import random
import html
import string
import pickle
import json
from functools import wraps
from typing import Dict, Any, List
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes
)
from datetime import datetime, timedelta

# ---- Your Bot Token ----
TELEGRAM_TOKEN = "8287776342:AAGT468t40l1U6KZ6r1lUc_nJdEUfoKBtmE"

# ---- APIs ----
NUM_API = "http://46.202.179.237:8080/num={number}" 
VEHICLE_API = "https://rc-info-ng.vercel.app/?rc="
AADHAR_API = "http://dark-op.dev-is.xyz/?key=wasdark&aadhaar="
UPI_API = "http://dark-op.dev-is.xyz/?key=wasdark&upi="

# ---- Admin ID ----  
ADMIN_ID = [961896183] 

# ---- Channel for Force Subscribe ----
CHANNEL_USERNAME = "@Modxclusivee"

# ---- Database ----
USERS_FILE = 'users.pkl'
REDEEM_CODES_FILE = 'redeem_codes.pkl'
API_STATUS_FILE = 'api_status.pkl'

# Global variables for data storage
users = {}
redeem_codes = {}
api_status = {}

def load_data():
    """Load or initialize data from pickle files."""
    global users, redeem_codes, api_status
    try:
        with open(USERS_FILE, 'rb') as f:
            users = pickle.load(f)
        print("✅ Users data loaded successfully.")
    except (FileNotFoundError, EOFError):
        users = {}
        print("⚠️ Users data file not found, starting with empty data.")

    try:
        with open(REDEEM_CODES_FILE, 'rb') as f:
            redeem_codes = pickle.load(f)
        print("✅ Redeem codes loaded successfully.")
    except (FileNotFoundError, EOFError):
        redeem_codes = {}
        print("⚠️ Redeem codes file not found, starting with empty data.")

    try:
        with open(API_STATUS_FILE, 'rb') as f:
            api_status = pickle.load(f)
        print("✅ API status data loaded successfully.")
    except (FileNotFoundError, EOFError):
        api_status = {'num': True, 'vehicle': True, 'aadhar': True, 'upi': True}
        print("⚠️ API status file not found, starting with all services active.")

def save_data():
    """Save data to pickle files."""
    try:
        with open(USERS_FILE, 'wb') as f:
            pickle.dump(users, f)
        with open(REDEEM_CODES_FILE, 'wb') as f:
            pickle.dump(redeem_codes, f)
        with open(API_STATUS_FILE, 'wb') as f:
            pickle.dump(api_status, f)
    except Exception as e:
        print(f"❌ Error saving data: {e}")

# Logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Escape for MarkdownV2
def escape_md(text: str) -> str:
    """Helper function to escape special characters for MarkdownV2."""
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    # Escape backslashes first, then other special characters
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text.replace("\\", "\\\\"))


# ------------------------- FORCE SUBSCRIBE DECORATOR -------------------------
def check_channel_membership(func):
    """Decorator to check if a user is a member of the required channel."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return

        if users.get(user.id, {}).get("banned", False):
            logger.info(f"Blocked access for banned user {user.id}")
            if update.effective_message:
                await update.effective_message.reply_text("❌ You have been banned from using this bot.")
            return

        if user.id in ADMIN_ID:
            return await func(update, context, *args, **kwargs)

        try:
            member = await context.bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user.id)
            if member.status not in ['creator', 'administrator', 'member']:
                join_button = InlineKeyboardButton("Join Channel", url=f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}")
                reply_markup = InlineKeyboardMarkup([[join_button]])
                message_text = (
                    "🚨 *Access Denied!*\n\n"
                    "To use this feature, you must first join our official channel. "
                    "Please join and then try again."
                )
                if update.callback_query:
                    await update.callback_query.answer(text="You must join the channel first!", show_alert=True)
                elif update.effective_message:
                    await update.effective_message.reply_text(
                        message_text, parse_mode="Markdown", reply_markup=reply_markup
                    )
                return
        except Exception as e:
            logger.error(f"Error checking membership for user {user.id}: {e}")
            if update.effective_message:
                await update.effective_message.reply_text(
                    f"Sorry, I couldn't verify your channel membership. Please ensure the bot is an admin in the channel and try again.\n\n`Error: {escape_md(str(e))}`"
                )
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


# ------------------------- CONVERSATION STATES -------------------------
(
    GEN_LIMIT, GEN_DAYS, REVOKE_USER, REVOKE_CONFIRM, 
    SEARCH_USER, EDIT_USER_ID, EDIT_USER_ACTION, EDIT_USER_VALUE,
    BROADCAST_MESSAGE, BROADCAST_CONFIRM, API_STATUS_MENU
) = range(11)

def find_user_id(identifier: str) -> int | None:
    """Finds a user's ID whether given an ID or a username."""
    try:
        user_id = int(identifier)
        if user_id in users:
            return user_id
    except ValueError:
        search_username = identifier.lstrip('@').lower()
        for uid, user_data in users.items():
            if user_data.get('username') and user_data['username'].lower() == search_username:
                return uid
    return None


# ------------------------- START -------------------------
@check_channel_membership
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command, registering new users and handling referrals."""
    user = update.message.from_user
    uid = user.id

    try:
        member = await context.bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=uid)
        if member.status not in ['creator', 'administrator', 'member']:
            join_button = InlineKeyboardButton("Join Channel", url=f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}")
            reply_markup = InlineKeyboardMarkup([[join_button]])
            await update.message.reply_text(
                "🚨 *Access Denied!*\n\n"
                "To use this bot, you must first join our official channel. "
                "Please join and then type /start again.",
                parse_mode="Markdown", reply_markup=reply_markup
            )
            return
    except Exception as e:
        logger.error(f"Error checking membership for user {uid} on start: {e}")
        await update.message.reply_text(
            f"Sorry, I couldn't verify your channel membership. Please ensure the bot is an admin in the channel and try again.\n\n`Error: {escape_md(str(e))}`"
        )
        return

    ref_id = None
    if context.args:
        try:
            ref_id = int(context.args[0])
            if ref_id == uid:
                ref_id = None
        except (ValueError, IndexError):
            ref_id = None

    if uid not in users:
        users[uid] = {
            "first_name": user.first_name, "username": user.username, "limit": 5,
            "expiry": datetime.max, "unlimited": False, "referred_by": ref_id, "referrals": []
        }
        await update.message.reply_text(
            f"🎉 Welcome {user.first_name}! You got *5 free searches*.", parse_mode="Markdown"
        )
        if ref_id and ref_id in users:
            users[ref_id]["limit"] += 5
            users[ref_id].setdefault("referrals", []).append(uid)
            try:
                await context.bot.send_message(
                    chat_id=ref_id,
                    text=f"🥳 *New Referral\\!* You earned *5 free searches* because {escape_md(user.first_name)} joined using your link\\.",
                    parse_mode="MarkdownV2"
                )
            except Exception as e:
                logger.error(f"Failed to notify referrer {ref_id}: {e}")
    else:
        users[uid]['first_name'] = user.first_name
        users[uid]['username'] = user.username

    save_data()

    keyboard = [
        ["📞 Number Search", "🚗 Vehicle Search"],
        ["💳 Aadhar Search", "🏦 UPI Search"],
        ["👤 Profile Info", "ℹ️ About Bot"],
        ["🎟 Redeem Code", "💳 Buy Plan"],
        ["🗣 Refer & Earn"]
    ]
    
    start_message = (
    "✨ *Welcome to ModXclusive OSINT Bot* ✨\n\n"
    "Use the following commands or the buttons below:\n\n"
    "📞 `/num <mobile_number>` → Search mobile info\n"
    "🚗 `/vehicle <vehicle_number>` → Search vehicle info\n"
    "💳 `/aadhar <aadhar_number>` → Search Aadhar info\n"
    "🏦 `/upi <upi_id>` → Search UPI info\n\n"
    "*⚡ Example:*\n"
    "`/num 9876543210`\n"
    "`/vehicle HR07W9009`\n"
    "`/aadhar 123456789012`\n"
    "`/upi user@bank`\n\n"
    "🤖 *BOT ADMIN*: @MOUKTIK5911" 
)
    await update.message.reply_text(
        start_message, parse_mode="Markdown", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

# ------------------------- ADMIN PANEL-------------------------
async def panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the enhanced admin panel for authorized users."""
    if update.message:
        user_id = update.message.from_user.id
        send_message = update.message.reply_text
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
        send_message = update.callback_query.edit_message_text
    else:
        return

    if user_id not in ADMIN_ID:
        return await send_message("⛔ Unauthorized")

    kb = [
        [
            InlineKeyboardButton("🎟 Generate Code", callback_data="gen"),
            InlineKeyboardButton("👤 Search User", callback_data="search_user_start")
        ],
        [
            InlineKeyboardButton("✏️ Edit User", callback_data="edit_user_start"),
            InlineKeyboardButton("📢 Broadcast", callback_data="broadcast_start")
        ],
        [
            InlineKeyboardButton("👥 Active Users", callback_data="users_page_0"),
            InlineKeyboardButton("📈 Top Referrers", callback_data="referrers_page_0")
        ],
        [
            InlineKeyboardButton("🚫 Revoke Access", callback_data="revoke_start"),
            InlineKeyboardButton("⚙️ API Status", callback_data="api_status_menu_entry")
        ]
    ]
    await send_message("⚙️ *Admin Panel*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def panel_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles callback queries from the admin panel."""
    query = update.callback_query
    await query.answer()
    d = query.data

    if d == "gen":
        kb = [
            [InlineKeyboardButton("🔢 Limit Based", callback_data="gen_limit_start")],
            [InlineKeyboardButton("♾️ Unlimited", callback_data="gen_unlimited_start")]
        ]
        await query.edit_message_text("Choose Code Type:", reply_markup=InlineKeyboardMarkup(kb))

    elif d.startswith("referrers_page_"):
        try:
            page = int(d.split('_')[-1])
        except (ValueError, IndexError):
            page = 0
        
        ITEMS_PER_PAGE = 10
        
        referrer_list = sorted(
            [u for u in users.items() if u[1].get('referrals')],
            key=lambda i: len(i[1].get('referrals', [])),
            reverse=True
        )
        if not referrer_list:
            return await query.edit_message_text("No one has referred a user yet.")
        
        total_referrers = len(referrer_list)
        total_pages = math.ceil(total_referrers / ITEMS_PER_PAGE)
        
        start_index = page * ITEMS_PER_PAGE
        end_index = start_index + ITEMS_PER_PAGE
        paginated_referrers = referrer_list[start_index:end_index]
        
        msg = f"📈 *Total Referrers: {escape_md(str(total_referrers))}*\n\n"
        
        for idx, (uid, user_data) in enumerate(paginated_referrers, start=start_index + 1):
            name = escape_md(user_data.get('first_name', 'Unknown'))
            count = len(user_data.get('referrals', []))
            msg += f"{idx}\\. *{name}* \\(`{uid}`\\) → {count} Referrals\n"
        
        msg += f"\nPage {page + 1} of {total_pages}"
        
        navigation_buttons = []
        if page > 0:
            navigation_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"referrers_page_{page-1}"))
        if end_index < total_referrers:
            navigation_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"referrers_page_{page+1}"))
        
        kb = [navigation_buttons] if navigation_buttons else []
        kb.append([InlineKeyboardButton("⬅️ Back to Panel", callback_data="back_to_panel")])
        
        await query.edit_message_text(msg, parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup(kb))

    elif d == "gen_limit_start":
        await query.edit_message_text("Enter search limit (e.g., 10):")
        return GEN_LIMIT

    elif d == "gen_unlimited_start":
        await query.edit_message_text("Enter validity in days (0 for Lifetime):")
        return GEN_DAYS

    elif d == "revoke_start":
        await query.edit_message_text("🚫 Enter the *User ID* or *@username* you want to revoke access for:", parse_mode="Markdown")
        return REVOKE_USER

    elif d == "search_user_start":
        await query.edit_message_text("👤 Enter the *User ID* or *@username* to search for:", parse_mode="Markdown")
        return SEARCH_USER

    elif d == "edit_user_start":
        await query.edit_message_text("✏️ Enter the User ID or @username of the user you want to edit:")
        return EDIT_USER_ID

    elif d == "broadcast_start":
        await query.edit_message_text("📢 Enter the message you want to broadcast to all users.")
        return BROADCAST_MESSAGE
    
    elif d == "api_status_menu_entry":
        await api_status_handler(update, context)
        return API_STATUS_MENU
        
    elif d.startswith("users_page_"):
        try:
            page = int(d.split('_')[-1])
        except (ValueError, IndexError):
            page = 0
            
        ITEMS_PER_PAGE = 10
        user_list = list(users.items())
        
        if not user_list:
            return await query.edit_message_text("No active users.")
        
        total_users = len(user_list)
        total_pages = math.ceil(total_users / ITEMS_PER_PAGE)
        
        start_index = page * ITEMS_PER_PAGE
        end_index = start_index + ITEMS_PER_PAGE
        paginated_users = user_list[start_index:end_index]
        
        msg = f"👥 *Active Users: {escape_md(str(total_users))}*\n\n"
        
        for idx, (uid, user_data) in enumerate(paginated_users, start=start_index + 1):
            
            first_name = user_data.get('first_name', 'Unknown User')
            username = user_data.get('username')
            
            display_name = f"*{escape_md(first_name)}*"
            if username:
                display_name += f" \\(@{escape_md(username)}\\)"

            plan_info = 'Unlimited' if user_data.get('unlimited') else f"{user_data.get('limit', 0)} searches"
            exp = "Lifetime" if user_data.get('expiry') == datetime.max else user_data.get('expiry').strftime('%Y-%m-%d')
            status = " 🚫 *BANNED*" if user_data.get("banned") else ""

            msg += f"{idx}\\. {display_name}\n"
            
            msg += f"    → `ID: {uid}` \\| Plan: {escape_md(plan_info)} \\| Exp: {escape_md(exp)}{status}\n"
            
            
        msg += f"\nPage {page + 1} of {total_pages}"
        
        navigation_buttons = []
        if page > 0:
            navigation_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"users_page_{page-1}"))
        if end_index < total_users:
            navigation_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"users_page_{page+1}"))
        
        kb = [navigation_buttons] if navigation_buttons else []
        kb.append([InlineKeyboardButton("⬅️ Back to Panel", callback_data="back_to_panel")])
        
        await query.edit_message_text(msg, parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup(kb))
    
    elif d == "back_to_panel":
        await panel(update, context)
        return ConversationHandler.END


# ------------------------- API Status Handlers -------------------------

async def api_status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # Only answer the query if it's NOT the entry point (which calls panel_actions first)
    if not query.data.endswith("entry"): 
        await query.answer()

    api_name = query.data.replace("toggle_api_", "")
    if query.data.startswith("toggle_api_") and api_name in api_status:
        api_status[api_name] = not api_status[api_name]
        save_data()
        await query.answer(f"{api_name.upper()} service {'enabled' if api_status[api_name] else 'disabled'}.", show_alert=True)
    
    kb = [
        [InlineKeyboardButton(f"📞 Num Search: {'✅ On' if api_status.get('num', True) else '❌ Off'}", callback_data="toggle_api_num")],
        [InlineKeyboardButton(f"🚗 Vehicle Search: {'✅ On' if api_status.get('vehicle', True) else '❌ Off'}", callback_data="toggle_api_vehicle")],
        [InlineKeyboardButton(f"💳 Aadhar Search: {'✅ On' if api_status.get('aadhar', True) else '❌ Off'}", callback_data="toggle_api_aadhar")],
        [InlineKeyboardButton(f"🏦 UPI Search: {'✅ On' if api_status.get('upi', True) else '❌ Off'}", callback_data="toggle_api_upi")],
        [InlineKeyboardButton("⬅️ Back to Panel", callback_data="back_to_panel")]
    ]
    
    status_msg = "⚙️ *Current API Status*:\n\nSelect a service to toggle its status\\."
    
    await query.edit_message_text(status_msg, parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup(kb))
    return API_STATUS_MENU

async def back_to_panel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exits the API status conversation and returns to the main panel."""
    await update.callback_query.answer()
    await panel(update, context)
    return ConversationHandler.END


# ------------------------- OTHER CONVERSATION HANDLERS -------------------------
async def gen_limit_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(update.message.text.strip())
        if limit <= 0:
            await update.message.reply_text("❌ Limit must be a positive number.")
            return GEN_LIMIT
            
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        redeem_codes[code] = {"limit": limit, "days": 0, "unlimited": False}
        save_data()
        msg = (f"🎟 *New Code Generated!*\n\n`Code:  ` `{code}`\n`Plan:  ` {limit} Searches\n`Validity: ` Lifetime")
        await update.message.reply_text(msg, parse_mode="Markdown")
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ Invalid number. Try again.")
        return GEN_LIMIT

async def gen_days_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text.strip())
        if days < 0:
            await update.message.reply_text("❌ Days must be 0 or a positive number.")
            return GEN_DAYS
            
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        redeem_codes[code] = {"limit": 0, "days": days, "unlimited": True}
        save_data()
        validity = "Lifetime" if days == 0 else f"{days} days"
        msg = (f"🎟 *New Code Generated!*\n\n`Code:  ` `{code}`\n`Plan:  ` Unlimited\n`Validity: ` {validity}")
        await update.message.reply_text(msg, parse_mode="Markdown")
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ Invalid input. Enter number of days.")
        return GEN_DAYS

async def revoke_user_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    identifier = update.message.text.strip()
    user_id_to_revoke = find_user_id(identifier)
    
    if user_id_to_revoke is None:
        await update.message.reply_text("❌ User not found.")
        return ConversationHandler.END

    if user_id_to_revoke in ADMIN_ID:
        await update.message.reply_text("❌ You cannot revoke an admin.")
        return ConversationHandler.END
        
    context.user_data['user_to_revoke'] = user_id_to_revoke
    user_info = users[user_id_to_revoke]
    name = user_info.get('first_name', 'N/A')
    
    kb = [[
        InlineKeyboardButton("✅ Yes, Revoke", callback_data="revoke_confirm_yes"),
        InlineKeyboardButton("❌ No, Cancel", callback_data="revoke_confirm_no")
    ]]
    await update.message.reply_text(
        f"⚠️ Are you sure you want to revoke access for user *{escape_md(name)}* (`{user_id_to_revoke}`)?",
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return REVOKE_CONFIRM

async def revoke_confirm_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id_to_revoke = context.user_data.get('user_to_revoke')

    if query.data == 'revoke_confirm_yes':
        if user_id_to_revoke in users:
            del users[user_id_to_revoke]
            save_data()
            await query.edit_message_text(f"✅ Access for user `{user_id_to_revoke}` has been revoked.")
        else:
            await query.edit_message_text("❌ User not found (they may have been removed already).")
    else:
        await query.edit_message_text("❌ Action cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

async def search_user_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    identifier = update.message.text.strip()
    user_id_to_search = find_user_id(identifier)

    if user_id_to_search:
        d = users[user_id_to_search]
        name = d.get('first_name', 'N/A')
        username = d.get('username')
        display_name = f"@{username}" if username else name
        plan = "♾️ Unlimited" if d.get("unlimited") else f"🔢 {d.get('limit', 0)} Searches"
        expiry = "Lifetime" if d.get("expiry") == datetime.max else d.get("expiry").strftime('%d-%b-%Y')
        total_referrals = len(d.get('referrals', []))
        
        status = "\n🚫 *Status:* BANNED" if d.get("banned") else ""

        msg = (
            f"👤 *User Profile*\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 *Name:* {escape_md(display_name)}\n"
            f"🆔 *User ID:* `{user_id_to_search}`\n"
            f"✅ *Plan:* {escape_md(plan)}\n"
            f"📅 *Valid Till:* {escape_md(expiry)}\n"
            f"🤝 *Total Referrals:* {total_referrals}"
            f"{escape_md(status)}"
        )
        await update.message.reply_text(msg, parse_mode="MarkdownV2")
    else:
        await update.message.reply_text("❌ User not found.")
    return ConversationHandler.END
    
async def edit_user_id_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    identifier = update.message.text.strip()
    user_id_to_edit = find_user_id(identifier)

    if user_id_to_edit is None:
        await update.message.reply_text("❌ User not found. Please try again.")
        return EDIT_USER_ID
        
    context.user_data['user_to_edit'] = user_id_to_edit
    
    is_banned = users.get(user_id_to_edit, {}).get("banned", False)
    ban_button = InlineKeyboardButton("✅ Unban User", callback_data="edit_unban_user") if is_banned else InlineKeyboardButton("🚫 Ban User", callback_data="edit_ban_user")

    kb = [
        [InlineKeyboardButton("➕ Add/Remove Searches", callback_data="edit_add_limit")],
        [InlineKeyboardButton("🗓 Set Expiry (Days)", callback_data="edit_set_expiry")],
        [InlineKeyboardButton("♾️ Set Unlimited Plan", callback_data="edit_make_unlimited")],
        [ban_button],
        [InlineKeyboardButton("❌ Cancel", callback_data="edit_cancel")]
    ]
    await update.message.reply_text(f"Select an action for user `{user_id_to_edit}`:", reply_markup=InlineKeyboardMarkup(kb))
    return EDIT_USER_ACTION

async def edit_user_action_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data
    context.user_data['edit_action'] = action
    user_id = context.user_data.get('user_to_edit')

    if action == 'edit_add_limit':
        await query.edit_message_text("🔢 How many searches to add? (Use a negative number to remove, e.g., -5)")
        return EDIT_USER_VALUE
    elif action == 'edit_set_expiry':
        await query.edit_message_text("🗓 Enter new plan validity in days from now (use 0 for Lifetime).")
        return EDIT_USER_VALUE
    elif action == 'edit_make_unlimited':
        await query.edit_message_text("♾️ Enter validity in days for the Unlimited plan (use 0 for Lifetime).")
        return EDIT_USER_VALUE
    elif action == 'edit_ban_user':
        if user_id in users:
            users[user_id]['banned'] = True
            save_data()
            await query.edit_message_text(f"🚫 User `{user_id}` has been **banned**.", parse_mode="Markdown")
            try:
                await context.bot.send_message(chat_id=user_id, text="🚫 You have been banned from using this bot by an admin.")
            except Exception as e:
                logger.error(f"Failed to send ban notification to {user_id}: {e}")
        else:
            await query.edit_message_text("❌ User not found.")
        context.user_data.clear()
        return ConversationHandler.END
    elif action == 'edit_unban_user':
        if user_id in users:
            users[user_id].pop('banned', None)
            save_data()
            await query.edit_message_text(f"✅ User `{user_id}` has been **unbanned**.", parse_mode="Markdown")
            try:
                await context.bot.send_message(chat_id=user_id, text="✅ You have been unbanned by an admin and can now use the bot again.")
            except Exception as e:
                logger.error(f"Failed to send unban notification to {user_id}: {e}")
        else:
            await query.edit_message_text("❌ User not found.")
        context.user_data.clear()
        return ConversationHandler.END
    elif action == 'edit_cancel':
        await query.edit_message_text("❌ Edit action cancelled.")
        context.user_data.clear()
        return ConversationHandler.END
    
    if query.data == "edit_cancel":
        await query.edit_message_text("❌ Edit action cancelled.")
        context.user_data.clear()
        return ConversationHandler.END

async def edit_user_value_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = context.user_data.get('user_to_edit')
    action = context.user_data.get('edit_action')
    try:
        value = int(update.message.text.strip())
        notification_text = None

        if user_id not in users:
            await update.message.reply_text("❌ User not found.")
        elif action == 'edit_add_limit':
            users[user_id]['limit'] = users[user_id].get('limit', 0) + value
            # Ensure limit doesn't go negative
            if users[user_id]['limit'] < 0:
                users[user_id]['limit'] = 0
                
            save_data()
            await update.message.reply_text(f"✅ Updated searches for user `{user_id}`. New total: *{users[user_id]['limit']}*.", parse_mode="Markdown")
            notification_text = f"An admin has updated your plan. You now have *{users[user_id]['limit']}* searches available."
        elif action == 'edit_set_expiry':
            users[user_id]['unlimited'] = False
            users[user_id]['expiry'] = datetime.max if value == 0 else datetime.now() + timedelta(days=value)
            expiry_date = "Lifetime" if value == 0 else users[user_id]['expiry'].strftime('%d-%b-%Y')
            save_data()
            await update.message.reply_text(f"✅ Plan for user `{user_id}` now expires on: *{expiry_date}*.", parse_mode="Markdown")
            notification_text = f"An admin has updated your plan. Your new expiry date is: *{expiry_date}*."
        elif action == 'edit_make_unlimited':
            users[user_id]['unlimited'] = True
            users[user_id]['expiry'] = datetime.max if value == 0 else datetime.now() + timedelta(days=value)
            # When making unlimited, reset limit to 0 (no expiry based limit applies)
            users[user_id]['limit'] = 0 
            expiry_date = "Lifetime" if value == 0 else users[user_id]['expiry'].strftime('%d-%b-%Y')
            save_data()
            await update.message.reply_text(f"✅ User `{user_id}` plan has been set to *Unlimited*. Expires on: *{expiry_date}*.", parse_mode="Markdown")
            notification_text = f"An admin has set your plan to *Unlimited*. It is valid until: *{expiry_date}*."
        
        if notification_text:
            try:
                await context.bot.send_message(chat_id=user_id, text=f"✨ *Plan Update*\n\n{notification_text}", parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Failed to send plan update notification to {user_id}: {e}")

    except ValueError:
        await update.message.reply_text("❌ That's not a valid number. Please try again.")
        return EDIT_USER_VALUE
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {e}")
    context.user_data.clear()
    return ConversationHandler.END

async def broadcast_message_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['broadcast_message'] = update.message.text
    total_users = len(users)
    kb = [[
        InlineKeyboardButton("✅ Yes, Send Now", callback_data="broadcast_confirm_yes"),
        InlineKeyboardButton("❌ No, Cancel", callback_data="broadcast_confirm_no")
    ]]
    await update.message.reply_text(
        f"Your message is:\n\n---\n{update.message.text}\n---\n\nAre you sure you want to send this to *{total_users}* users?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return BROADCAST_CONFIRM

async def broadcast_confirm_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == 'broadcast_confirm_no':
        await query.edit_message_text("❌ Broadcast cancelled.")
        context.user_data.clear()
        return ConversationHandler.END

    await query.edit_message_text("🚀 Sending broadcast... Please wait.")
    message_to_send = context.user_data['broadcast_message']
    
    final_message = f"🔊 *Broadcast*\n\n{message_to_send}"
    
    success_count = 0
    fail_count = 0
    
    for user_id in list(users.keys()):
        try:
            await context.bot.send_message(chat_id=user_id, text=final_message, parse_mode="Markdown")
            success_count += 1
        except Exception as e:
            logger.warning(f"Failed to send broadcast to {user_id}: {e}")
            fail_count += 1
        await asyncio.sleep(0.2) 

    await query.edit_message_text(f"✅ Broadcast complete!\n\nSent: {success_count}\nFailed: {fail_count}")
    context.user_data.clear()
    return ConversationHandler.END
    
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Action cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

# ------------------------- BUTTON HANDLER -------------------------
@check_channel_membership
async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text
    actions = {
        "📞 Number Search": "/num <number>", 
        "🚗 Vehicle Search": "/vehicle <number>",
        "💳 Aadhar Search": "/aadhar <number>",
        "🏦 UPI Search": "/upi <id>",
        "🎟 Redeem Code": "/redeem <code>", 
        "👤 Profile Info": profile,
        "ℹ️ About Bot": about, 
        "💳 Buy Plan": buy, 
        "🗣 Refer & Earn": referral
    }
    action = actions.get(txt)
    if isinstance(action, str):
        await update.message.reply_text(f"Please use the command format: `{action}`", parse_mode="Markdown")
    elif callable(action):
        await action(update, context)

# ------------------------- REFERRAL & OTHER COMMANDS -------------------------
@check_channel_membership
async def referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in users:
        await update.message.reply_text("Please use the /start command first.")
        return
    bot_username = (await context.bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start={user_id}"
    ref_count = len(users[user_id].get('referrals', []))
    msg = (
        f"🗣️ *Refer & Earn!*\n\n"
        f"Share your link! For every new user who joins, you get *5 free searches*.\n\n"
        f"🔗 *Your Link:*\n`{referral_link}`\n\n"
        f"You have referred *{ref_count}* people."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

@check_channel_membership
async def activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚡ *Activation System*\n\n"
        "👉 To activate your plan, use a redeem code:\n"
        "`/redeem <code>`\n\n"
        "💡 To purchase a plan, use /buy.", parse_mode="Markdown"
    )

@check_channel_membership
async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔢 Limited Search Plans", callback_data="choose_limited")],
        [InlineKeyboardButton("♾ Unlimited Plans", callback_data="choose_unlimited")]
    ]
    await update.message.reply_text(
        "💳 *Choose your plan type:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    plans_map = {
        "plan_l1": ("1 Search", "₹10", "https://rzp.io/rzp/r4eaI90n"),
        "plan_l5": ("5 Searches", "₹40", "https://rzp.io/rzp/h7kVPn1y"),
        "plan_l25": ("25 Searches", "₹130", "https://rzp.io/rzp/HYtEGly"),
        "plan_l50": ("50 Searches", "₹400", "https://rzp.io/rzp/LG9OJAf"),
        "plan_l100": ("100 Searches", "₹700", "https://rzp.io/rzp/OJhDqfW"),
        "plan_u1d": ("1 Day Unlimited", "₹100", "https://rzp.io/rzp/MpIfspdg"),
        "plan_u7d": ("7 Days Unlimited", "₹300", "https://rzp.io/rzp/R387Qvv"),
        "plan_u1m": ("1 Month Unlimited", "₹800", "https://rzp.io/rzp/DKJa40f"),
        "plan_u6m": ("6 Month Unlimited", "₹1500", "https://rzp.io/rzp/apxVz2K"),
        "plan_u1Y": ("12 Month Unlimited", "₹2000", "https://rzp.io/rzp/Og0sFHe2"),
    }
    if data == "choose_limited":
        kb = [
            [InlineKeyboardButton("1 Search - ₹10", callback_data="plan_l1")],
            [InlineKeyboardButton("5 Searches - ₹40", callback_data="plan_l5")],
            [InlineKeyboardButton("25 Searches - ₹130", callback_data="plan_l25")],
            [InlineKeyboardButton("50 Searches - ₹400", callback_data="plan_l50")],
            [InlineKeyboardButton("100 Searches - ₹700", callback_data="plan_l100")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_buy_options")]
        ]
        await query.edit_message_text("🔢 *Limited Search Plans*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    elif data == "choose_unlimited":
        kb = [
            [InlineKeyboardButton("1 Day Unlimited - ₹100", callback_data="plan_u1d")],
            [InlineKeyboardButton("7 Days Unlimited - ₹300", callback_data="plan_u7d")],
            [InlineKeyboardButton("1 Month Unlimited - ₹800", callback_data="plan_u1m")],
            [InlineKeyboardButton("6 Month Unlimited - ₹1500", callback_data="plan_u6m")],
            [InlineKeyboardButton("12 Month Unlimited - ₹2000", callback_data="plan_u1Y")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_buy_options")]
        ]
        await query.edit_message_text("♾ *Unlimited Plans*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    elif data == "back_to_buy_options":
        kb = [[InlineKeyboardButton("🔢 Limited Plans", callback_data="choose_limited")], [InlineKeyboardButton("♾ Unlimited Plans", callback_data="choose_unlimited")]]
        await query.edit_message_text("💳 *Choose your plan type:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    elif data in plans_map:
        plan_name, price, link = plans_map[data]
        payment_message = (f"💳 *Payment for: {plan_name}*\n\n✅ After paying, send a message to the admin @Mouktik5911 with your payment screenshot for verification.\n\n⚡ Your plan will be activated shortly after.")
        kb = [[InlineKeyboardButton(f"Pay {price} Now", url=link)]]
        await query.edit_message_text(payment_message, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb), disable_web_page_preview=True)

@check_channel_membership
async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "ℹ️ *About ModXclusive OSINT Bot*\n"
        "━━━━━━━━━━━━━━━\n\n"
        "• 📞 Mobile Number Lookup\n"
        "• 🚗 Vehicle Info Lookup\n"
        "• 💳 Paid Plans Available (/buy)\n\n"
        "*👨‍💻 Developer:* @MOUKTIK5911\n"
        "*🔒 Note:* For Educational Purposes Only"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

@check_channel_membership
async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    uid = user.id
    if uid not in users:
        return await update.message.reply_text("❌ No active plan. Use /start to register.")
    
    d = users[uid]
    plan = "♾️ Unlimited" if d.get("unlimited") else f"🔢 {d.get('limit', 0)} Searches"
    expiry = "Lifetime" if d.get("expiry") == datetime.max else d.get("expiry").strftime('%d-%b-%Y')
    total_referrals = len(d.get('referrals', []))
    referrals_text = f"🤝 *Total Referrals:* {total_referrals}"
    
    referred_users_list = d.get('referrals', [])
    if referred_users_list:
        referrals_text += "\n\n👥 *Users You Referred:*\n"
        for referred_uid in referred_users_list:
            referred_user_data = users.get(referred_uid)
            if referred_user_data:
                name = escape_md(referred_user_data.get('first_name', 'Unknown'))
                referrals_text += f"• {name} \\(`{referred_uid}`\\)\n"
            else:
                referrals_text += f"• Unknown User \\(`{referred_uid}`\\)\n"

    msg = (
        f"👤 *User Profile*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"👤 *Name:* {escape_md(user.full_name)}\n"
        f"🆔 *User ID:* `{uid}`\n"
        f"✅ *Plan:* {escape_md(plan)}\n"
        f"📅 *Valid Till:* {escape_md(expiry)}\n"
        f"{referrals_text}"
    )
    await update.message.reply_text(msg, parse_mode="MarkdownV2")

# ------------------------- SEARCH & REDEEM FUNCTIONS -------------------------
def check_user_plan_access(user_id):
    if user_id not in users:
        return False, "❌ Use /start to get started."
    user_data = users[user_id]
    # Check for ban status first
    if user_data.get('banned'):
        return False, "❌ You have been banned from using this bot."
    if user_data.get('unlimited') and (user_data.get('expiry') > datetime.now()):
        return True, None
    if user_data.get('limit', 0) > 0:
        return True, None
    return False, "❌ You have run out of searches or your plan has expired. Use /buy to get more."

@check_channel_membership
async def search_num(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    has_access, message = check_user_plan_access(user_id)
    if not has_access:
        return await update.message.reply_text(message)

    if not api_status.get('num', True):
        return await update.message.reply_text("❌ This service is temporarily unavailable. Please try again later.")
    
    if not context.args or len(context.args) > 1 or not re.match(r"^\d{10}$", context.args[0]):
        return await update.message.reply_text(
            "❌ *Invalid Format* \n\nPlease use the correct format: `/num 9876543210`\n(Must be exactly 10 digits, no spaces or country code)",
            parse_mode="MarkdownV2" 
        )
    
    number = context.args[0]
    msg = await update.message.reply_text(f"🔍 Searching for `{escape_md(number)}`\\.\\.\\.", parse_mode="MarkdownV2")
    
    try:
        # Assuming NUM_API is defined as a format string, e.g., "http://api.example.com/search?num={number}"
        url = NUM_API.format(number=number)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    
                    # CRITICAL FIX 1: Extract all 'dataX' objects from the response
                    results_list = []
                    for key, value in data.items():
                        if key.startswith('data') and isinstance(value, dict):
                            results_list.append(value)

                    if results_list:
                        # CRITICAL FIX 2: Corrected display_map keys to match the provided API response structure (fname, alt, id)
                        display_map = {
                            'mobile': ("📞", "*Mobile*"), 
                            'name': ("👤", "*Name*"),
                            'fname': ("👨‍👦", "*Father's Name*"), # Key in API response
                            'address': ("🏠", "*Address*"),
                            'circle': ("📍", "*Circle*"),
                            'id': ("🆔", "*ID/Aadhaar*"), # Key in API response
                            'alt': ("📱", "*Alt No*"), # Key in API response
                        }
                        
                        # CRITICAL FIX 3: Corrected display_order to use the actual API keys
                        display_order = ['mobile', 'name', 'fname', 'address', 'circle', 'id', 'alt'] 
                        
                        result_msg = f"📞 *Number Search Results for {escape_md(number)}*\n"
                        result_msg += "━━━━━━━━━━━━━━━━━━\n"  
                        
                        for item_dict in results_list:
                            # Use the search number if 'mobile' is missing
                            if 'mobile' not in item_dict or not item_dict['mobile']:
                                item_dict['mobile'] = number 
                            
                            remaining_details = item_dict.copy()
                            
                            for key in display_order:
                                if key in remaining_details and remaining_details[key] and str(remaining_details[key]).strip().upper() != 'NA':
                                    icon, label = display_map.get(key, ("", f"*{escape_md(key.title())}*"))
                                    value = str(remaining_details[key])
                                    
                                    # Clean up address formatting if needed
                                    if key == 'address':
                                        value = value.replace('!', ' ').strip()
                                        
                                    result_msg += f"{icon} {label}: `{escape_md(value)}`\n"
                                    
                                    del remaining_details[key]
                            
                            # Append any other details found in the dictionary but not in the main order
                            for key, value in remaining_details.items():
                                # Exclude 'success' and 'Channel' from remaining details
                                if value and key not in ['success', 'Channel'] and str(value).strip().upper() != 'NA':
                                    # Added replacement to display 'alt_mobile' nicely if it's not in the main order
                                    result_msg += f"🔸 *{escape_md(key.replace('_', ' ').title())}:* `{escape_md(str(value))}`\n" 

                            if results_list.index(item_dict) < len(results_list) - 1:
                                result_msg += "━━━━━━━━━━━━━━━━━━\n"
                        
                        await msg.edit_text(result_msg, parse_mode="MarkdownV2")

                        if not users[user_id].get('unlimited'):
                            users[user_id]['limit'] -= 1
                            save_data()
                    else:
                        await msg.edit_text("❌ No results found.")
                else:
                    await msg.edit_text(f"❌ Service unavailable (Status: {response.status})")
    except Exception as e:
        print(f"Error in search_num: {e}") 
        await msg.edit_text("❌ An unexpected error occurred while searching. Please try again later.")

@check_channel_membership
async def search_vehicle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    has_access, message = check_user_plan_access(user_id)
    if not has_access:
        return await update.message.reply_text(message)
    if not api_status.get('vehicle', True):
        return await update.message.reply_text("❌ This service is temporarily unavailable.")

    if not context.args or len(context.args) > 1:
        return await update.message.reply_text(
            "❌ *Invalid Format* \\- *Missing/Too Many Arguments*\n"
            "Please use the correct format: `/vehicle HR26BC1234`",
            parse_mode="MarkdownV2"
        )
        
    vehicle_number = context.args[0].upper()
    
    if not re.match(r"^[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{1,4}$", vehicle_number):
        return await update.message.reply_text(
            "❌ *Invalid Vehicle Number Format*\n\n"
            "Please ensure it's a valid vehicle number format, e.g., `/vehicle HR26BC1234` "
            "(Format: 2 Letters, 1-2 Digits, 1-3 Letters, 1-4 Digits).",
            parse_mode="MarkdownV2"
        )
        
    
    msg = await update.message.reply_text(
        f"🔍 Searching for `{escape_md(vehicle_number)}`\\.\\.\\.",
        parse_mode="MarkdownV2"
    )
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
            async with session.get(VEHICLE_API + vehicle_number) as response:
                if response.status == 200:
                    try:
                        data = await response.json(content_type=None)
                    except (aiohttp.ContentTypeError, json.JSONDecodeError):
                        return await msg.edit_text("❌ The API returned an invalid or empty response.")

                    
                    if data: 
                        # Remove 'owner' key if present (already covered by owner_name/father_name)
                        if 'owner' in data:
                            del data['owner']
                        
                        display_map = {
                            'rc_number': ("🚗", "*RC Number*"),
                            'owner_name': ("👤", "*Owner Name*"),
                            'father_name': ("👨‍👦", "*Father/Husband Name*"),
                            'registration_date': ("📅", "*Reg\\. Date*"),
                            'maker_model': ("🚘", "*Make / Model*"),
                            'vehicle_class': ("🗂️", "*Vehicle Class*"),
                            'fuel_type': ("⛽", "*Fuel Type*"),
                            'fuel_norms': ("🟢", "*Emission Norms*"),
                            'insurance_expiry': ("🛡️", "*Insurance Expiry*"),
                            'fitness_upto': ("⚙️", "*Fitness Upto*"),
                            'puc_upto': ("💨", "*PUC Upto*"),
                            'financier_name': ("🏦", "*Financier*"),
                            'rto': ("📍", "*RTO*"),
                            'address': ("🏠", "*Address*"),
                        }
                        
                        display_order = list(display_map.keys())
                        
                        result_msg = "🚗 *Vehicle Search Results*\n"
                        result_msg += "━━━━━━━━━━━━━━━━━━\n"
                        result_msg += f"🚗 *Number:* `{escape_md(vehicle_number)}`\n" 
                        
                        remaining_details = data.copy()
                        
                        for key in display_order:
                            value = remaining_details.get(key)
                            if key == 'rc_number':
                                if key in remaining_details:
                                    del remaining_details[key]
                                continue

                            if value is not None and value != "NA" and str(value).strip():
                                icon, label = display_map[key]
                                value_text = str(value)
                                
                                value_text_mono = f"`{escape_md(value_text)}`"

                                result_msg += f"{icon} {label}: {value_text_mono}\n"
                                
                            if key in remaining_details:
                                del remaining_details[key]

                        extra_details_added = False
                        ignore_keys = ['owner_serial_no', 'insurance_company', 'insurance_upto', 'tax_upto', 'city', 'phone', 'model_name', 'credit', 'developer']

                        for key, value in remaining_details.items():
                            if value and key not in ignore_keys:
                                if not extra_details_added:
                                    result_msg += "➖➖➖➖➖➖➖➖➖➖\n"
                                    extra_details_added = True
                                
                                label = escape_md(key.replace('_', ' ').title())
                                result_msg += f"🔸 *{label}:* `{escape_md(str(value))}`\n"
                                
                        
                        await msg.edit_text(result_msg, parse_mode="MarkdownV2")

                        if not users[user_id].get('unlimited'):
                            users[user_id]['limit'] -= 1
                            save_data()
                            
                    else: 
                        await msg.edit_text(f"❌ No results found for vehicle number `{escape_md(vehicle_number)}`.", parse_mode="MarkdownV2")

                else:
                    await msg.edit_text(f"❌ Service unavailable (Status: {response.status})")
    except Exception as e:
        await msg.edit_text("❌ An unexpected error occurred while searching. Please try again later.", parse_mode="MarkdownV2")

@check_channel_membership
async def search_aadhar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    has_access, message = check_user_plan_access(user_id)
    if not has_access:
        return await update.message.reply_text(message)

    if not api_status.get('aadhar', True):
        return await update.message.reply_text("❌ This service is temporarily unavailable. Please try again later.")
    
    if not context.args or len(context.args) > 1 or not re.match(r"^\d{12}$", context.args[0]):
        return await update.message.reply_text(
            "❌ *Invalid Format* \n\nPlease use the correct format: `/aadhar 123456789012`\n(Must be exactly 12 digits)",
            parse_mode="MarkdownV2"
        )
    
    aadhar_number = context.args[0]
    msg = await update.message.reply_text(f"🔍 Searching for Aadhar `{escape_md(aadhar_number)}`\\.\\.\\.", parse_mode="MarkdownV2")
    
    try:
        headers = {'Cache-Control': 'no-cache', 'Pragma': 'no-cache', 'Expires': '0'}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
            logger.info(f"Making preliminary request for AADHAR {aadhar_number} to clear server cache.")
            await session.get(AADHAR_API + aadhar_number, headers=headers)
            await asyncio.sleep(1)
            
            logger.info(f"Making main request for AADHAR {aadhar_number}.")
            async with session.get(AADHAR_API + aadhar_number, headers=headers) as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    results_list = data.get("data") if isinstance(data, dict) else data
                    
                    if results_list and isinstance(results_list, list) and results_list[0]: # Check if list is not empty
                        # CRITICAL FIX 3: Corrected display_map keys for Aadhar (assuming it follows Num API naming convention)
                        display_map = {
                            'name': ("👤", "*Name*"),
                            'mobile': ("📱", "*Mobile*"),
                            'father_name': ("👨‍👦", "*Father*"), # Corrected from 'fname'
                            'address': ("🏠", "*Address*"),
                            'circle': ("📍", "*Circle*"),
                            'id_number': ("🆔", "*ID/Aadhaar*"),  # Corrected from 'id'
                            'alt_mobile': ("📞", "*Alt Contact*"), # Corrected from 'alt'
                        }
                        
                        # CRITICAL FIX 3: Corrected display_order keys
                        display_order = ['name', 'mobile', 'father_name', 'address', 'circle', 'id_number', 'alt_mobile']
                        
                        result_msg = "💳 *Aadhar Search Results*\n"
                        result_msg += "━━━━━━━━━━━━━━━━━━\n"
                        result_msg += f"🆔 *Aadhar:* `{escape_md(aadhar_number)}`\n\n"
                        
                        for item_dict in results_list:
                            remaining_details = item_dict.copy()
                            
                            for key in display_order:
                                if key in remaining_details and remaining_details[key]:
                                    icon, label = display_map.get(key, ("", f"*{escape_md(key.title())}*"))
                                    value = str(remaining_details[key])
                                    
                                    result_msg += f"{icon} {label}: `{escape_md(value)}`\n"
                                    
                                    del remaining_details[key]
                            
                            for key, value in remaining_details.items():
                                # Exclude 'id' (internal ID) and other unneeded keys
                                if value and key not in ['id', 'success']: 
                                    result_msg += f"🔸 *{escape_md(key.replace('_', ' ').title())}:* `{escape_md(str(value))}`\n"

                            if results_list.index(item_dict) < len(results_list) - 1:
                                result_msg += "━━━━━━━━━━━━━━━━━━\n"

                        await msg.edit_text(result_msg, parse_mode="MarkdownV2")
                        
                        if not users[user_id].get('unlimited'):
                            users[user_id]['limit'] -= 1
                            save_data()
                    else:
                        await msg.edit_text("❌ No results found.")
                else:
                    await msg.edit_text(f"❌ Service unavailable (Status: {response.status})")
    except Exception as e:
        logger.error(f"Error in search_aadhar: {e}")
        await msg.edit_text("❌ An unexpected error occurred while searching. Please try again later.", parse_mode="MarkdownV2")

@check_channel_membership
async def search_upi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    has_access, message = check_user_plan_access(user_id)
    if not has_access:
        return await update.message.reply_text(message)

    if not api_status.get('upi', True):
        return await update.message.reply_text("❌ This service is temporarily unavailable\\.", parse_mode="MarkdownV2")

    if not context.args or len(context.args) > 1 or not re.match(r"^[\w.-]+@[\w.-]+$", context.args[0]):
        return await update.message.reply_text(
            "❌ *Invalid Format* \n\nPlease use the correct format: `/upi user@bank`",
            parse_mode="MarkdownV2"
        )
    
    upi_id = context.args[0]
    msg = await update.message.reply_text(f"🔍 Searching for UPI ID `{escape_md(upi_id)}`\\.\\.\\.", parse_mode="MarkdownV2")
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
            async with session.get(UPI_API + upi_id) as response:
                if response.status == 200:
                    try:
                        data = await response.json(content_type=None)
                    except (aiohttp.ContentTypeError, json.JSONDecodeError):
                        error_text = await response.text()
                        logger.error(f"UPI API returned non-JSON response: {error_text}")
                        return await msg.edit_text(f"❌ API Error: {escape_md(error_text[:200])}\\.", parse_mode="MarkdownV2")
                    
                    if (isinstance(data, dict) and data.get("error")) or not data or (isinstance(data, dict) and data.get("status") == "false"):
                        error_msg = ""
                        if isinstance(data, dict):
                            error_msg = data.get("error") or data.get("message", "No details found for the given UPI ID")
                        else:
                            error_msg = "No details found for the given UPI ID"
                            
                        await msg.edit_text(f"❌ {escape_md(error_msg)}", parse_mode="MarkdownV2")
                    else:
                        # Improved UPI result formatting by attempting to pull out key fields if available
                        result_msg = "🏦 *UPI Search Results*\n"
                        result_msg += "━━━━━━━━━━━━━━━━━━\n"

                        # Try to find common UPI fields
                        name = data.get('payeeName') or data.get('name')
                        bank = data.get('bankName') or upi_id.split('@')[-1]

                        if name:
                            result_msg += f"👤 *Payee Name:* `{escape_md(name)}`\n"
                        result_msg += f"🆔 *UPI ID:* `{escape_md(upi_id)}`\n"
                        result_msg += f"🏦 *Bank/PSP:* `{escape_md(bank)}`\n"

                        # Add remaining details as raw JSON block
                        result_msg += "\n*Raw Data Details:*\n"
                        json_result_str = json.dumps(data, indent=2, ensure_ascii=False)
                        
                        result_msg += (
                            f"```json\n"
                            f"{json_result_str}\n"
                            f"```"
                        )
                        
                        await msg.edit_text(result_msg, parse_mode="MarkdownV2")

                        if not users[user_id].get('unlimited'):
                            users[user_id]['limit'] -= 1
                            save_data()

                else:
                    await msg.edit_text(f"❌ Service unavailable (Status: {response.status})", parse_mode="MarkdownV2")

    except aiohttp.ClientError as e:
        logger.error(f"AIOHTTP Client Error in search_upi: {e}")
        await msg.edit_text("❌ *Network Error*: The connection to the UPI API failed or timed out\\. Please try again in a moment\\.", parse_mode="MarkdownV2")
        return
    except Exception as e:
        logger.error(f"Unexpected Error in search_upi: {e}")
        await msg.edit_text("❌ An unexpected error occurred while searching\\. Please try again later\\.", parse_mode="MarkdownV2")
        return



@check_channel_membership
async def redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    if not context.args:
        return await update.message.reply_text("🎟 Usage: `/redeem YOUR_CODE`")
    
    code = context.args[0].upper()
    if code in redeem_codes:
        code_data = redeem_codes.pop(code)
        users.setdefault(uid, {"limit": 0, "referrals": []})
        
        plan_details = ""
        if code_data.get('unlimited'):
            users[uid]['unlimited'] = True
            days = code_data['days']
            users[uid]['expiry'] = datetime.max if days == 0 else datetime.now() + timedelta(days=days)
            validity = "Lifetime" if days == 0 else f"{days} days"
            plan_details = f"Unlimited Plan ({validity})"
            users[uid]['limit'] = 0
        else:
            limit = code_data.get('limit', 0)
            users[uid]['unlimited'] = False
            users[uid]['limit'] = users[uid].get('limit', 0) + limit
            if not users[uid].get('unlimited'):
                users[uid]['expiry'] = datetime.max
            plan_details = f"{limit} Searches"
        
        save_data()
        await update.message.reply_text("🎉 Code accepted! Your plan has been updated. Use /profile to check.")
        
        try:
            user_info = update.message.from_user
            user_display = f"{user_info.first_name}"
            if user_info.username:
                user_display += f" (@{user_info.username})"
            
            d = users[uid]
            new_plan = "♾️ Unlimited" if d.get("unlimited") else f"🔢 {d.get('limit', 0)} Searches"
            new_expiry = "Lifetime" if d.get("expiry") == datetime.max else d.get("expiry").strftime('%d-%b-%Y')

            admin_message = (
                f"🎉 *Code Redeemed!*\n\n"
                f"👤 *User:* {escape_md(user_display)} (`{uid}`)\n"
                f"🎟️ *Code:* `{code}`\n"
                f"🎁 *Plan Added:* {escape_md(plan_details)}\n\n"
                f"📊 *User's New Status:*\n"
                f"  - Plan: {escape_md(new_plan)}\n"
                f"  - Valid Till: {escape_md(new_expiry)}"
            )
            
            for admin_id in ADMIN_ID:
                await context.bot.send_message(chat_id=admin_id, text=admin_message, parse_mode="MarkdownV2")
        except Exception as e:
            logger.error(f"Failed to send redeem notification to admins: {e}")

    else:
        await update.message.reply_text("❌ Invalid or expired redeem code.")

def main():
    """Starts the bot."""
    load_data()
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    text_only_filter = filters.TEXT & ~filters.COMMAND

    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(panel_actions, pattern="^gen_limit_start$"),
            CallbackQueryHandler(panel_actions, pattern="^gen_unlimited_start$"),
            CallbackQueryHandler(panel_actions, pattern="^revoke_start$"),
            CallbackQueryHandler(panel_actions, pattern="^search_user_start$"),
            CallbackQueryHandler(panel_actions, pattern="^edit_user_start$"),
            CallbackQueryHandler(panel_actions, pattern="^broadcast_start$"),
            CallbackQueryHandler(api_status_handler, pattern="^api_status_menu_entry$"),
        ],
        states={
            GEN_LIMIT: [MessageHandler(text_only_filter, gen_limit_step)],
            GEN_DAYS: [MessageHandler(text_only_filter, gen_days_step)],
            REVOKE_USER: [MessageHandler(text_only_filter, revoke_user_step)],
            REVOKE_CONFIRM: [CallbackQueryHandler(revoke_confirm_step)],
            SEARCH_USER: [MessageHandler(text_only_filter, search_user_step)],
            EDIT_USER_ID: [MessageHandler(text_only_filter, edit_user_id_step)],
            EDIT_USER_ACTION: [
                CallbackQueryHandler(edit_user_action_step, pattern="^edit_add_limit$"),
                CallbackQueryHandler(edit_user_action_step, pattern="^edit_set_expiry$"),
                CallbackQueryHandler(edit_user_action_step, pattern="^edit_make_unlimited$"),
                CallbackQueryHandler(edit_user_action_step, pattern="^edit_ban_user$"),
                CallbackQueryHandler(edit_user_action_step, pattern="^edit_unban_user$"),
                CallbackQueryHandler(edit_user_action_step, pattern="^edit_cancel$"),
            ],
            EDIT_USER_VALUE: [MessageHandler(text_only_filter, edit_user_value_step)],
            BROADCAST_MESSAGE: [MessageHandler(text_only_filter, broadcast_message_step)],
            BROADCAST_CONFIRM: [CallbackQueryHandler(broadcast_confirm_step)],
            API_STATUS_MENU: [
                CallbackQueryHandler(api_status_handler, pattern="^toggle_api_"),
                CallbackQueryHandler(back_to_panel_handler, pattern="^back_to_panel$")
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_user=True,
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("profile", profile))
    app.add_handler(CommandHandler("about", about))
    app.add_handler(CommandHandler("redeem", redeem))
    app.add_handler(CommandHandler("panel", panel))
    app.add_handler(CommandHandler("buy", buy))
    app.add_handler(CommandHandler("activate", activate))
    app.add_handler(CommandHandler("referral", referral))
    
    app.add_handler(CommandHandler("num", search_num))
    app.add_handler(CommandHandler("vehicle", search_vehicle))
    app.add_handler(CommandHandler("aadhar", search_aadhar))
    app.add_handler(CommandHandler("upi", search_upi))
    
    app.add_handler(conv_handler)
    
    app.add_handler(CallbackQueryHandler(panel_actions, pattern="^gen$"))
    app.add_handler(CallbackQueryHandler(panel_actions, pattern="^users_page_"))
    app.add_handler(CallbackQueryHandler(panel_actions, pattern="^referrers_page_"))
    app.add_handler(CallbackQueryHandler(panel_actions, pattern="^back_to_panel$"))
    
    app.add_handler(CallbackQueryHandler(button_actions))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    
    print("✅ Bot started...")
    app.run_polling()

if __name__ == "__main__":
    main()