import logging
import asyncio
from datetime import datetime
import pytz
import pandas as pd
import base64
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    PollAnswerHandler,
    ContextTypes,
    CallbackContext,
    Application # Import Application explicitly if needed for type hints elsewhere, though Builder usually suffices
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import nest_asyncio
from google.oauth2.service_account import Credentials
import gspread
import json
import os # Import os to potentially get port if needed for webhook (though we are avoiding webhook)

nest_asyncio.apply()

# === Configuration ===
BOT_TOKEN = "7897221989:AAHZoD6r03Qj21v4za2Zha3XFwW5o5Hw4h8" # Consider using environment variables for tokens
GROUP_CHAT_ID = -4607914574
SCHEDULE_CSV = "ipl_schedule.csv"
PREDICTIONS_SHEET_ID = "1iCENqo8p3o3twurO7-CSs490C1xrhxK7cNIsUJ4ThBA"
POLL_MAP_SHEET_ID = "1LogmznPifIPt7GQQPQ7UndHOup4Eo55ThraIUMeH2uE"
# Decode credentials - ensure this works correctly in your environment
try:
    creds_json_decoded = base64.b64decode(creds_json).decode("utf-8")
    creds_dict = json.loads(creds_json_decoded)
except Exception as e:
    logger.error(f"Fatal error decoding/parsing credentials JSON: {e}")
    # Decide how to handle this - exit or raise? For now, log and raise.
    raise ValueError("Could not load credentials") from e


# === Logging Setup ===
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# Reduce default httpx logging noise if desired
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# === Google Sheets Interaction ===
def authorize_gspread():
    """Authorizes Google Sheets API client."""
    try:
        # Use the pre-parsed dictionary
        creds = Credentials.from_service_account_info(
            creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Error authorizing Google Sheets: {e}")
        raise

def get_sheet(gc, sheet_id):
    """Retrieves a Google Sheet by its ID."""
    try:
        return gc.open_by_key(sheet_id).sheet1
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error accessing sheet {sheet_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Generic error accessing sheet {sheet_id}: {e}")
        raise

def get_predictions_df(sheet):
    """Retrieves predictions data from the Google Sheet."""
    try:
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error getting predictions data: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        logger.error(f"Error getting predictions data: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

def save_predictions_df(sheet, df):
    """Saves predictions data to the Google Sheet."""
    try:
        sheet.clear()
        # Ensure all data is serializable (convert complex types if necessary)
        data_to_write = [df.columns.values.tolist()] + df.astype(str).values.tolist()
        sheet.update(data_to_write)
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error saving predictions data: {e}")
        raise
    except Exception as e:
        logger.error(f"Error saving predictions data: {e}")
        raise

def get_poll_map(sheet):
    """Retrieves the poll ID to match number mapping from the Google Sheet."""
    try:
        rows = sheet.get_all_records()
        return {
            str(row["poll_id"]): int(row["MatchNo"])
            for row in rows
            if "poll_id" in row and row.get("poll_id") and "MatchNo" in row and row.get("MatchNo") # Added checks for non-empty values
        }
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error getting poll map: {e}")
        return {} # Return empty dict on error
    except Exception as e:
        logger.error(f"Error getting poll map: {e}")
        return {} # Return empty dict on error

def save_poll_id(sheet, poll_id, match_no):
    """Saves a poll ID and its corresponding match number to the Google Sheet."""
    try:
        sheet.append_row([str(poll_id), match_no])
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error saving poll ID: {e}")
        raise
    except Exception as e:
        logger.error(f"Error saving poll ID: {e}")
        raise


# === Helper: Load Schedule ===
def load_schedule_mapping(csv_file):
    """Loads the match schedule from a CSV file."""
    schedule_mapping = {}
    try:
        df = pd.read_csv(csv_file)
        df.columns = df.columns.str.strip() # Clean column names

        required_cols = ["MatchNo", "Date", "Day", "Teams", "MatchTime", "Venue", "PollStartTime", "PollEndTime"]
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV missing one or more required columns: {required_cols}")

        for index, row in df.iterrows():
            try:
                # Attempt conversion and strip, handle potential errors per row
                match_no = int(str(row["MatchNo"]).strip())
                schedule_mapping[match_no] = {
                    "Date": str(row["Date"]).strip(),
                    "Day": str(row["Day"]).strip(),
                    "Teams": str(row["Teams"]).strip(),
                    "MatchTime": str(row["MatchTime"]).strip(),
                    "Venue": str(row["Venue"]).strip(),
                    "PollStartTime": str(row["PollStartTime"]).strip(),
                    "PollEndTime": str(row["PollEndTime"]).strip(),
                }
                # Validate team format early
                if " vs " not in schedule_mapping[match_no]["Teams"]:
                    logger.warning(f"Match {match_no}: Teams format '{schedule_mapping[match_no]['Teams']}' might be incorrect (expected 'Team A vs Team B').")
                # Validate date/time formats early
                try:
                     datetime.strptime(schedule_mapping[match_no]["Date"], "%d %b %Y")
                     datetime.strptime(schedule_mapping[match_no]["PollStartTime"], "%I:%M %p")
                except ValueError as time_e:
                    logger.error(f"Invalid date/time format in CSV for Match {match_no}: {time_e}. Row: {row.to_dict()}")
                    # Decide whether to skip this match or raise - skipping for now
                    del schedule_mapping[match_no] # Remove invalid entry
                    continue

            except (ValueError, TypeError, KeyError) as e:
                logger.error(f"Error processing CSV row {index+2}: {row.to_dict()}. Exception: {e}")
                # Decide whether to skip or raise. Skipping for now.
                continue # Skip this row

    except FileNotFoundError:
        logger.error(f"Fatal Error: Schedule CSV file not found at {csv_file}")
        raise
    except ValueError as e: # Catch column errors
         logger.error(f"Fatal Error loading schedule from CSV: {e}")
         raise
    except Exception as e:
        logger.error(f"Fatal Error loading schedule from CSV: {e}")
        raise
    logger.info(f"Successfully loaded {len(schedule_mapping)} matches from schedule.")
    return schedule_mapping


# === Bot Commands and Logic ===
async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the chat ID to the group."""
    if not update.effective_chat:
        logger.warning("get_chat_id called without effective_chat.")
        return
    chat_id_to_send = update.effective_chat.id
    message_text = f"Chat ID: `{chat_id_to_send}`"
    try:
        await context.bot.send_message(
            chat_id=chat_id_to_send,
            text=message_text,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error in get_chat_id sending message: {e}")
        # Optionally try sending to the main group if sending to the user fails
        try:
             await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=f"Error getting chat ID for a user: {e}")
        except Exception as inner_e:
             logger.error(f"Error sending error message to group chat: {inner_e}")


async def scheduled_poll(context: CallbackContext): # APScheduler passes context directly
    """Posts a poll for a scheduled match. Modified for APScheduler."""
    job_data = context.job.kwargs # Get data passed via scheduler args
    bot = context.bot
    match_no = job_data['match_no']
    match_info = job_data['match_info']

    logger.info(f"Running scheduled poll job for Match {match_no}")

    try:
        match_name = match_info["Teams"]
        question = (
            f"Match {match_no}: {match_name}\n" f"Venue: {match_info['Venue']}\n" "Who will win?"
        )
        options = match_name.split(" vs ")
        if len(options) != 2:
             logger.error(f"Cannot create poll for Match {match_no}. Invalid team format: {match_name}")
             return

        poll_message = await bot.send_poll(
            GROUP_CHAT_ID, question, options, is_anonymous=False # Usually prediction polls are not anonymous
            # allow_multiple_answers=False # Default is False
        )
        logger.info(f"Poll sent for Match {match_no}. Poll ID: {poll_message.poll.id}")

        # Store poll info - Access bot_data via context
        # It's generally better to retrieve sheets fresh if possible, or handle potential stale references
        try:
            gc = context.bot_data["gc"]
            poll_map_sheet = get_sheet(gc, POLL_MAP_SHEET_ID) # Re-fetch sheet object potentially
            save_poll_id(poll_map_sheet, poll_message.poll.id, match_no)
            logger.info(f"Poll ID {poll_message.poll.id} mapped to Match {match_no} in Google Sheet.")
        except Exception as sheet_e:
             logger.error(f"Error saving poll ID {poll_message.poll.id} to Google Sheet for Match {match_no}: {sheet_e}")
             # Consider sending an alert message?

    except Exception as e:
        logger.error(f"Error in scheduled_poll for Match {match_no}: {e}")
        try:
            await bot.send_message(GROUP_CHAT_ID, text=f"Error creating poll for Match {match_no}: {e}")
        except Exception as inner_e:
            logger.error(f"Error sending error message to group chat: {inner_e}")

async def startpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts a poll manually."""
    if not update.message or not update.effective_chat:
         logger.warning("startpoll called without message or effective_chat.")
         return

    try:
        if not context.args:
            await update.message.reply_text("Usage: /startpoll <match_no>")
            return

        try:
             match_no = int(context.args[0])
        except ValueError:
             await update.message.reply_text("Invalid match number. Please provide an integer.")
             return

        # Access schedule_mapping safely (assuming it's loaded globally or in bot_data)
        if 'schedule_mapping' not in globals():
             logger.error("Schedule mapping not loaded.")
             await update.message.reply_text("Error: Schedule data is not available.")
             return
        match_info = schedule_mapping.get(match_no)

        if not match_info:
            await update.message.reply_text(f"Match number {match_no} not found in the schedule.")
            return

        match_name = match_info["Teams"]
        question = (
            f"Match {match_no}: {match_name}\n" f"Venue: {match_info['Venue']}\n" "Who will win?"
        )
        options = match_name.split(" vs ")
        if len(options) != 2:
             logger.error(f"Cannot create manual poll for Match {match_no}. Invalid team format: {match_name}")
             await update.message.reply_text(f"Error: Invalid team format for Match {match_no} ('{match_name}'). Cannot create poll.")
             return

        poll_message = await context.bot.send_poll(
            update.effective_chat.id, question, options, is_anonymous=False
        )
        logger.info(f"Manual poll sent for Match {match_no} by {update.effective_user.name if update.effective_user else 'Unknown'}. Poll ID: {poll_message.poll.id}")

        # Store poll info
        try:
            gc = context.bot_data["gc"]
            poll_map_sheet = get_sheet(gc, POLL_MAP_SHEET_ID) # Re-fetch sheet
            save_poll_id(poll_map_sheet, poll_message.poll.id, match_no)
            logger.info(f"Manual Poll ID {poll_message.poll.id} mapped to Match {match_no} in Google Sheet.")
        except Exception as sheet_e:
             logger.error(f"Error saving manual poll ID {poll_message.poll.id} to Google Sheet for Match {match_no}: {sheet_e}")
             # Consider sending an alert message?

    except Exception as e:
        logger.exception(f"Error in startpoll command: {e}") # Use exception to log traceback
        try:
            await context.bot.send_message(GROUP_CHAT_ID, text=f"Error processing /startpoll command: {e}")
        except Exception as inner_e:
            logger.error(f"Error sending error message to group chat: {inner_e}")

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a user's poll answer."""
    if not update.poll_answer:
        logger.warning("handle_poll_answer called without poll_answer.")
        return

    answer = update.poll_answer
    poll_id = answer.poll_id
    user = answer.user
    option_ids = answer.option_ids # This is a list, usually with one element for non-multi-choice polls

    if not user:
        logger.warning(f"Received poll answer for poll {poll_id} without user information. Skipping.")
        return

    logger.info(f"Received poll answer from {user.full_name} (ID: {user.id}) for poll {poll_id}. Options: {option_ids}")

    try:
        # Retrieve necessary data from bot_data or re-fetch
        gc = context.bot_data["gc"]
        # It's safer to re-fetch sheets or handle potential errors if they become invalid
        try:
            poll_map_sheet = get_sheet(gc, POLL_MAP_SHEET_ID)
            pred_sheet = get_sheet(gc, PREDICTIONS_SHEET_ID)
            poll_map = get_poll_map(poll_map_sheet) # Get fresh map
        except Exception as sheet_e:
            logger.error(f"Failed to get sheet data in handle_poll_answer: {sheet_e}")
            # Maybe notify user or group?
            return

        # Find match_no using the poll_map dictionary
        match_no = poll_map.get(str(poll_id))

        if match_no is None:
            logger.warning(f"No match found for poll_id: {poll_id} in the poll map. Maybe an old poll?")
            return

        # Access schedule_mapping safely
        if 'schedule_mapping' not in globals():
             logger.error(f"Schedule mapping not loaded while handling answer for poll {poll_id}.")
             return # Cannot proceed without schedule
        match_info = schedule_mapping.get(match_no)

        if not match_info:
            logger.warning(f"No match info found for match_no: {match_no} (from poll {poll_id})")
            return

        # Handle vote retraction (empty option_ids)
        if not option_ids:
            logger.info(f"{user.full_name} retracted vote for poll {poll_id} (Match {match_no}). Removing prediction.")
            try:
                df = get_predictions_df(pred_sheet)
                if df.empty:
                    logger.info("Prediction sheet is empty, nothing to remove.")
                    return

                # Define types for filtering
                df['MatchNo'] = pd.to_numeric(df['MatchNo'], errors='coerce')
                username = user.full_name # Or user.username if preferred and available

                row_mask = (df["MatchNo"] == match_no) & (df["Username"] == username)
                if row_mask.any():
                    df = df[~row_mask] # Remove rows matching the condition
                    save_predictions_df(pred_sheet, df)
                    logger.info(f"Removed prediction for {username} for Match {match_no}.")
                else:
                    logger.info(f"No existing prediction found for {username} for Match {match_no} to remove.")
            except Exception as e:
                logger.error(f"Error removing prediction for {username}, Match {match_no}: {e}")
            return # Stop processing after retraction

        # --- Process valid vote ---
        chosen_option_index = option_ids[0] # Assuming single choice poll
        options = match_info["Teams"].split(" vs ")
        if chosen_option_index >= len(options):
             logger.error(f"Invalid option index {chosen_option_index} for poll {poll_id}, Match {match_no}. Options: {options}")
             return

        chosen_team = options[chosen_option_index].strip()
        username = user.full_name # Or user.username

        try:
            df = get_predictions_df(pred_sheet)

            # Ensure columns exist, create if first vote ever
            expected_cols = ["MatchNo", "Match", "Username", "Prediction", "Correct"]
            if df.empty:
                df = pd.DataFrame(columns=expected_cols)
            else:
                # Add missing columns if necessary (e.g., if sheet was manually cleared)
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = None if col != "Correct" else 0 # Default Correct to 0

            # Ensure MatchNo is numeric for comparison
            df['MatchNo'] = pd.to_numeric(df['MatchNo'], errors='coerce')

            # Find if user already voted for this match
            row_mask = (df["MatchNo"] == match_no) & (df["Username"] == username)
            match_display_name = match_info["Teams"] # Use the full team string

            if df[row_mask].empty:
                # Add new prediction row
                new_row_dict = {
                    "MatchNo": match_no,
                    "Match": match_display_name,
                    "Username": username,
                    "Prediction": chosen_team,
                    "Correct": 0  # Initialize Correct column
                }
                # Ensure new row aligns with DataFrame columns
                new_row_df = pd.DataFrame([new_row_dict], columns=df.columns)
                df = pd.concat([df, new_row_df], ignore_index=True)
                logger.info(f"Recorded new vote: {username} voted {chosen_team} for Match {match_no}.")
            else:
                # Update existing prediction
                # Use .loc[row_mask, column_name] = value
                df.loc[row_mask, "Prediction"] = chosen_team
                df.loc[row_mask, "Correct"] = 0 # Reset correctness score on vote change
                logger.info(f"Updated vote: {username} changed vote to {chosen_team} for Match {match_no}.")

            # Convert 'Correct' back to string if needed by Sheets, though saving handles types
            df['Correct'] = df['Correct'].fillna(0).astype(int) # Ensure Correct is integer

            save_predictions_df(pred_sheet, df)

        except Exception as e:
            logger.error(f"Error processing vote for {username}, Match {match_no}: {e}")
            # Maybe send an error message?

    except Exception as e:
        logger.exception(f"General error in handle_poll_answer for poll {poll_id}: {e}")
        try:
            await context.bot.send_message(GROUP_CHAT_ID, text=f"Error processing a poll answer: {e}")
        except Exception as inner_e:
            logger.error(f"Error sending error message to group chat: {inner_e}")


async def score_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scores a match and updates predictions."""
    if not update.message or not update.effective_chat:
         logger.warning("score_match called without message or effective_chat.")
         return

    try:
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /score <match_no> <Winning Team Name>")
            return

        try:
            match_no = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid match number. Please provide an integer.")
            return

        # Join all remaining args to allow for team names with spaces
        winner = " ".join(context.args[1:]).strip()
        if not winner:
             await update.message.reply_text("Please provide the winning team name.")
             return

        logger.info(f"Scoring Match {match_no}. Declared winner: {winner}. Initiated by {update.effective_user.name if update.effective_user else 'Unknown'}")

        # Access schedule_mapping to validate winner name against teams
        if 'schedule_mapping' not in globals():
            logger.error("Schedule mapping not loaded during scoring.")
            await update.message.reply_text("Error: Schedule data is not available for validation.")
            # Optionally proceed without validation or return
            # return
            match_info = None # Indicate validation is not possible
        else:
             match_info = schedule_mapping.get(match_no)

        if not match_info:
             await update.message.reply_text(f"Match number {match_no} not found in the schedule. Cannot score.")
             return

        # Validate winner name (case-insensitive check against the two teams)
        teams = [team.strip() for team in match_info["Teams"].split(" vs ")]
        if winner.lower() not in [team.lower() for team in teams]:
            await update.message.reply_text(f"Invalid winner '{winner}'. For Match {match_no}, expected one of: {teams[0]}, {teams[1]}")
            return
        # Standardize winner name to match the case in the schedule/predictions
        winner = teams[0] if winner.lower() == teams[0].lower() else teams[1]

        try:
            gc = context.bot_data["gc"]
            pred_sheet = get_sheet(gc, PREDICTIONS_SHEET_ID)
            df = get_predictions_df(pred_sheet)

            if df.empty:
                 await update.message.reply_text(f"No predictions found for Match {match_no}. Nothing to score.")
                 return

            # Ensure MatchNo is numeric and Correct exists
            df['MatchNo'] = pd.to_numeric(df['MatchNo'], errors='coerce')
            if 'Correct' not in df.columns:
                df['Correct'] = 0
            df['Correct'] = pd.to_numeric(df['Correct'], errors='coerce').fillna(0) # Ensure numeric for calculation

            # Filter for the specific match
            match_mask = df["MatchNo"] == match_no

            if not match_mask.any():
                 await update.message.reply_text(f"No predictions found specifically for Match {match_no} in the sheet.")
                 return

            # Update 'Correct' column: 1 if Prediction matches winner, 0 otherwise
            # Apply only to rows matching the match_no
            df.loc[match_mask, "Correct"] = df.loc[match_mask, "Prediction"].apply(
                lambda prediction: 1 if str(prediction).strip().lower() == winner.lower() else 0
            )

            # Fill NaN in 'Correct' just in case, ensure int type
            df['Correct'] = df['Correct'].fillna(0).astype(int)

            save_predictions_df(pred_sheet, df)
            logger.info(f"Successfully updated scores for Match {match_no}. Winner: {winner}")
            await update.message.reply_text(
                f"Score updated for Match {match_no}. Winner: {winner}. Predictions marked."
            )

        except Exception as e:
            logger.error(f"Error accessing sheet or saving scores for Match {match_no}: {e}")
            await update.message.reply_text(f"An error occurred while updating scores for Match {match_no}.")

    except Exception as e:
        logger.exception(f"Error in score_match command: {e}")
        try:
            await context.bot.send_message(GROUP_CHAT_ID, text=f"Error processing /score command: {e}")
        except Exception as inner_e:
            logger.error(f"Error sending error message to group chat: {inner_e}")


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the leaderboard."""
    if not update.message:
         logger.warning("leaderboard called without message.")
         return
    try:
        gc = context.bot_data["gc"]
        pred_sheet = get_sheet(gc, PREDICTIONS_SHEET_ID)
        df = get_predictions_df(pred_sheet)

        if df.empty:
            await update.message.reply_text("No predictions found yet to generate a leaderboard.")
            return

        # Ensure Correct column exists and is numeric
        if 'Correct' not in df.columns:
             await update.message.reply_text("Leaderboard cannot be generated: 'Correct' column missing.")
             logger.warning("Leaderboard generation failed: Missing 'Correct' column.")
             return
        df["Correct"] = pd.to_numeric(df["Correct"], errors="coerce").fillna(0).astype(int)

        # Ensure Username column exists
        if 'Username' not in df.columns:
            await update.message.reply_text("Leaderboard cannot be generated: 'Username' column missing.")
            logger.warning("Leaderboard generation failed: Missing 'Username' column.")
            return
        df = df.dropna(subset=['Username']) # Remove rows with no username

        # Calculate leaderboard
        lb = (
            df.groupby("Username")["Correct"]
            .sum()
            .reset_index()
            .sort_values("Correct", ascending=False)
        )

        if lb.empty:
             await update.message.reply_text("No scores recorded yet for the leaderboard.")
             return

        msg = "🏆 *Leaderboard* 🏆\n\n"
        rank = 1
        for index, row in lb.iterrows():
             # Handle potential non-string usernames just in case
            username = str(row['Username'])
            score = int(row['Correct'])
            # Add emoji for top ranks
            rank_emoji = ""
            if rank == 1: rank_emoji = "🥇 "
            elif rank == 2: rank_emoji = "🥈 "
            elif rank == 3: rank_emoji = "🥉 "

            msg += f"{rank_emoji}{rank}. {username}: {score} points\n"
            rank += 1

        await update.message.reply_text(msg, parse_mode="Markdown")

    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error generating leaderboard: {e}")
        await update.message.reply_text("Error accessing prediction data for leaderboard.")
    except Exception as e:
        logger.exception(f"Error in leaderboard command: {e}")
        await update.message.reply_text("An error occurred while generating the leaderboard.")
        try:
            await context.bot.send_message(GROUP_CHAT_ID, text=f"Error generating /leaderboard: {e}")
        except Exception as inner_e:
            logger.error(f"Error sending error message to group chat: {inner_e}")

async def error_handler(update: object, context: CallbackContext):
    """Logs errors and sends a message to the admin/group."""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

    # Send error message to the designated group chat
    try:
        # Try to get basic info if update is available
        update_str = update if isinstance(update, str) else str(update) # Basic string representation
        message = (
            f"An exception occurred in the bot:\n"
            f"<pre>{context.error}</pre>\n\n"
            f"Update that caused the error:\n"
            f"<pre>{update_str[:1000]}</pre>" # Limit update length
        )
        await context.bot.send_message(
            chat_id=GROUP_CHAT_ID, text=message, parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Exception in error_handler itself while sending message: {e}")


async def check_and_delete_webhook(bot, retries=5, delay=15): # Slightly reduced default delay
    """Checks for and deletes any active webhooks with retries. Returns True if webhook is confirmed gone, False otherwise."""
    logger.info("Checking webhook status...")
    for attempt in range(retries):
        try:
            webhook_info = await bot.get_webhook_info()
            if webhook_info and webhook_info.url: # Check webhook_info is not None
                logger.warning(
                    f"Webhook found with URL: {webhook_info.url}. Attempting deletion... (Attempt {attempt + 1}/{retries})"
                )
                delete_success = await bot.delete_webhook(drop_pending_updates=True)
                if delete_success:
                    logger.info("Webhook successfully deleted.")
                    # Verify deletion
                    await asyncio.sleep(2) # Short pause before verifying
                    webhook_info_after = await bot.get_webhook_info()
                    if not (webhook_info_after and webhook_info_after.url):
                         logger.info("Webhook deletion confirmed.")
                         return True
                    else:
                         logger.warning("Webhook deletion command succeeded, but webhook info still shows a URL. Retrying check...")
                else:
                     logger.error(f"bot.delete_webhook call returned False (Attempt {attempt + 1}/{retries}).")

            else:
                logger.info("No active webhook found.")
                return True # No webhook to delete, success
        except Exception as e:
            logger.error(
                f"Error during webhook check/delete: {e} (Attempt {attempt + 1}/{retries})"
            )

        if attempt < retries - 1:
            logger.info(f"Waiting {delay} seconds before retrying webhook check/delete...")
            await asyncio.sleep(delay)

    logger.error("Failed to confirm webhook deletion after multiple retries.")
    return False # Failed to delete/confirm deletion


# --- Global schedule mapping ---
schedule_mapping = {}

async def main():
    """Main function to initialize and start the bot."""
    global schedule_mapping # Allow modification by main

    # Load schedule first, as it's needed early
    try:
        schedule_mapping = load_schedule_mapping(SCHEDULE_CSV)
        if not schedule_mapping:
             logger.error("Schedule mapping is empty after loading. Check CSV and logs. Exiting.")
             return # Cannot proceed without a schedule
    except Exception as e:
        logger.error(f"Failed to load schedule mapping: {e}. Bot cannot start.")
        return # Stop if schedule fails

    # Initialize Google Sheets connection
    try:
        gc = authorize_gspread()
    except Exception as e:
        logger.error(f"Failed to authorize Google Sheets: {e}. Bot cannot start.")
        return # Stop if Gsheets auth fails

    # Build the application
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # IMPORTANT: Delete webhook BEFORE initializing handlers or starting polling
    webhook_deleted = await check_and_delete_webhook(application.bot)
    if not webhook_deleted:
        logger.error("Failed to ensure webhook was deleted. Bot will not start polling to avoid conflicts.")
        # You might want to raise an error here or exit forcefully depending on desired behavior
        return # Stop the bot

    # Store gspread client and fetch initial sheets (handle potential errors)
    application.bot_data["gc"] = gc
    try:
         # Store sheet IDs, let handlers fetch sheets as needed? Or fetch here.
         # Fetching here risks stale objects if connection drops, but simpler.
         # Fetching in handlers adds slight overhead per request. Let's fetch here for now.
        application.bot_data["pred_sheet_id"] = PREDICTIONS_SHEET_ID # Store IDs
        application.bot_data["poll_map_sheet_id"] = POLL_MAP_SHEET_ID
        logger.info("Google Sheets authorized. Sheet IDs stored in bot_data.")
        # Optionally fetch initial sheet objects here and store them if preferred
        # application.bot_data["pred_sheet"] = get_sheet(gc, PREDICTIONS_SHEET_ID)
        # application.bot_data["poll_map_sheet"] = get_sheet(gc, POLL_MAP_SHEET_ID)
    except Exception as e:
        logger.error(f"Error getting initial sheet objects: {e}. Bot may have issues with sheet operations.")
        # Decide whether to proceed or stop

    # Add handlers
    application.add_handler(CommandHandler("startpoll", startpoll))
    application.add_handler(CommandHandler("score", score_match))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("getchatid", get_chat_id))
    application.add_handler(PollAnswerHandler(handle_poll_answer))

    # Add error handler - IMPORTANT
    application.add_error_handler(error_handler)

    # Create and start the scheduler
    scheduler = AsyncIOScheduler(timezone=pytz.timezone("Asia/Kolkata")) # Set timezone directly
    ist = pytz.timezone("Asia/Kolkata")
    now_utc = datetime.now(pytz.utc)
    jobs_scheduled = 0

    # Schedule polls
    for match_no, match_info in schedule_mapping.items():
        try:
            # Use consistent date parsing
            match_date_str = match_info["Date"]
            poll_time_str = match_info["PollStartTime"]
            match_date = datetime.strptime(match_date_str, "%d %b %Y").date() # Get date part only
            poll_time = datetime.strptime(poll_time_str, "%I:%M %p").time() # Get time part only

            # Combine date and time, localize with IST, then convert to UTC
            poll_dt_naive = datetime.combine(match_date, poll_time)
            poll_dt_ist = ist.localize(poll_dt_naive)
            poll_dt_utc = poll_dt_ist.astimezone(pytz.utc)

        except (ValueError, KeyError) as e:
            logger.error(f"Invalid date/time format or missing key for Match {match_no}: {e}. Skipping schedule.")
            continue # Skip scheduling this match

        if poll_dt_utc > now_utc:
            try:
                # Pass data needed by the job function via kwargs
                job_kwargs = {'match_no': match_no, 'match_info': match_info}
                scheduler.add_job(
                    scheduled_poll,
                    "date",
                    run_date=poll_dt_utc, # Use UTC datetime for APScheduler
                    kwargs=job_kwargs, # Pass data using kwargs
                    id=f"poll_{match_no}",
                    misfire_grace_time=300, # Allow 5 mins delay if bot restarts
                    replace_existing=True # Replace if job with same id exists
                )
                jobs_scheduled += 1
                # logger.info(f"Scheduled poll for Match {match_no} at {poll_dt_utc} (UTC)") # Log scheduling time
            except Exception as sched_e:
                 logger.error(f"Error scheduling job for Match {match_no}: {sched_e}")
        # else: # Optionally log matches whose poll time has passed
            # logger.info(f"Poll start time for Match {match_no} ({poll_dt_utc} UTC) has already passed. Skipping schedule.")


    if jobs_scheduled > 0:
        scheduler.start()
        logger.info(f"Scheduler started with {jobs_scheduled} poll jobs.")
    else:
        logger.info("Scheduler started, but no future poll jobs were scheduled.")


    # Start polling
    logger.info("Starting bot polling...")
    try:
        # Use run_polling directly. No need for gather if periodic check is removed.
        await application.run_polling(allowed_updates=Update.ALL_TYPES) # Specify updates if needed
    except Exception as e:
        logger.exception(f"Critical error during application.run_polling: {e}") # Log full traceback
    finally:
        # Cleanup happens automatically with run_polling context manager usually
        # If scheduler is running, shut it down gracefully
        if scheduler.running:
            scheduler.shutdown()
        logger.info("Bot application stopped.")


if __name__ == "__main__":
    try:
      asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped manually.")
    except Exception as e:
        # This catches errors during initial setup in main() before run_polling
        logger.exception(f"Critical error during bot startup or shutdown: {e}")
