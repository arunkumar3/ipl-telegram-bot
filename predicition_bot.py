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
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import nest_asyncio
from google.oauth2.service_account import Credentials
import gspread
import json

nest_asyncio.apply()

# === Configuration ===
BOT_TOKEN = "7897221989:AAHZoD6r03Qj21v4za2Zha3XFwW5o5Hw4h8"
GROUP_CHAT_ID = -4607914574
SCHEDULE_CSV = "ipl_schedule.csv"
PREDICTIONS_SHEET_ID = "1iCENqo8p3o3twurO7-CSs490C1xrhxK7cNIsUJ4ThBA"
POLL_MAP_SHEET_ID = "1LogmznPifIPt7GQQPQ7UndHOup4Eo55ThraIUMeH2uE"
creds_json = """
eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsICJwcm9qZWN0X2lkIjogImlwbC1wcmVkaWNpdGlvbnMiLCAicHJpdmF0ZV9rZXlfaWQiOiAiOGIxZDgwMDQzOTg2YjUwZjYyOGQzMzFiYzdiMWE0OWYxYTUzMTBlNCIsICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2QUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktZd2dnU2lBZ0VBQW9JQkFRQ3NyOTgzUkZyRlpUeVZcbmExcnpYUjk1R1VKb2xDcVZzcDBiajRzTDVxdXVOdmpLSjJ3K296YjNWRHRpQ0lPV1pnaytFOUJwZE55SWk0bnFcbnVVUXlmZWxLT1FsNEw5OWNCZWxaNktTQkZMTC9uYnBaS21qRzBLRDUxTCtSVjdGSVQ1Yzc4dGdSNk54L095SGNcbjA5aEtqeUJuMzhMTm54VkZiQ2Z2cU1BVWFmbVRoZjRJNGE3UkFYTmk4ZjhjZVBrWkJablk2YzZkNXFGdTMyUFdcbmJSZEZaMFRFeGhXRFFjMDJiSzhLN0g4dm9pRVZYTFpLWjJNNnkxN3F1NUZtUFBtZHVubWpYbXNnN3VUSFNpT3lcbmhrd0R3SlI3Mlo2d2ZmaGVJR3ZpbHlWb3hERmx3bW15T1ViVTVuZnlrbmtLN2xNZHJZbjFmM0F1S2pTelB1aElcblc0VVQzZFlYQWdNQkFBRUNnZ0VBSlZrTUw4bkt6L0pyUGUyd0IvNVY5anp1VGV2dG9kNjFkK1o5cmg4L2RqaFJcbmFuZElRK3ZNMFlVWUtzV29uL2lGZXpXUjE1ejhyVk53aXFGekRIQ0s2aENYNmJTQTNFZ3pCY3o0OXluZzVNUGFcbkw3cXFXb1Y0cTAvRjlzcytmbU1vVkVEYlZsUkVqQWZmOVFDa1FNdmZ1RmQrckRZQnhiZjBrekt1Q0R3N1RCcE1cbjR5MDB0VzlHeGEybDF3YkQ1ZGlSM3I5OFNhUzRNUkVHQVBXd2FWSEszVGttRnJ3c2lwbGFUeFRCSGZNTlpEMUVcbmFQeGdYNk1qendzbDRGRDNWZ1JhbWlMKzdxd1RrNUdvZHFzNCtGdFVnRVNuQXNWVWRBb2pLK1ZyWDRsZER6UmNcbmtXeVBYQ3BUK3orL2pzUzd4c3ZsTzM1L2lQTEIydjk5eWJ4M21xLzFvUUtCZ1FEdzE5TXhuVjNyY0NnZTZkNHRcbkwxZ1F6cFhEM2x0V0F1K1VPZFhWYlVpZkRWMVowOXhzc1NMdVBuU1ZEOU0rR3NoTWZSRkRNZWwvUG1xK3lHdkFcbmpYK2JxMEFoU3pJVnkvSGJhU241YUxzMkd1N2t3SWpqa0x5ZzhVR3pTNmJzK2VSTXBLSTJIL0lXSjRWa3lLUnFcblVaYm9yZit3TVIwa0YyY282UUU5V3hQZS93S0JnUUMzamYrQUZIY2gvbGNXVlVWMVkzQTBXRy9OTE5VaHhyb0tcbm1aa2xuVVhEd0lNQnNVNUZqekFFZkFFajZGSXE2VlpoRjQzTksyTll3SmxwaTcybW11amszQmd6Vml1OGRSZG5cbjQwakFOWEJya2c4SzNMS3FnVmRoZ0NiQXFiQ1FZRzFoSEppL3RRaFJwM0J3QUMzbms5b0VGZFhpSzV1TnRJLzFcblBycTlOUmdnNlFLQmdDSDRqMVdFT09jb25zQWRoTFVpNUcwYWRvMTJJN1B5SGhEdVIzY2ZQd3NRTzRhY0Y0OU5cblBQd1YyeVBiWTVSeStxV3ZUbXdIOGtOOGJsb1Nzd0FwOVVIajJkdllXMnd2cENHcXA3MENSTVhRN3JsZFh2R2FcblRNRDJ4cW1mbGgvKzczRFFHQUZDYUVjdnMrVVBXQUdYR0k0aFhOdGhVaGJ4SmgvakhjV2x2eHZKQW9HQU9YWDJcbldmNE9IVklscVJRZ25sTDJ1U3hHTTVDcFY5MkNOL2RGZmdUeDVnbkorU21zT3hKTUVkdFA4QkcyUjBDc2pkQjFcbno1aVpqUnNkNjNDWGVpUmNhK2lLbXVlSzRZQTJSNHRiSnZDVHROa1FaSElhYkUzNU1NaVJXUmJGOHl4OGtUNEdcbmcxMEVzYXNkQTdMS3JBZ1k0OWFDRWo5Y2ZzdmJsUWFDSnVFTUlLRUNnWUFGelFEYTE3ZzhiRzl2MkNTWHlzTWxcbmNjY1d0WSt3T3VaQTZYem9jOWJKMHI4WkMyVzJkNXRCUHpqOHpXQm9ISWdpYitYemtDZ3F2M2pPaXRKQXkreXFcbktvb3hiamVZOE9uRFg3RmQyeS9JcVZXOHRTelBaNTRGMEFyWVFlMkdGekRwbHdHZnE0OERsZy9Cc3VyRHhQaURcbk8yOVlWZkd5WkZRb0ZTRjdsT2E0bkE9PVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwgImNsaWVudF9lbWFpbCI6ICJpcGwtcHJlZGljdGlvbnNAaXBsLXByZWRpY2l0aW9ucy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsICJjbGllbnRfaWQiOiAiMTAyOTk2OTMzMTQ0NTc1NTEwMjgzIiwgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwgInRva2VuX3VyaSI6ICJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlbiIsICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvaXBsLXByZWRpY3Rpb25zJTQwaXBsLXByZWRpY2l0aW9ucy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20ifQ==
"""  # Replace with your service account JSON

# === Logging Setup ===
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# === Google Sheets Interaction ===
def authorize_gspread():
    """Authorizes Google Sheets API client."""
    try:
        creds_dict = json.loads(base64.b64decode(creds_json).decode("utf-8"))
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
    except Exception as e:
        logger.error(f"Error accessing sheet {sheet_id}: {e}")
        raise



def get_predictions_df(sheet):
    """Retrieves predictions data from the Google Sheet."""
    try:
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error getting predictions data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error



def save_predictions_df(sheet, df):
    """Saves predictions data to the Google Sheet."""
    try:
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
    except Exception as e:
        logger.error(f"Error saving predictions data: {e}")
        raise



def get_poll_map(sheet):
    """Retrieves the poll ID to match number mapping from the Google Sheet."""
    try:
        rows = sheet.get_all_records()
        return {
            str(row["poll_id"]): int(row["match_no"])
            for row in rows
            if "poll_id" in row and "MatchNo" in row
        }
    except Exception as e:
        logger.error(f"Error getting poll map: {e}")
        return {}  # Return empty dict on error



def save_poll_id(sheet, poll_id, match_no):
    """Saves a poll ID and its corresponding match number to the Google Sheet."""
    try:
        sheet.append_row([str(poll_id), match_no])
    except Exception as e:
        logger.error(f"Error saving poll ID: {e}")
        raise


# === Helper: Load Schedule ===
def load_schedule_mapping(csv_file):
    """Loads the match schedule from a CSV file."""
    schedule_mapping = {}
    try:
        df = pd.read_csv(csv_file)
        df.columns = df.columns.str.strip()  # Clean column names

        for _, row in df.iterrows():
            try:
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
            except Exception as e:
                logger.error(f"Error processing row: {row}. Exception: {e}")
    except FileNotFoundError:
        logger.error(f"Error: File not found at {csv_file}")
        raise
    except Exception as e:
        logger.error(f"Error loading schedule from CSV: {e}")
        raise
    return schedule_mapping


# === Bot Commands and Logic ===
async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the chat ID to the group."""
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Chat ID: `{update.effective_chat.id}`",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error in get_chat_id: {e}")
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=f"Error: {e}")



async def scheduled_poll(bot, match_no, match_info):
    """Posts a poll for a scheduled match."""
    try:
        match_name = match_info["Teams"]
        question = (
            f"Match {match_no}: {match_name}\n" f"Venue: {match_info['Venue']}\n" "Who will win?"
        )
        options = match_name.split(" vs ")

        poll_message = await bot.send_poll(
            GROUP_CHAT_ID, question, options, False, None, False
        )

        # Store poll info
        bot_data = bot.get_context().bot_data
        bot_data[poll_message.poll.id] = {
            "match_no": match_no,
            "match_name": match_name,
            "options": options,
        }
        gc = bot_data["gc"]  # Get the authorized gspread client
        poll_map_sheet = bot_data["poll_map_sheet"]
        save_poll_id(poll_map_sheet, poll_message.poll.id, match_no)
        logger.info(f"Poll posted for match {match_no}: {match_name}")
    except Exception as e:
        logger.error(f"Error in scheduled_poll: {e}")
        await bot.send_message(GROUP_CHAT_ID, text=f"Error: {e}")



async def startpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts a poll manually."""
    try:
        if not context.args:
            await update.message.reply_text("Usage: /startpoll <match_no>")
            return
        match_no = int(context.args[0])
        match_info = schedule_mapping.get(match_no)  # Use the loaded schedule
        if not match_info:
            await update.message.reply_text("Invalid match number.")
            return

        question = (
            f"Match {match_no}: {match_info['Teams']}\n" f"Venue: {match_info['Venue']}\n" "Who will win?"
        )
        options = match_info["Teams"].split(" vs ")

        poll_message = await context.bot.send_poll(
            update.effective_chat.id, question, options, False, None, False
        )
        context.bot_data[poll_message.poll.id] = {
            "match_no": match_no,
            "match_name": match_info["Teams"],
            "options": options,
        }
        gc = context.bot_data["gc"]  # Get the authorized gspread client.
        poll_map_sheet = context.bot_data["poll_map_sheet"]
        save_poll_id(poll_map_sheet, poll_message.poll.id, match_no)
    except Exception as e:
        logger.error(f"Error in startpoll: {e}")
        await context.bot.send_message(GROUP_CHAT_ID, text=f"Error: {e}")



async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a user's poll answer."""
    try:
        answer = update.poll_answer
        poll_id = answer.poll_id
        user = answer.user
        option_ids = answer.option_ids

        bot_data = context.bot_data
        gc = bot_data["gc"]  # Get the authorized gspread client
        poll_map_sheet = bot_data["poll_map_sheet"]
        pred_sheet = bot_data["pred_sheet"]

        poll_map_df = pd.DataFrame(poll_map_sheet.get_all_records())
        poll_map_df["poll_id"] = poll_map_df["poll_id"].astype(
            str
        )  # Ensure poll_id is treated as string
        match_row = poll_map_df[poll_map_df["poll_id"] == str(poll_id)]

        if match_row.empty:
            logger.warning(f"No match found for poll_id: {poll_id}")
            return
        match_no = int(match_row.iloc[0]["MatchNo"])

        match_info = schedule_mapping.get(match_no)  # Access schedule_mapping
        if not match_info:
            logger.warning(f"No match info found for match_no: {match_no}")
            return

        if not option_ids:
            logger.warning(
                f"{user.full_name} submitted an empty vote for poll {poll_id}. Skipping."
            )
            return
        chosen_team = match_info["Teams"].split(" vs ")[option_ids[0]]
        username = user.full_name

        df = get_predictions_df(pred_sheet)
        if df.empty:
            df = pd.DataFrame(
                columns=["MatchNo", "Match", "Username", "Prediction", "Correct"]
            )
        row_mask = (df["MatchNo"] == int(match_no)) & (df["Username"] == username)

        if df[row_mask].empty:
            new_row = pd.DataFrame(
                [[int(match_no), match_info["Teams"], username, chosen_team, ""]],
                columns=["MatchNo", "Match", "Username", "Prediction", "Correct"],
            )
            df = pd.concat([df, new_row], ignore_index=True)
        else:
            df.loc[row_mask, "Prediction"] = chosen_team
            df.loc[row_mask, "Correct"] = ""
        save_predictions_df(pred_sheet, df)
        logger.info(f"{username} voted {chosen_team} for match {match_no}.")
    except Exception as e:
        logger.error(f"Error in handle_poll_answer: {e}")
        await context.bot.send_message(GROUP_CHAT_ID, text=f"Error: {e}")



async def score_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scores a match and updates predictions."""
    try:
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /score <match_no> <winner>")
            return
        match_no = int(context.args[0])
        winner = context.args[1]

        bot_data = context.bot_data
        gc = bot_data["gc"]  # Get the authorized gspread client
        pred_sheet = bot_data["pred_sheet"]

        df = get_predictions_df(pred_sheet)
        df.loc[df["MatchNo"] == match_no, "Correct"] = df[
            df["MatchNo"] == match_no
        ]["Prediction"].apply(lambda x: 1 if x == winner else 0)
        save_predictions_df(pred_sheet, df)
        await update.message.reply_text(
            f"Score updated for match {match_no}. Winner: {winner}"
        )
    except Exception as e:
        logger.error(f"Error in score_match: {e}")
        await context.bot.send_message(GROUP_CHAT_ID, text=f"Error: {e}")



async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the leaderboard."""
    try:
        bot_data = context.bot_data
        gc = bot_data["gc"]  # Get the authorized gspread client
        pred_sheet = bot_data["pred_sheet"]
        df = get_predictions_df(pred_sheet)
        if df.empty:
            await update.message.reply_text("No predictions found.")
            return
        df["Correct"] = pd.to_numeric(df["Correct"], errors="coerce").fillna(0).astype(
            int
        )
        lb = (
            df.groupby("Username")["Correct"]
            .sum()
            .reset_index()
            .sort_values("Correct", ascending=False)
        )
        msg = "🏆 *Leaderboard* 🏆\n\n"
        for _, row in lb.iterrows():
            msg += f"{row['Username']}: {row['Correct']} points\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in leaderboard: {e}")
        await context.bot.send_message(GROUP_CHAT_ID, text=f"Error: {e}")



async def error_handler(update: Update, context: CallbackContext):
    """Handles errors during bot operation."""
    logger.error(f"Update {update} caused error {context.error}")
    try:
        await context.bot.send_message(
            chat_id=GROUP_CHAT_ID, text=f"An error occurred: {context.error}"
        )  # send message to group
    except Exception as e:
        logger.error(f"Error sending error message: {e}")



async def check_and_delete_webhook(bot, retries=3, delay=5):
    """Checks for and deletes any active webhooks with retries."""
    for attempt in range(retries):
        try:
            webhook_info = await bot.get_webhook_info()
            if webhook_info.url:
                logger.warning(
                    f"Webhook found, deleting... (Attempt {attempt + 1}/{retries})"
                )
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook deleted.")
                return  # Exit if successful
            else:
                logger.info("Webhook is not active.")
                return
        except Exception as e:
            logger.error(
                f"Error deleting webhook: {e} (Attempt {attempt + 1}/{retries})"
            )
            if attempt < retries - 1:
                await asyncio.sleep(delay)  # Wait before retrying
    logger.error("Failed to delete webhook after multiple retries.")  # Log on final failure



async def main():
    """Main function to start the bot."""
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Initialize Google Sheets connection
    gc = authorize_gspread()

    # Store gspread client and sheets in bot_data for use in handlers
    app.bot_data["gc"] = gc
    app.bot_data["pred_sheet"] = get_sheet(gc, PREDICTIONS_SHEET_ID)
    app.bot_data["poll_map_sheet"] = get_sheet(gc, POLL_MAP_SHEET_ID)

    # Delete webhook at the very start, with retries
    await check_and_delete_webhook(app.bot)

    # Add handlers
    app.add_handler(CommandHandler("startpoll", startpoll))
    app.add_handler(CommandHandler("score", score_match))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("getchatid", get_chat_id))
    app.add_handler(PollAnswerHandler(handle_poll_answer))
    app.add_error_handler(error_handler)

    # Load match schedule
    global schedule_mapping  # Make it accessible to scheduled_poll
    schedule_mapping = load_schedule_mapping(SCHEDULE_CSV)

    # Create a scheduler
    scheduler = AsyncIOScheduler()
    ist = pytz.timezone("Asia/Kolkata")
    now_utc = datetime.now(pytz.utc)

    # Schedule polls
    for match_no, match_info in schedule_mapping.items():
        try:
            match_date = datetime.strptime(match_info["Date"], "%d %b %Y")
            poll_time = datetime.strptime(match_info["PollStartTime"], "%I:%M %p").time()
            poll_dt_ist = ist.localize(datetime.combine(match_date, poll_time))
            poll_dt_utc = poll_dt_ist.astimezone(pytz.utc)
        except Exception as e:
            logger.error(f"Invalid datetime for match {match_no}: {e}")
            continue

        if poll_dt_utc > now_utc:
            scheduler.add_job(
                scheduled_poll,
                "date",
                run_date=poll_dt_utc,
                args=[app.bot, match_no, match_info],
                id=f"poll_{match_no}",
            )

    scheduler.start()

    # Define a periodic task to check and delete the webhook
    async def periodic_check():
        while True:
            await check_and_delete_webhook(app.bot)
            await asyncio.sleep(
                30
            )  # Check every 5 minutes (more frequent, adjust as needed)

    # Run the bot and the periodic check concurrently
    try:
        await asyncio.gather(app.run_polling(), periodic_check())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logging.info("Application exiting.")


if __name__ == "__main__":
    asyncio.run(main())
