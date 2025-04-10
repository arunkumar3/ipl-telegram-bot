import logging
import os
import json
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

nest_asyncio.apply()

# === Config ===
BOT_TOKEN = "7897221989:AAHZoD6r03Qj21v4za2Zha3XFwW5o5Hw4h8"
GROUP_CHAT_ID = -4607914574
SCHEDULE_CSV = "ipl_schedule.csv"
PREDICTIONS_SHEET_ID = "1iCENqo8p3o3twurO7-CSs490C1xrhxK7cNIsUJ4ThBA"
POLL_MAP_SHEET_ID = "1LogmznPifIPt7GQQPQ7UndHOup4Eo55ThraIUMeH2uE"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    level=logging.INFO,  # Set the desired logging level here
)

polls = {}

# === Google Sheets Interaction ===
creds_json = """
eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsICJwcm9qZWN0X2lkIjogImlwbC1wcmVkaWNpdGlvbnMiLCAicHJpdmF0ZV9rZXlfaWQiOiAiOGIxZDgwMDQzOTg2YjUwZjYyOGQzMzFiYzdiMWE0OWYxYTUzMTBlNCIsICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2QUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktZd2dnU2lBZ0VBQW9JQkFRQ3NyOTgzUkZyRlpUeVZcbmExcnpYUjk1R1VKb2xDcVZzcDBiajRzTDVxdXVOdmpLSjJ3K296YjNWRHRpQ0lPV1pnaytFOUJwZE55SWk0bnFcbnVVUXlmZWxLT1FsNEw5OWNCZWxaNktTQkZMTC9uYnBaS21qRzBLRDUxTCtSVjdGSVQ1Yzc4dGdSNk54L095SGNcbjA5aEtqeUJuMzhMTm54VkZiQ2Z2cU1BVWFmbVRoZjRJNGE3UkFYTmk4ZjhjZVBrWkJablk2YzZkNXFGdTMyUFdcbmJSZEZaMFRFeGhXRFFjMDJiSzhLN0g4dm9pRVZYTFpLWjJNNnkxN3F1NUZtUFBtZHVubWpYbXNnN3VUSFNpT3lcbmhrd0R3SlI3Mlo2d2ZmaGVJR3ZpbHlWb3hERmx3bW15T1ViVTVuZnlrbmtLN2xNZHJZbjFmM0F1S2pTelB1aElcblc0VVQzZFlYQWdNQkFBRUNnZ0VBSlZrTUw4bkt6L0pyUGUyd0IvNVY5anp1VGV2dG9kNjFkK1o5cmg4L2RqaFJcbmFuZElRK3ZNMFlVWUtzV29uL2lGZXpXUjE1ejhyVk53aXFGekRIQ0s2aENYNmJTQTNFZ3pCY3o0OXluZzVNUGFcbkw3cXFXb1Y0cTAvRjlzcytmbU1vVkVEYlZsUkVqQWZmOVFDa1FNdmZ1RmQrckRZQnhiZjBrekt1Q0R3N1RCcE1cbjR5MDB0VzlHeGEybDF3YkQ1ZGlSM3I5OFNhUzRNUkVHQVBXd2FWSEszVGttRnJ3c2lwbGFUeFRCSGZNTlpEMUVcbmFQeGdYNk1qendzbDRGRDNWZ1JhbWlMKzdxd1RrNUdvZHFzNCtGdFVnRVNuQXNWVWRBb2pLK1ZyWDRsZER6UmNcbmtXeVBYQ3BUK3orL2pzUzd4c3ZsTzM1L2lQTEIydjk5eWJ4M21xLzFvUUtCZ1FEdzE5TXhuVjNyY0NnZTZkNHRcbkwxZ1F6cFhEM2x0V0F1K1VPZFhWYlVpZkRWMVowOXhzc1NMdVBuU1ZEOU0rR3NoTWZSRkRNZWwvUG1xK3lHdkFcbmpYK2JxMEFoU3pJVnkvSGJhU241YUxzMkd1N2t3SWpqa0x5ZzhVR3pTNmJzK2VSTXBLSTJIL0lXSjRWa3lLUnFcblVaYm9yZit3TVIwa0YyY282UUU5V3hQZS93S0JnUUMzamYrQUZIY2gvbGNXVlVWMVkzQTBXRy9OTE5VaHhyb0tcbm1aa2xuVVhEd0lNQnNVNUZqekFFZkFFajZGSXE2VlpoRjQzTksyTll3SmxwaTcybW11amszQmd6Vml1OGRSZG5cbjQwakFOWEJya2c4SzNMS3FnVmRoZ0NiQXFiQ1FZRzFoSEppL3RRaFJwM0J3QUMzbms5b0VGZFhpSzV1TnRJLzFcblBycTlOUmdnNlFLQmdDSDRqMVdFT09jb25zQWRoTFVpNUcwYWRvMTJJN1B5SGhEdVIzY2ZQd3NRTzRhY0Y0OU5cblBQd1YyeVBiWTVSeStxV3ZUbXdIOGtOOGJsb1Nzd0FwOVVIajJkdllXMnd2cENHcXA3MENSTVhRN3JsZFh2R2FcblRNRDJ4cW1mbGgvKzczRFFHQUZDYUVjdnMrVVBXQUdYR0k0aFhOdGhVaGJ4SmgvakhjV2x2eHZKQW9HQU9YWDJcbldmNE9IVklscVJRZ25sTDJ1U3hHTTVDcFY5MkNOL2RGZmdUeDVnbkorU21zT3hKTUVkdFA4QkcyUjBDc2pkQjFcbno1aVpqUnNkNjNDWGVpUmNhK2lLbXVlSzRZQTJSNHRiSnZDVHROa1FaSElhYkUzNU1NaVJXUmJGOHl4OGtUNEdcbmcxMEVzYXNkQTdMS3JBZ1k0OWFDRWo5Y2ZzdmJsUWFDSnVFTUlLRUNnWUFGelFEYTE3ZzhiRzl2MkNTWHlzTWxcbmNjY1d0WSt3T3VaQTZYem9jOWJKMHI4WkMyVzJkNXRCUHpqOHpXQm9ISWdpYitYemtDZ3F2M2pPaXRKQXkreXFcbktvb3hiamVZOE9uRFg3RmQyeS9JcVZXOHRTelBaNTRGMEFyWVFlMkdGekRwbHdHZnE0OERsZy9Cc3VyRHhQaURcbk8yOVlWZkd5WkZRb0ZTRjdsT2E0bkE9PVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwgImNsaWVudF9lbWFpbCI6ICJpcGwtcHJlZGljdGlvbnNAaXBsLXByZWRpY2l0aW9ucy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsICJjbGllbnRfaWQiOiAiMTAyOTk2OTMzMTQ0NTc1NTEwMjgzIiwgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwgInRva2VuX3VyaSI6ICJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlbiIsICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvaXBsLXByZWRpY3Rpb25zJTQwaXBsLXByZWRpY2l0aW9ucy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20ifQ==
"""
creds_dict = json.loads(base64.b64decode(creds_json).decode("utf-8"))
creds = Credentials.from_service_account_info(
    creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

pred_sheet = gc.open_by_key(PREDICTIONS_SHEET_ID).sheet1
poll_map_sheet = gc.open_by_key(POLL_MAP_SHEET_ID).sheet1


def get_predictions_df():
    try:
        data = pred_sheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Error in get_predictions_df: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def save_predictions_df(df):
    try:
        pred_sheet.clear()
        pred_sheet.update([df.columns.values.tolist()] + df.values.tolist())
    except Exception as e:
        logging.error(f"Error in save_predictions_df: {e}")


def get_poll_map():
    try:
        rows = poll_map_sheet.get_all_records()
        return {row["PollID"]: row["MatchNo"] for row in rows if "PollID" in row and "MatchNo" in row}
    except Exception as e:
        logging.error(f"Error in get_poll_map: {e}")
        return {}  # Return an empty dict in case of error


def save_poll_id(poll_id, match_no):
    try:
        poll_map_sheet.append_row([str(poll_id), match_no])
    except Exception as e:
        logging.error(f"Error in save_poll_id: {e}")


# === Helper: Load Schedule ===
def load_schedule_mapping(csv_file):
    schedule_mapping = {}
    try:
        df = pd.read_csv(csv_file)
        df.columns = df.columns.str.strip()

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
                logging.error(f"Error processing row: {row}. Exception: {e}")
    except FileNotFoundError:
        logging.error(f"Error: File not found at {csv_file}")
    except Exception as e:
        logging.error(f"Error loading schedule from CSV: {e}")
    return schedule_mapping


schedule_mapping = load_schedule_mapping(SCHEDULE_CSV)


# === Bot Commands and Logic ===
async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text(f"Chat ID: `{update.effective_chat.id}`", parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error in get_chat_id: {e}")


async def scheduled_poll(bot, match_no, match_info):
    try:
        match_name = match_info["Teams"]
        question = f"Match {match_no}: {match_name}\nVenue: {match_info['Venue']}\nWho will win?"
        options = match_name.split(" vs ")

        poll_message = await bot.send_poll(GROUP_CHAT_ID, question, options, False, None, False)

        polls[poll_message.poll.id] = {"match_no": match_no, "match_name": match_name, "options": options}
        save_poll_id(poll_message.poll.id, match_no)
        logging.info(f"Poll posted for match {match_no}: {match_name}")
    except Exception as e:
        logging.error(f"Error in scheduled_poll: {e}")



async def startpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args:
            await update.message.reply_text("Usage: /startpoll <match_no>")
            return
        match_no = int(context.args[0])
        match_info = schedule_mapping.get(match_no)
        if not match_info:
            await update.message.reply_text("Invalid match number.")
            return
        question = f"Match {match_no}: {match_info['Teams']}\nVenue: {match_info['Venue']}\nWho will win?"
        options = match_info['Teams'].split(" vs ")

        poll_message = await context.bot.send_poll(update.effective_chat.id, question, options, False, None, False)
        context.bot_data[poll_message.poll.id] = {"match_no": match_no, "match_name": match_info['Teams'],
                                                 "options": options}
        save_poll_id(poll_message.poll.id, match_no)
    except Exception as e:
        logging.error(f"Error in startpoll: {e}")



async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        answer = update.poll_answer
        poll_id = answer.poll_id
        user = answer.user
        option_ids = answer.option_ids

        poll_map_df = pd.DataFrame(poll_map_sheet.get_all_records())
        poll_map_df["PollID"] = poll_map_df["PollID"].astype(str)  # Ensure PollID is treated as string
        match_row = poll_map_df[poll_map_df["PollID"] == str(poll_id)]

        if match_row.empty:
            logging.warning(f"No match found for poll_id: {poll_id}")
            return
        match_no = int(match_row.iloc[0]["MatchNo"])

        match_info = schedule_mapping.get(match_no)
        if not match_info:
            logging.warning(f"No match info found for match_no: {match_no}")
            return
        if not option_ids:
            logging.warning(f"{user.full_name} submitted an empty vote for poll {poll_id}. Skipping.")
            return
        chosen_team = match_info['Teams'].split(" vs ")[option_ids[0]]
        username = user.full_name

        df = get_predictions_df()
        if df.empty:
            df = pd.DataFrame(columns=["MatchNo", "Match", "Username", "Prediction", "Correct"])
        row_mask = (df["MatchNo"] == int(match_no)) & (df["Username"] == username)

        if df[row_mask].empty:
            new_row = pd.DataFrame([[int(match_no), match_info['Teams'], username, chosen_team, ""]],
                                   columns=["MatchNo", "Match", "Username", "Prediction", "Correct"])
            df = pd.concat([df, new_row], ignore_index=True)
        else:
            df.loc[row_mask, "Prediction"] = chosen_team
            df.loc[row_mask, "Correct"] = ""
        save_predictions_df(df)
        logging.info(f"{username} voted {chosen_team} for match {match_no}.")
    except Exception as e:
        logging.error(f"Error in handle_poll_answer: {e}")



async def score_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /score <match_no> <winner>")
            return
        match_no = int(context.args[0])
        winner = context.args[1]

        df = get_predictions_df()
        df.loc[df["MatchNo"] == match_no, "Correct"] = df[df["MatchNo"] == match_no]["Prediction"].apply(
            lambda x: 1 if x == winner else 0)
        save_predictions_df(df)
        await update.message.reply_text(f"Score updated for match {match_no}. Winner: {winner}")
    except Exception as e:
        logging.error(f"Error in score_match: {e}")



async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        df = get_predictions_df()
        if df.empty:
            await update.message.reply_text("No predictions found.")
            return
        df["Correct"] = pd.to_numeric(df["Correct"], errors="coerce").fillna(0).astype(int)
        lb = df.groupby("Username")["Correct"].sum().reset_index().sort_values("Correct", ascending=False)
        msg = "🏆 *Leaderboard* 🏆\n\n"
        for _, row in lb.iterrows():
            msg += f"{row['Username']}: {row['Correct']} points\n"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error in leaderboard: {e}")



async def error_handler(update: Update, context: CallbackContext):
    """Log the error and send a telegram message."""
    logging.error(f"Update {update} caused error {context.error}")
    try:
        await context.bot.send_message(chat_id=GROUP_CHAT_ID,
                                       text=f"An error occurred: {context.error}")  # send message to group
    except Exception as e:
        logging.error(f"Error sending error message: {e}")



async def check_and_delete_webhook(bot):
    """Check for and delete any active webhooks."""
    try:
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            logging.warning("Webhook found, deleting...")
            await bot.delete_webhook(drop_pending_updates=True)
            logging.info("Webhook deleted.")
        else:
            logging.info("Webhook is not active.")
    except Exception as e:
        logging.error(f"Error checking/deleting webhook: {e}")



async def main():
    """Start the bot."""
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Add handlers
    app.add_handler(CommandHandler("startpoll", startpoll))
    app.add_handler(CommandHandler("score", score_match))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("getchatid", get_chat_id))
    app.add_handler(PollAnswerHandler(handle_poll_answer))
    app.add_error_handler(error_handler)

    # Initial check and delete webhook
    await check_and_delete_webhook(app.bot)

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
            logging.error(f"Invalid datetime for match {match_no}: {e}")
            continue

        if poll_dt_utc > now_utc:
            scheduler.add_job(
                scheduled_poll,
                'date',
                run_date=poll_dt_utc,
                args=[app.bot, match_no, match_info],
                id=f"poll_{match_no}",
            )

    scheduler.start()

    # Define a periodic task to check and delete the webhook
    async def periodic_check():
        while True:
            await check_and_delete_webhook(app.bot)
            await asyncio.sleep(600)  # Check every 10 minutes (adjust as needed)

    # Run the bot and the periodic check concurrently
    try:
        await asyncio.gather(app.run_polling(), periodic_check())
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Application exiting.")


if __name__ == "__main__":
    asyncio.run(main())
