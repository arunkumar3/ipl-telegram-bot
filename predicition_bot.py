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
eyJ0eXBlIjoic2VydmljZV9hY2NvdW50IiwicHJvamVjdF9pZCI6ImlwbC1wcmVkaWN0aW9ucyIsInByaXZhdGVfa2V5X2lkIjoiMWUyYzgwZGJmN2E5NWUyMmYwNjIyYTU0MTlhOTk0NWEwNmZlZTdkNSIsInByaXZhdGVfa2V5IjoiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXG5NSUlFdmdJQkFEQU5CZ2txaGtpRzl3MEJBUUVGRUFBU0NCS2d3Z2dTa0FnRUFBb0lCQVFEMmxiSlF5UmpNRWRQZ1xud2VYSyt4SnNXRENOUzdFcE8yekpaL3pNblptdWNocjJ2UG1reE1aYjlNMVc4OXBSSW8ycVV6YVhHRzBha0puRVxuLytKNlIrT1pnMGVyZTYrbG5VQVpzRW5NS0hlNHlTeVZaRnRqTjhKb1BzS2lSWU5LbzFxT2xCT3lqOTM1RUVUT1xuS3NNbU5EM0NHZEJMamhUTm9wc0EvZWJwaXJSeUNDEUElxZ2dFYnN1TzZPTkp1R2t2aFduRXprdUNkZ0JtWXM0bFxuVXZvKytsV3Foa0J1aVM0bTdEQlk4UUY4RjVBMXFTMEYvU3k1QjVnQWJNa3YvSVNHQmdWTGo4bWlsQWJUN3NWcVxuOWRVSW5yRzZKR2UrNVJaL3hsS0JCMzRxTzNiMXdoRTVNSk9XRzZwNUVVVTY1SVBFR3BiVm44TytjVVF4eFRrS1xuWEwzSGo0SExBZ01CQUFFQ2dnRUFMU3RNeDdtZW5qc2h5Y2thOCt5NytqYkpaZ2lZZ2tvTmFuTnUvbmhjSDNWdFxuZFFjMjM3VGY5UlpKeXdUT1dCQnhUT01EVy9nd2ZDNUN6TEJtNXlsbUNzZmVpQTRYSmNwQlpMTkRRVytrUnFnclxuVDQ1Ym96Z2lsMy92blZRSWNkS3Nrdk9URENHSWNzaEZJbEw0L25XUlhvM2w5d0VtMUdoVjVuK2NIZW0zbGkrSVxuYUhWTGhxdmpLUDgydUJ6aktlMnU4Q0tDQ25kdko1QmVzQzYyY3VtVkhZYnRSWFVsOHArbjBpZ2hhdkV6TWxXOFxuTEczSS9XeEFDNWNvbTduaGNEV2l2ays4VXdQL3lUZHdUT09UdU9EN3Njb05xbm9zb3JxOXEzRUJyTFFzUGtOZ1xudmhhcEp6bFZKTUdHeHNhVGhuUEFBNzhiTE5hVEVYUSs2WUVpemVFZXVRS0JnUUQ3d1M5MVpIUVdMQm5BS2Y4Zlxuci9vL1QxVURSY1dwYlhVaWVTTnhSZi82VDBkVnJQZG5WM3RKdlRIL1ZPMHlpMkdMVmV4N0RuZEdnUFlZR3hSclxuOTdYQmkyQ1paYXk3cWx0YWRlWkN1eUc3WlFKeTZ1TmRHRnM3RWwweHpCTEc0Q01lc3B5WWh5OFNoaTVCTmYyR1xuSnl0SXVXaUJ0Q0hwWGR4RG5ka2FwRWJBM3dLQmdRRDZ2akZvcmpHazF6WVpBbGphSCtLcHRLdC9YSDZwYng1ZVxueVVPQXFmM1ZDQWx3aVpycWlKd1N4bnM2SU13eHREMWlrRnRNb0doKzRUWG5ZRXlvbEUzYklsU2UzOXVubVdyMFxuVFJIR3RWMmxUZk9oUklTWEIvRFVlRG01WFAxRnV1b3VEYmxMRjJhbG1ETndycDIycFFnK3FTV0VoL1YrM1pBeVxuMG9JTmU0YkFsUUtCZ0g2MWY4WnM5Y1NIRTdyVktGUHhoVmxCKzM2M0trSVpGa0J3aWZja0RTOFZvY2lzVXFVUFxuc2J5dVhiQ3VOT2dnb01xNVIxbTBNVElxRERLYnhvNkUwVlVGYW13d1F
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
