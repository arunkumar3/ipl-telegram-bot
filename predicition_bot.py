import logging
import os
import json
import asyncio
from datetime import datetime
import pytz
import pandas as pd

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    PollAnswerHandler,
    ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import nest_asyncio

from google.oauth2.service_account import Credentials
import gspread

nest_asyncio.apply()

creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64")

if creds_json is None:
    raise ValueError("❌ Environment variable 'GOOGLE_SERVICE_ACCOUNT_JSON_BASE64' is not set!")

creds_dict = json.loads(creds_json)

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

PREDICTIONS_SHEET_ID = "1iCENqo8p3o3twurO7-CSs490C1xrhxK7cNIsUJ4ThBA"
POLL_MAP_SHEET_ID = "1LogmznPifIPt7GQQPQ7UndHOup4Eo55ThraIUMeH2uE"

pred_sheet = gc.open_by_key(PREDICTIONS_SHEET_ID).sheet1
poll_map_sheet = gc.open_by_key(POLL_MAP_SHEET_ID).sheet1

# === Config ===
BOT_TOKEN = "7897221989:AAHZoD6r03Qj21v4za2Zha3XFwW5o5Hw4h8"
GROUP_CHAT_ID = -4607914574
SCHEDULE_CSV = "ipl_schedule.csv"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

polls = {}

# === Helper: Load Schedule ===
def load_schedule_mapping(csv_file):
    df = pd.read_csv(csv_file)
    df.columns = df.columns.str.strip()
    mapping = {}
    for _, row in df.iterrows():
        try:
            match_no = int(str(row["MatchNo"]).strip())
            mapping[match_no] = {
                "Date": str(row["Date"]).strip(),
                "Day": str(row["Day"]).strip(),
                "Teams": str(row["Teams"]).strip(),
                "MatchTime": str(row["MatchTime"]).strip(),
                "Venue": str(row["Venue"]).strip(),
                "PollStartTime": str(row["PollStartTime"]).strip(),
                "PollEndTime": str(row["PollEndTime"]).strip()
            }
        except Exception as e:
            logging.error(f"Error processing row: {row}. Exception: {e}")
    return mapping

schedule_mapping = load_schedule_mapping(SCHEDULE_CSV)

# === Google Sheets Interaction ===
def get_predictions_df():
    data = pred_sheet.get_all_records()
    return pd.DataFrame(data)

def save_predictions_df(df):
    pred_sheet.clear()
    pred_sheet.update([df.columns.values.tolist()] + df.values.tolist())

def get_poll_map():
    rows = poll_map_sheet.get_all_records()
    return {row["PollID"]: row["MatchNo"] for row in rows if "PollID" in row and "MatchNo" in row}

def save_poll_id(poll_id, match_no):
    poll_map_sheet.append_row([poll_id, match_no])

# === Bot Commands and Logic ===
async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID: `{update.effective_chat.id}`", parse_mode="Markdown")

async def scheduled_poll(bot, match_no, match_info):
    match_name = match_info["Teams"]
    question = f"Match {match_no}: {match_name}\nVenue: {match_info['Venue']}\nWho will win?"
    options = match_name.split(" vs ")

    poll_message = await bot.send_poll(GROUP_CHAT_ID, question, options, False, None, False)

    polls[poll_message.poll.id] = {"match_no": match_no, "match_name": match_name, "options": options}
    save_poll_id(poll_message.poll.id, match_no)
    logging.info(f"Poll posted for match {match_no}: {match_name}")

async def startpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    context.bot_data[poll_message.poll.id] = {"match_no": match_no, "match_name": match_info['Teams'], "options": options}
    save_poll_id(poll_message.poll.id, match_no)

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.poll_answer
    poll_id = answer.poll_id
    user = answer.user
    option_ids = answer.option_ids

    poll_map = get_poll_map()
    match_no = poll_map.get(poll_id)
    if not match_no:
        return
    match_info = schedule_mapping.get(int(match_no))
    if not match_info:
        return

    chosen_team = match_info['Teams'].split(" vs ")[option_ids[0]]
    username = user.full_name

    df = get_predictions_df()
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

async def score_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

# === MAIN ===
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("startpoll", startpoll))
    app.add_handler(CommandHandler("score", score_match))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("getchatid", get_chat_id))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    scheduler = AsyncIOScheduler()
    ist = pytz.timezone("Asia/Kolkata")
    now_utc = datetime.now(pytz.utc)

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
            scheduler.add_job(scheduled_poll, 'date', run_date=poll_dt_utc, args=[app.bot, match_no, match_info],
                              id=f"poll_{match_no}")

    scheduler.start()
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
