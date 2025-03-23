import logging
import os
import asyncio
import pandas as pd
from datetime import datetime
import pytz

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    PollAnswerHandler,
    ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import nest_asyncio

# Allow nested event loops (helpful if running in notebooks or certain IDEs)
nest_asyncio.apply()

# === CONFIGURATION ===
BOT_TOKEN = "7897221989:AAHZoD6r03Qj21v4za2Zha3XFwW5o5Hw4h8"        # Replace with your actual bot token
GROUP_CHAT_ID = -4607914574            # Replace with your Telegram group chat ID
SCHEDULE_CSV = "ipl_schedule.csv"         # CSV file with the match schedule
PREDICTIONS_CSV = "ipl_predictions.csv"   # CSV file to store user predictions

# Set up logging for debugging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Global dictionary to store poll info (maps poll_id -> match details)
polls = {}

# -------------------------------
# Helper Function to Load Schedule from CSV
# -------------------------------
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


async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID: `{update.effective_chat.id}`", parse_mode="Markdown")


async def scheduled_poll(bot, match_no, match_info):
    match_name = match_info["Teams"]
    question = f"Match {match_no}: {match_name}\nVenue: {match_info['Venue']}\nWho will win?"
    options = match_name.split(" vs ")

    poll_message = await bot.send_poll(
        GROUP_CHAT_ID,
        question,
        options,
        False,
        None,
        False
    )

    polls[poll_message.poll.id] = {
        "match_no": match_no,
        "match_name": match_name,
        "options": options
    }

    # Save poll mapping persistently
    if os.path.exists(POLL_MAP_FILE):
        with open(POLL_MAP_FILE, "r") as f:
            poll_map = json.load(f)
    else:
        poll_map = {}
    poll_map[poll_message.poll.id] = match_no
    with open(POLL_MAP_FILE, "w") as f:
        json.dump(poll_map, f)

    logging.info(f"[Scheduled] Poll posted for match {match_no}: {match_name}")


async def startpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /startpoll <match_no>")
        return
    try:
        match_no = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Match number must be an integer.")
        return
    match_info = schedule_mapping.get(match_no)
    if not match_info:
        await update.message.reply_text(f"No match found for match #{match_no}.")
        return
    match_name = match_info["Teams"]
    question = f"Match {match_no}: {match_name}\nVenue: {match_info['Venue']}\nWho will win?"
    options = match_name.split(" vs ")
    poll_message = await context.bot.send_poll(
        update.effective_chat.id,
        question,
        options,
        False,
        None,
        False
    )
    context.bot_data[poll_message.poll.id] = {
        "match_no": match_no,
        "match_name": match_name,
        "options": options
    }
    if os.path.exists(POLL_MAP_FILE):
        with open(POLL_MAP_FILE, "r") as f:
            poll_map = json.load(f)
    else:
        poll_map = {}
    poll_map[poll_message.poll.id] = match_no
    with open(POLL_MAP_FILE, "w") as f:
        json.dump(poll_map, f)

    logging.info(f"[Manual] Poll posted for match {match_no}: {match_name}")


async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.poll_answer
    poll_id = answer.poll_id
    user = answer.user
    option_ids = answer.option_ids

    if os.path.exists(POLL_MAP_FILE):
        with open(POLL_MAP_FILE, "r") as f:
            poll_map = json.load(f)
        match_no = poll_map.get(poll_id)
    else:
        return

    if not match_no:
        return

    match_info = schedule_mapping.get(int(match_no))
    if not match_info:
        return

    match_name = match_info["Teams"]
    options = match_name.split(" vs ")
    chosen_team = options[option_ids[0]] if option_ids else None
    username = user.full_name

    if os.path.exists(PREDICTIONS_CSV):
        df = pd.read_csv(PREDICTIONS_CSV)
    else:
        df = pd.DataFrame(columns=["MatchNo", "Match", "Username", "Prediction", "Correct"])

    row_mask = (df["MatchNo"] == int(match_no)) & (df["Username"] == username)
    if df[row_mask].empty:
        new_row = {
            "MatchNo": int(match_no),
            "Match": match_name,
            "Username": username,
            "Prediction": chosen_team,
            "Correct": ""
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df.loc[row_mask, "Prediction"] = chosen_team
        df.loc[row_mask, "Correct"] = ""

    df.to_csv(PREDICTIONS_CSV, index=False)
    logging.info(f"{username} voted {chosen_team} for match {match_no} ({match_name}).")

# -------------------------------
# Command: /score <match_no> <winner>
# -------------------------------
async def score_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Manually inputs the result of a match and updates predictions.
    Usage: /score <match_no> <winner>
    """
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /score <match_no> <winner>")
        return

    try:
        match_no = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Match number must be an integer.")
        return

    winner = context.args[1]
    if not os.path.exists(PREDICTIONS_CSV):
        await update.message.reply_text("No predictions found.")
        return

    df = pd.read_csv(PREDICTIONS_CSV)
    row_mask = (df["MatchNo"] == match_no)
    if df[row_mask].empty:
        await update.message.reply_text(f"No predictions found for match #{match_no}.")
        return

    df.loc[row_mask, "Correct"] = df.loc[row_mask, "Prediction"].apply(
        lambda pred: 1 if pred == winner else 0
    )
    df.to_csv(PREDICTIONS_CSV, index=False)
    await update.message.reply_text(f"Score updated for match {match_no}. Winner: {winner}")
    await update.message.reply_text(f"Results for match {match_no} have been recorded!")

# -------------------------------
# Command: /leaderboard
# -------------------------------
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Displays the leaderboard by summing correct predictions per user.
    """
    if not os.path.exists(PREDICTIONS_CSV):
        await update.message.reply_text("No predictions made yet.")
        return

    df = pd.read_csv(PREDICTIONS_CSV)
    if df.empty:
        await update.message.reply_text("No predictions found in CSV.")
        return

    df["Correct"] = pd.to_numeric(df["Correct"], errors="coerce").fillna(0).astype(int)
    lb = df.groupby("Username")["Correct"].sum().reset_index()
    lb = lb.sort_values("Correct", ascending=False)

    msg = "🏆 *Leaderboard* 🏆\n\n"
    for _, row in lb.iterrows():
        msg += f"{row['Username']}: {row['Correct']} points\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

# -------------------------------
# Main Function: Start Bot and Scheduler
# -------------------------------
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register command handlers
    app.add_handler(CommandHandler("startpoll", startpoll))
    app.add_handler(CommandHandler("score", score_match))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("getchatid", get_chat_id))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    # Set up APScheduler for automatic poll scheduling
    scheduler = AsyncIOScheduler()
    ist = pytz.timezone("Asia/Kolkata")
    now_utc = datetime.now(pytz.utc)

    # Schedule a poll for every match from the schedule mapping
    for match_no, match_info in schedule_mapping.items():
        try:
            # Combine Date and Poll Start Time from CSV (e.g., "22 Mar 2025" and "12:00 AM")
            match_date = datetime.strptime(match_info["Date"], "%d %b %Y")
            poll_start_time = datetime.strptime(match_info["PollStartTime"], "%I:%M %p").time()
            poll_start_dt_ist = ist.localize(datetime.combine(match_date, poll_start_time))
            poll_start_dt_utc = poll_start_dt_ist.astimezone(pytz.utc)
        except Exception as e:
            logging.error(f"Error parsing schedule for match {match_no}: {e}")
            continue

        if poll_start_dt_utc > now_utc:
            scheduler.add_job(
                scheduled_poll, 'date',
                run_date=poll_start_dt_utc,
                args=[app.bot, match_no, match_info],
                id=f"poll_{match_no}"
            )
            logging.info(f"Scheduled poll for match {match_no} at {poll_start_dt_utc}")
        else:
            logging.info(f"Poll start time for match {match_no} is in the past; skipping scheduling.")

    scheduler.start()
    logging.info("Scheduler started with automatic poll jobs.")

    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
