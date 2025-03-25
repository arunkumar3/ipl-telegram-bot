import logging
import os
import json
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

nest_asyncio.apply()
import base64
import aiohttp

async def commit_files_to_github():
    repo = os.getenv("GITHUB_REPO")
    branch = os.getenv("GITHUB_BRANCH", "main")
    token = os.getenv("GITHUB_PAT")
    author_name = os.getenv("BOT_COMMIT_NAME", "Bot")
    author_email = os.getenv("BOT_COMMIT_EMAIL", "bot@example.com")

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            logging.info("🔁 Getting latest commit SHA...")
            async with session.get(f"https://api.github.com/repos/{repo}/git/ref/heads/{branch}") as r:
                if r.status != 200:
                    logging.error(f"❌ Failed to get branch ref: {await r.text()}")
                    return
                ref_data = await r.json()

            commit_sha = ref_data["object"]["sha"]
            logging.info(f"✅ Found latest commit SHA: {commit_sha}")

            async with session.get(f"https://api.github.com/repos/{repo}/git/commits/{commit_sha}") as r:
                if r.status != 200:
                    logging.error(f"❌ Failed to get commit data: {await r.text()}")
                    return
                commit_data = await r.json()

            base_tree = commit_data["tree"]["sha"]

            files_to_commit = ["ipl_predictions.csv", "poll_map.json"]
            blobs = []

            for file_path in files_to_commit:
                if not os.path.exists(file_path):
                    logging.warning(f"⚠️ File not found: {file_path}")
                    continue

                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                async with session.post(f"https://api.github.com/repos/{repo}/git/blobs", json={
                    "content": content,
                    "encoding": "utf-8"
                }) as r:
                    if r.status != 201:
                        logging.error(f"❌ Blob creation failed for {file_path}: {await r.text()}")
                        return
                    blob_data = await r.json()

                blobs.append({
                    "path": file_path,
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob_data["sha"]
                })

            async with session.post(f"https://api.github.com/repos/{repo}/git/trees", json={
                "base_tree": base_tree,
                "tree": blobs
            }) as r:
                if r.status != 201:
                    logging.error(f"❌ Tree creation failed: {await r.text()}")
                    return
                tree_data = await r.json()

            async with session.post(f"https://api.github.com/repos/{repo}/git/commits", json={
                "message": "📥 Update predictions and poll map",
                "tree": tree_data["sha"],
                "parents": [commit_sha],
                "author": {
                    "name": author_name,
                    "email": author_email
                }
            }) as r:
                if r.status != 201:
                    logging.error(f"❌ Commit creation failed: {await r.text()}")
                    return
                new_commit = await r.json()

            async with session.patch(f"https://api.github.com/repos/{repo}/git/refs/heads/{branch}", json={
                "sha": new_commit["sha"]
            }) as r:
                if r.status != 200:
                    logging.error(f"❌ Failed to update ref: {await r.text()}")
                    return

            logging.info("✅ GitHub commit successful.")

    except Exception as e:
        logging.exception(f"🚨 Exception while committing to GitHub: {e}")

        

# === CONFIGURATION ===
BOT_TOKEN = "7897221989:AAHZoD6r03Qj21v4za2Zha3XFwW5o5Hw4h8"
GROUP_CHAT_ID = -4607914574
SCHEDULE_CSV = "ipl_schedule.csv"
PREDICTIONS_CSV = "ipl_predictions.csv"
POLL_MAP_FILE = "poll_map.json"  # ✅ FIXED: Added missing definition

# === Logging ===
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Global dictionary (in-memory fallback)
polls = {}

# === Load Match Schedule from CSV ===
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

# === Get Chat ID ===
async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID: `{update.effective_chat.id}`", parse_mode="Markdown")

# === Scheduled Poll ===
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

    if os.path.exists(POLL_MAP_FILE):
        with open(POLL_MAP_FILE, "r") as f:
            poll_map = json.load(f)
    else:
        poll_map = {}
    poll_map[poll_message.poll.id] = match_no
    with open(POLL_MAP_FILE, "w") as f:
        json.dump(poll_map, f)
    logging.info(f"[Scheduled] Poll posted for match {match_no}: {match_name}")
    logging.info(f"Poll map updated: {poll_message.poll.id} → Match {match_no}")

# === Manual Poll: /startpoll <match_no> ===
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
    logging.info(f"Poll map updated: {poll_message.poll.id} → Match {match_no}")

# === Handle Poll Answers ===
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
    await commit_files_to_github()
    logging.info(f"{username} voted {chosen_team} for match {match_no} ({match_name}).")
    

# === /score Command ===
async def score_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    await commit_files_to_github()
    await update.message.reply_text(f"Score updated for match {match_no}. Winner: {winner}")
    await update.message.reply_text(f"Results for match {match_no} have been recorded!")
    

# === /leaderboard Command ===
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

# === /commit Command ===
async def commit_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Trying to commit files to GitHub...")

    try:
        await commit_files_to_github()
        await update.message.reply_text("✅ Files successfully committed to GitHub.")
    except Exception as e:
        logging.exception("❌ Exception during manual /commit")
        await update.message.reply_text(f"❌ Commit failed: {e}")


# === MAIN ===
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("startpoll", startpoll))
    app.add_handler(CommandHandler("score", score_match))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("getchatid", get_chat_id))
    app.add_handler(PollAnswerHandler(handle_poll_answer))
    app.add_handler(CommandHandler("commit", commit_now))


    scheduler = AsyncIOScheduler()
    ist = pytz.timezone("Asia/Kolkata")
    now_utc = datetime.now(pytz.utc)

    for match_no, match_info in schedule_mapping.items():
        try:
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
            logging.info(f"Poll time for match {match_no} is in the past; skipping.")

    scheduler.start()
    logging.info("Scheduler started with automatic poll jobs.")

    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
