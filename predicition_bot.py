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
    ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import nest_asyncio

from google.oauth2.service_account import Credentials
import gspread

nest_asyncio.apply()

creds_json = """
eyJ0eXBlIjoic2VydmljZV9hY2NvdW50IiwicHJvamVjdF9pZCI6ImlwbC1wcmVkaWNpdGlvbnMiLCJwcml2YXRlX2tleV9pZCI6IjFlMmM4MGRiZjdhOTVlMjJmMDYyMmE1NDE5YTk5NDVhMDZmZWU3ZDUiLCJwcml2YXRlX2tleSI6Ii0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZnSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2d3Z2dTa0FnRUFBb0lCQVFEMmxiSlF5UmpNRWRQZ1xud2VYSyt4SnNXRENOUzdFcE8yekpaL3pNblptdWNocjJ2UG1reE1aYjlNMVc4OXBSSW8ycVV6YVhHRzBha0puRVxuLytKNlIrT1pnMGVyZTYrbG5VQVpzRW5NS0hlNHlTeVZaRnRqTjhKb1BzS2lSWU5LbzFxT2xCT3lqOTM1RUVUT1xuS3NNbU5EM0NHZEJMamhUTm9wc0EvZWJwaXJSeUNEUElxZ2dFYnN1TzZPTkp1R2t2aFduRXprdUNkZ0JtWXM0bFxuVXZvKytsV3Foa0J1aVM0bTdEQlk4UUY4RjVBMXFTMEYvU3k1QjVnQWJNa3YvSVNHQmdWTGo4bWlsQWJUN3NWcVxuOWRVSW5yRzZKR2UrNVJaL3hsS0JCMzRxTzNiMXdoRTVNSk9XRzZwNUVVVTY1SVBFR3BiVm44TytjVVF4eFRrS1xuWEwzSGo0SExBZ01CQUFFQ2dnRUFMU3RNeDdtZW5qc2h5Y2thOCt5NytqYkpaZ2lZZ2tvTmFuTnUvbmhjSDNWdFxuZFFjMjM3VGY5UlpKeXdUT1dCQnhUT01EVy9nd2ZDNUN6TEJtNXlsbUNzZmVpQTRYSmNwQlpMTkRRVytrUnFnclxuVDQ1Ym96Z2lsMy92blZRSWNkS3Nrdk9URENHSWNzaEZJbEw0L25XUlhvM2w5d0VtMUdoVjVuK2NIZW0zbGkrSVxuYUhWTGhxdmpLUDgydUJ6aktlMnU4Q0tDQ25kdko1QmVzQzYyY3VtVkhZYnRSWFVsOHArbjBpZ2hhdkV6TWxXOFxuTEczSS9XeEFDNWNvbTduaGNEV2l2ays4VXdQL3lUZHdUT09UdU9EN3Njb05xbm9zb3JxOXEzRUJyTFFzUGtOZ1xudmhhcEp6bFZKTUdHeHNhVGhuUEFBNzhiTE5hVEVYUSs2WUVpemVFZXVRS0JnUUQ3d1M5MVpIUVdMQm5BS2Y4Zlxuci9vL1QxVURSY1dwYlhVaWVTTnhSZi82VDBkVnJQZG5WM3RKdlRIL1ZPMHlpMkdMVmV4N0RuZEdnUFlZR3hSclxuOTdYQmkyQ1paYXk3cWx0YWRlWkN1eUc3WlFKeTZ1TmRHRnM3RWwweHpCTEc0Q01lc3B5WWh5OFNoaTVCTmYyR1xuSnl0SXVXaUJ0Q0hwWGR4RG5ka2FwRWJBM3dLQmdRRDZ2akZvcmpHazF6WVpBbGphSCtLcHRLdC9YSDZwYng1ZVxueVVPQXFmM1ZDQWx3aVpycWlKd1N4bnM2SU13eHREMWlrRnRNb0doKzRUWG5ZRXlvbEUzYklsU2UzOXVubVdyMFxuVFJIR3RWMmxUZk9oUklTWEIvRFVlRG01WFAxRnV1b3VEYmxMRjJhbG1ETndycDIycFFnK3FTV0VoL1YrM1pBeVxuMG9JTmU0YkFsUUtCZ0g2MWY4WnM5Y1NIRTdyVktGUHhoVmxCKzM2M0trSVpGa0J3aWZja0RTOFZvY2lzVXFVUFxuc2J5dVhiQ3VOT2dnb01xNVIxbTBNVElxRERLYnhvNkUwVlVGYW13cWNXTE8za1hNVzBVdzNFeHVEV3A3Y0UzVlxueVcwVTFCVVJLazR3VjF6Rzl1d0o5aFl6dEtvYm0ydGU0WGtyNEQ2UHhCV3BxUWZiTlg4a09Yd3ZBb0dCQUo1MFxuNHpTTUlNTlRYZFNnTHhacFlBeHZLSkhzR2Y5cFFZQVZJSnVHMGVwMmtjQ1V0Vm5SeXcveWJwMWxiS1ZjaWc1blxudThySTlFQjZnbDRkOVZQenBOLys2Z3NjM09zbGdQbXlXckdBbkJXREZadXNlVDRZdnBFSENUT2pHRXVndTYwdVxuN3hJTlQ4a0dUanUvbmR0Mm42YzVyWVA3aDZFTTA3dktYSFc0d29laEFvR0JBTDhEMHRIM1JKb05sUFFNYTVBd1xuUEdXS01VbVNTNy90eXg3TEh2Zm92YjVjUHNyK2hMT1laKytiUWRIcUh2alBIREx3RWYxQXBsU2dvcVlNSEgzQ1xuSjI5cHhGL3oyMXJVTG1mRS92WTRJNHpYWHZiQTEyT1hrakJBQkQwZzJtQTVRcStFVlNDR2pwNlRreS9oaTYyWlxuY0lXOWxZS1RUWDk2WEg1dm5WL00yUXV0XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLCJjbGllbnRfZW1haWwiOiJpcGwtcHJlZGljdGlvbnNAaXBsLXByZWRpY2l0aW9ucy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsImNsaWVudF9pZCI6IjEwMjk5NjkzMzE0NDU3NTUxMDI4MyIsImF1dGhfdXJpIjoiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLCJ0b2tlbl91cmkiOiJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlbiIsImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0X3VybCI6Imh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsImNsaWVudF94NTA5X2NlcnRfdXJsIjoiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9pcGwtcHJlZGljdGlvbnMlNDBpcGwtcHJlZGljaXRpb25zLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwidW5pdmVyc2VfZG9tYWluIjoiZ29vZ2xlYXBpcy5jb20ifQ==
"""

creds_dict = json.loads(base64.b64decode(creds_json).decode("utf-8"))

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
    poll_map_sheet.append_row([str(poll_id), match_no])

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

    # 🔁 Fetch poll map from Google Sheets
    try:
        poll_map_df = pd.DataFrame(poll_map_sheet.get_all_records())
        poll_map_df["poll_id"] = poll_map_df["poll_id"].astype(str)
        match_row = poll_map_df[poll_map_df["poll_id"] == str(poll_id)]
        if match_row.empty:
            logging.warning(f"No match found for poll_id: {poll_id}")
            return
        match_no = int(match_row.iloc[0]["match_no"])
    except Exception as e:
        logging.error(f"Error fetching poll_map: {e}")
        return

    # Get match info from preloaded schedule mapping
    match_info = schedule_mapping.get(match_no)
    if not match_info:
        logging.warning(f"No match info found for match_no: {match_no}")
        return
    if not option_ids:
        logging.warning(f"{user.full_name} submitted an empty vote for poll {poll_id}. Skipping.")
        return
    chosen_team = match_info['Teams'].split(" vs ")[option_ids[0]]
    username = user.full_name

    try:
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
    except Exception as e:
        logging.error(f"Error updating prediction: {e}")

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
    # Delete webhook before polling
    await app.bot.delete_webhook(drop_pending_updates=True)
    logging.info("✅ Webhook deleted before polling.")

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
            scheduler.add_job(
                scheduled_poll,
                'date',
                run_date=poll_dt_utc,
                args=[app.bot, match_no, match_info],
                id=f"poll_{match_no}"
            )

    scheduler.start()

    # ✅ Disable webhook before polling
    await app.bot.delete_webhook(drop_pending_updates=True)

    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())

