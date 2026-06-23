import gspread
from oauth2client.service_account import ServiceAccountCredentials
import config
import time
import threading
from datetime import datetime, timedelta
import uuid

# ==========================
# CACHE
# ==========================
SHEET_CACHE = None
SHEET_CACHE_TIME = None
SHEET_CACHE_TTL = 60

# ==========================
# BATCH SYSTEM
# ==========================
PENDING_WRITES = []
LOG_BUFFER = []

LAST_FLUSH_TIME = time.time()
FLUSH_INTERVAL = 5  # seconds (faster safe flush)

LOCK = threading.Lock()

CLAIM_AGENT_COL = 9
CLAIM_TIME_COL = 10
CLAIM_TOKEN_COL = 11
CLAIM_STATUS_COL = 12
CLAIM_TTL_MINUTES = 5
# --------------------------
# Logs disabled
# --------------------------
WRITE_LOGS = False

def flush_logs():
    """Logs disabled - do nothing"""
    global LOG_CACHE
    LOG_CACHE = []
    return


def add_log(row_number="", status="", log_type="", url="", video_id="", app_link="", message=""):
    """Logs disabled - do nothing"""
    return


# ==========================
# AUTH
# ==========================
def get_sheet():
    global SHEET_CACHE, SHEET_CACHE_TIME

    now = time.time()
    if SHEET_CACHE and (now - SHEET_CACHE_TIME < SHEET_CACHE_TTL):
        return SHEET_CACHE

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        config.CREDENTIALS_FILE,
        scope
    )

    client = gspread.authorize(creds)
    sheet = client.open_by_key(config.SPREADSHEET_ID).worksheet(config.WORKSHEET_NAME)

    SHEET_CACHE = sheet
    SHEET_CACHE_TIME = now
    return sheet


# ==========================
# QUEUE WRITES
# ==========================
def queue_update(range_str, values):
    with LOCK:
        PENDING_WRITES.append({
            "range": range_str,
            "values": values
        })


def queue_log(row_number="", status="", log_type="", url="", message=""):
    with LOCK:
        LOG_BUFFER.append({
            "time": datetime.now().isoformat(),
            "row_number": row_number,
            "status": status,
            "log_type": log_type,
            "url": url,
            "message": message
        })


# ==========================
# FLUSH WRITES
# ==========================
def flush_writes():
    global PENDING_WRITES

    with LOCK:
        if not PENDING_WRITES:
            return

        sheet = get_sheet()
        batch = PENDING_WRITES
        PENDING_WRITES = []

    try:
        print(f"🚀 FLUSH WRITES: {len(batch)} updates")
        sheet.batch_update(batch)
    except Exception as e:
        print("⚠ Write flush failed:", e)


# ==========================
# FLUSH LOGS
# ==========================
def flush_logs():
    global LOG_BUFFER

    with LOCK:
        if not LOG_BUFFER:
            return

        sheet = get_sheet()
        logs = LOG_BUFFER
        LOG_BUFFER = []

    rows = []
    for l in logs:
        rows.append([
            l["time"],
            l["status"],
            l["log_type"],
            l["row_number"],
            l["url"],
            l["message"]
        ])

    try:
        print(f"📝 FLUSH LOGS: {len(rows)} logs")
        sheet.append_rows(rows, value_input_option="RAW")
    except Exception as e:
        print("⚠ Log flush failed:", e)


# ==========================
# AUTO BACKGROUND FLUSH
# ==========================
def _background_flusher():
    while True:
        try:
            flush_writes()
            flush_logs()
        except Exception as e:
            print("⚠ Background flush error:", e)

        time.sleep(FLUSH_INTERVAL)


def start_background_flush():
    t = threading.Thread(target=_background_flusher, daemon=True)
    t.start()
    print("🟢 Background flush started")


# ==========================
# SAFE HELPERS
# ==========================
def update_combined_row(row_index, data):
    queue_update(f"A{row_index}:G{row_index}", [data])


def update_headline_and_description(row_index, headline, description):
    queue_update(f"M{row_index}:N{row_index}", [[headline, description]])


def mark_agent_done(row_num):
    queue_update(f"L{row_num}", [["DONE"]])


# ==========================
# OPTIONAL MANUAL FLUSH
# ==========================
def flush_all():
    flush_writes()
    flush_logs()
