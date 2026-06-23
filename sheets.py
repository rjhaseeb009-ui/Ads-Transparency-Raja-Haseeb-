import gspread
from oauth2client.service_account import ServiceAccountCredentials
import config
import time
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

BATCH_FLUSH_INTERVAL = 10  # seconds
LAST_FLUSH_TIME = time.time()

CLAIM_AGENT_COL = 9
CLAIM_TIME_COL = 10
CLAIM_TOKEN_COL = 11
CLAIM_STATUS_COL = 12
CLAIM_TTL_MINUTES = 5

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
# LOGGING (BATCHED)
# ==========================
def add_log(*args, **kwargs):
    """STORE ONLY (no API call)"""
    LOG_BUFFER.append({
        "args": args,
        "kwargs": kwargs,
        "time": datetime.now().isoformat()
    })


def flush_logs():
    """Flush logs in batch"""
    global LOG_BUFFER

    if not LOG_BUFFER:
        return

    sheet = get_sheet()

    rows = []
    for log in LOG_BUFFER:
        kw = log["kwargs"]
        rows.append([
            log["time"],
            kw.get("status", ""),
            kw.get("log_type", ""),
            kw.get("row_number", ""),
            kw.get("url", ""),
            kw.get("message", "")
        ])

    try:
        sheet.append_rows(rows, value_input_option="RAW")
    except Exception as e:
        print("⚠ Log flush failed:", e)

    LOG_BUFFER = []


# ==========================
# BATCH WRITE ENGINE
# ==========================
def queue_update(range_str, values):
    PENDING_WRITES.append({
        "range": range_str,
        "values": values
    })


def flush_writes():
    """Single batch API call for ALL updates"""
    global PENDING_WRITES

    if not PENDING_WRITES:
        return

    sheet = get_sheet()

    try:
        sheet.batch_update(PENDING_WRITES)
    except Exception as e:
        print("⚠ Batch write failed:", e)

    PENDING_WRITES = []


def auto_flush():
    """Call this frequently inside loop"""
    global LAST_FLUSH_TIME

    now = time.time()
    if now - LAST_FLUSH_TIME > BATCH_FLUSH_INTERVAL:
        flush_writes()
        flush_logs()
        LAST_FLUSH_TIME = now


# ==========================
# SHEET SNAPSHOT (OPTIMIZED)
# ==========================
def get_agent_rows_snapshot():
    sheet = get_sheet()
    values = sheet.get_all_values()

    rows = []
    for idx in range(1, len(values)):
        row = values[idx]
        row_num = idx + 1

        url = row[7].strip() if len(row) > 7 else ""
        video_id = row[5].strip() if len(row) > 5 else ""

        rows.append({
            "row_num": row_num,
            "url": url,
            "video_id": video_id,
            "processed": bool(video_id)
        })

    return rows


# ==========================
# CLAIM SYSTEM (NO WRITES)
# ==========================
IN_MEMORY_CLAIMS = {}


def get_next_agent_task(direction, agent_name, run_id):
    sheet = get_sheet()
    rows = get_agent_rows_snapshot()

    unprocessed = [r for r in rows if r["url"] and not r["processed"]]

    if not unprocessed:
        return None

    candidates = sorted(
        unprocessed,
        key=lambda x: x["row_num"],
        reverse=(direction == "bottom")
    )

    for r in candidates:
        row_num = r["row_num"]
        url = r["url"]

        # already claimed in memory
        if row_num in IN_MEMORY_CLAIMS:
            continue

        token = f"{agent_name}-{run_id}-{uuid.uuid4().hex[:8]}"

        IN_MEMORY_CLAIMS[row_num] = {
            "token": token,
            "agent": agent_name,
            "url": url
        }

        return row_num, url

    return None


# ==========================
# COMMIT CLAIM (BATCHED)
# ==========================
def commit_claim(row_num, agent_name):
    claim = IN_MEMORY_CLAIMS.get(row_num)
    if not claim:
        return

    queue_update(
        f"I{row_num}:L{row_num}",
        [[
            agent_name,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            claim["token"],
            "CLAIMED"
        ]]
    )


# ==========================
# MARK DONE (BATCHED)
# ==========================
def mark_agent_done(row_num, agent_name):
    queue_update(
        f"L{row_num}",
        [["DONE"]]
    )


# ==========================
# SAFE UPDATE HELPERS
# ==========================
def update_combined_row(row_index, data):
    queue_update(f"A{row_index}:G{row_index}", [data])


def update_headline_and_description(row_index, headline, description):
    queue_update(f"M{row_index}:N{row_index}", [[headline, description]])


# ==========================
# GLOBAL FLUSH (CALL THIS)
# ==========================
def flush_all():
    flush_writes()
    flush_logs()
