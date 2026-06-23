import gspread
from oauth2client.service_account import ServiceAccountCredentials
import config
import time
from datetime import datetime, timedelta
import uuid

# ==========================
# CACHE CONFIG - OPTIMIZED FOR 2 AGENTS
# ==========================
SHEET_CACHE = None
SHEET_CACHE_TIME = 0
SHEET_CACHE_TTL = 60  # Increased from 60 (auth cache)

SNAPSHOT_CACHE = None
SNAPSHOT_TIME = 0
SNAPSHOT_TTL = 45  # INCREASED from 10 - agents run slow enough to tolerate 45s delay

# Rate limiting for writes
LAST_WRITE_TIME = 0
MIN_WRITE_INTERVAL = 1.0  # 1 second minimum between sheet writes

# ==========================
# COLUMNS
# ==========================
CLAIM_AGENT_COL = 9
CLAIM_TIME_COL = 10
CLAIM_TOKEN_COL = 11
CLAIM_STATUS_COL = 12
CLAIM_TTL_MINUTES = 5

LOG_CACHE = []
WRITE_LOGS = False


# ==========================
# SHEET AUTH
# ==========================
def get_sheet():
    global SHEET_CACHE, SHEET_CACHE_TIME

    now = time.time()
    if SHEET_CACHE and (now - SHEET_CACHE_TIME) < SHEET_CACHE_TTL:
        return SHEET_CACHE

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        config.CREDENTIALS_FILE, scope
    )

    client = gspread.authorize(creds)
    sheet = client.open_by_key(config.SPREADSHEET_ID).worksheet(config.WORKSHEET_NAME)

    SHEET_CACHE = sheet
    SHEET_CACHE_TIME = now
    return sheet


# ==========================
# RATE LIMITING
# ==========================
def rate_limit_write():
    """Enforce minimum interval between sheet writes to avoid quota hits"""
    global LAST_WRITE_TIME
    elapsed = time.time() - LAST_WRITE_TIME
    if elapsed < MIN_WRITE_INTERVAL:
        sleep_time = MIN_WRITE_INTERVAL - elapsed
        time.sleep(sleep_time)
    LAST_WRITE_TIME = time.time()


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
# SNAPSHOT (CRITICAL OPTIMIZATION)
# ==========================
def get_agent_rows_snapshot():
    """
    ONE FULL READ ONLY (cached for 45 seconds to minimize API calls)
    """
    global SNAPSHOT_CACHE, SNAPSHOT_TIME

    now = time.time()
    if SNAPSHOT_CACHE and (now - SNAPSHOT_TIME) < SNAPSHOT_TTL:
        return SNAPSHOT_CACHE

    sheet = get_sheet()

    for attempt in range(5):
        try:
            values = sheet.get_all_values()
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                wait = 2 * (attempt + 1)
                print(f"⚠ 429 hit on snapshot read, retrying in {wait}s")
                time.sleep(wait)
            else:
                raise
    else:
        raise Exception("Failed to read sheet after retries")

    rows = []

    for idx in range(1, len(values)):
        row = values[idx]
        row_num = idx + 1

        url = row[7].strip() if len(row) > 7 else ""
        video_id = row[5].strip() if len(row) > 5 else ""

        claim_agent = row[8].strip() if len(row) > 8 else ""
        claim_time = row[9].strip() if len(row) > 9 else ""
        claim_token = row[10].strip() if len(row) > 10 else ""
        claim_status = row[11].strip() if len(row) > 11 else ""
        stop_flag = row[12].strip() if len(row) > 12 else ""

        rows.append({
            "row_num": row_num,
            "url": url,
            "video_id": video_id,
            "claim_agent": claim_agent,
            "claim_time": claim_time,
            "claim_token": claim_token,
            "claim_status": claim_status,
            "stop_flag": stop_flag,
            "processed": bool(video_id.strip()),
            "claim_expired": is_claim_expired(claim_time)
        })

    SNAPSHOT_CACHE = rows
    SNAPSHOT_TIME = now

    return rows


# ==========================
# HELPERS
# ==========================
def is_claim_expired(claim_time_text):
    if not claim_time_text:
        return True
    try:
        t = datetime.strptime(claim_time_text, "%Y-%m-%d %H:%M:%S")
        return datetime.now() - t > timedelta(minutes=CLAIM_TTL_MINUTES)
    except:
        return True


# ==========================
# CORE TASK PICKER (FIXED WITH WRITE RETRY + RATE LIMIT)
# ==========================
def get_next_agent_task(direction, agent_name, run_id):
    direction = direction.lower().strip()

    if direction not in ["top", "bottom"]:
        raise ValueError("direction must be top or bottom")

    sheet = get_sheet()
    rows = get_agent_rows_snapshot()

    unprocessed = [r for r in rows if r["url"] and not r["processed"]]

    if not unprocessed:
        return None

    # collision protection
    if len(unprocessed) == 1 and direction == "bottom":
        return "COLLISION_STOP"

    candidates = sorted(
        unprocessed,
        key=lambda x: x["row_num"],
        reverse=(direction == "bottom")
    )

    for c in candidates:
        row_num = c["row_num"]

        if c["stop_flag"].upper() == "STOP":
            return "COLLISION_STOP"

        # skip active claims
        if c["claim_agent"] and c["claim_agent"] != agent_name and not c["claim_expired"]:
            continue

        token = f"{agent_name}-{run_id}-{uuid.uuid4().hex[:10]}"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # APPLY RATE LIMIT BEFORE WRITING
        rate_limit_write()

        # SINGLE WRITE ONLY (claim row) WITH RETRY LOGIC
        for attempt in range(5):
            try:
                sheet.update(
                    f"I{row_num}:L{row_num}",
                    [[agent_name, now, token, "CLAIMED"]]
                )
                break  # Success - exit retry loop
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    wait = 2 * (attempt + 1)
                    print(f"⚠ 429 hit on claim write, retrying in {wait}s")
                    time.sleep(wait)
                else:
                    raise
        else:
            # Failed after all retries - skip this row and try next
            print(f"❌ Failed to claim row {row_num} after retries")
            continue

        return row_num, c["url"]

    return None


# ==========================
# BATCH UPDATE (COMBINED WRITES)
# ==========================
def update_row_batch(row_num, combined_data=None, headline=None, description=None, status=None):
    """
    Batches multiple updates into a single API call to reduce quota usage.
    
    Args:
        row_num: Row number to update
        combined_data: List for columns A-G (combined row)
        headline: Value for column M
        description: Value for column N
        status: Value for column L (CLAIM_STATUS_COL)
    """
    sheet = get_sheet()
    
    # Build batch update requests
    updates = []
    
    if combined_data:
        updates.append({
            'range': f"A{row_num}:G{row_num}",
            'values': [combined_data]
        })
    
    if headline is not None or description is not None:
        headline_val = headline if headline is not None else ""
        description_val = description if description is not None else ""
        updates.append({
            'range': f"M{row_num}:N{row_num}",
            'values': [[headline_val, description_val]]
        })
    
    if status:
        updates.append({
            'range': f"L{row_num}",
            'values': [[status]]
        })
    
    if not updates:
        return
    
    # Rate limit before batch write
    rate_limit_write()
    
    # Execute batch update with retry
    for attempt in range(3):
        try:
            if len(updates) == 1:
                # Single update
                u = updates[0]
                sheet.update(u['range'], u['values'])
            else:
                # Use batch_update for multiple ranges
                sheet.batch_update(updates)
            return
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < 2:
                wait = 2 * (attempt + 1)
                print(f"⚠ 429 hit on batch update, retrying in {wait}s")
                time.sleep(wait)
            else:
                print(f"❌ Batch update failed: {e}")
                return


# ==========================
# SIMPLE STATUS UPDATE WITH RETRY
# ==========================
def mark_agent_done(row_num, agent_name=None):
    """Mark a row as DONE"""
    rate_limit_write()
    
    sheet = get_sheet()
    for attempt in range(3):
        try:
            sheet.update_cell(row_num, CLAIM_STATUS_COL, "DONE")
            return
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < 2:
                wait = 2 * (attempt + 1)
                print(f"⚠ 429 hit on mark_done, retrying in {wait}s")
                time.sleep(wait)
            else:
                print(f"❌ Failed to mark row {row_num} as DONE: {e}")
                return


# ==========================
# LEGACY BULK UPDATE HELPERS (use update_row_batch instead)
# ==========================
def update_combined_row(row_index, data):
    """LEGACY: Use update_row_batch() instead"""
    update_row_batch(row_index, combined_data=data)


def update_headline_and_description(row_index, headline, description):
    """LEGACY: Use update_row_batch() instead"""
    update_row_batch(row_index, headline=headline, description=description)


# ==========================
# OPTIMIZED URL FETCH (NO EXTRA SNAPSHOT CALL)
# ==========================
def get_urls_with_retry():
    rows = get_agent_rows_snapshot()
    return [r["url"] for r in rows if r["url"]]


# ==========================
# OPTIONAL UTILS
# ==========================
def count_unprocessed_rows():
    rows = get_agent_rows_snapshot()
    return sum(1 for r in rows if r["url"] and not r["processed"])


def invalidate_snapshot():
    """Force snapshot cache to refresh on next call (use sparingly)"""
    global SNAPSHOT_CACHE, SNAPSHOT_TIME
    SNAPSHOT_CACHE = None
    SNAPSHOT_TIME = 0
