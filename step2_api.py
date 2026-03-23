"""
=============================================================
  STEP 2 — Live Incremental API
  Deploy to Railway / Render (free tier) — stays online 24/7
=============================================================
  Reads from the pre-built token_transfers.db created by
  step1_bulk_fetch.py, then only fetches NEW transfers
  (blocks after last_synced_block) on a schedule.

LOCAL TEST:
    pip install fastapi uvicorn apscheduler requests
    python step2_api.py

DEPLOY FREE (Railway):
    1. Push step2_api.py + token_transfers.db + requirements.txt to GitHub
    2. Connect repo to railway.app → deploys automatically
    3. Your API is live 24/7 at a free *.railway.app URL

DEPLOY FREE (Render):
    1. Push to GitHub
    2. New Web Service on render.com → connect repo
    3. Start command: uvicorn step2_api:app --host 0.0.0.0 --port $PORT
=============================================================
"""

import requests
import sqlite3
import json
import time
import threading
import logging
import os
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN_ADDRESS  = "0x7B7047C49eAf68B8514A20624773ca620e2CD4a3"
BASE_URL       = "https://blockscout.lisk.com/api/v2"
DB_FILE        = "token_transfers.db"
FETCH_INTERVAL = 100     # hours between incremental syncs
PORT           = int(os.environ.get("PORT", 8000))
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_db():
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def ensure_tables():
    con = get_db()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS transfers (
            tx_hash TEXT, log_index INTEGER, block_number INTEGER,
            timestamp TEXT, from_address TEXT, from_label TEXT,
            to_address TEXT, to_label TEXT, amount REAL, amount_raw TEXT,
            token_symbol TEXT, token_name TEXT, token_address TEXT,
            method TEXT, tx_type TEXT, fetched_at TEXT,
            PRIMARY KEY (tx_hash, log_index)
        );
        CREATE TABLE IF NOT EXISTS token_info (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS meta      (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS fetch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT, finished_at TEXT,
            new_records INTEGER, status TEXT, error TEXT
        );
    """)
    con.commit()
    con.close()


def get_meta(key, default=None):
    con = get_db()
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    con.close()
    return row[0] if row else default


def set_meta(key, value):
    con = get_db()
    con.execute("INSERT OR REPLACE INTO meta (key,value) VALUES (?,?)", (key, str(value)))
    con.commit()
    con.close()


def format_amount(raw, decimals):
    try:
        return float(int(raw)) / (10 ** int(decimals or 18))
    except Exception:
        return None


# ── Incremental fetcher ───────────────────────────────────────────────────────

def incremental_fetch():
    """Only fetch transfers newer than the last synced block."""
    started    = datetime.now(timezone.utc).isoformat()
    new_total  = 0
    error_msg  = None

    try:
        # Token info
        r = requests.get(f"{BASE_URL}/tokens/{TOKEN_ADDRESS}", timeout=15)
        r.raise_for_status()
        info = r.json()
        token_decimals = info.get("decimals", 18)
        token_symbol   = info.get("symbol", "")

        con = get_db()
        for k, v in info.items():
            con.execute("INSERT OR REPLACE INTO token_info (key,value) VALUES (?,?)",
                        (k, json.dumps(v)))
        con.commit()

        last_block  = int(get_meta("last_synced_block") or 0)
        fetched_at  = datetime.now(timezone.utc).isoformat()
        new_highest = last_block
        page_params = None
        stop        = False

        log.info("Incremental fetch — from block %d", last_block)

        while not stop:
            url = f"{BASE_URL}/tokens/{TOKEN_ADDRESS}/transfers"
            if page_params:
                url += "?" + "&".join(f"{k}={v}" for k, v in page_params.items())

            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data  = r.json()
            items = data.get("items", [])

            for tx in items:
                block = tx.get("block_number") or 0

                # Stop paging once we hit already-synced blocks
                if block and block <= last_block:
                    stop = True
                    break

                if block and block > new_highest:
                    new_highest = block

                total    = tx.get("total") or {}
                token    = tx.get("token") or {}
                decimals = total.get("decimals") or token.get("decimals") or token_decimals
                symbol   = token.get("symbol") or token_symbol
                raw_val  = total.get("value")
                amount   = format_amount(raw_val, decimals) if raw_val else None

                ts = tx.get("timestamp")
                if ts:
                    try:
                        ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).isoformat()
                    except Exception:
                        pass

                cur = con.execute("""
                    INSERT OR IGNORE INTO transfers
                    (tx_hash, log_index, block_number, timestamp,
                     from_address, from_label, to_address, to_label,
                     amount, amount_raw, token_symbol, token_name,
                     token_address, method, tx_type, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    tx.get("transaction_hash"),
                    tx.get("log_index") or 0,
                    block,
                    ts,
                    (tx.get("from") or {}).get("hash"),
                    (tx.get("from") or {}).get("name"),
                    (tx.get("to")   or {}).get("hash"),
                    (tx.get("to")   or {}).get("name"),
                    amount,
                    str(raw_val) if raw_val else None,
                    symbol,
                    token.get("name"),
                    token.get("address") or TOKEN_ADDRESS,
                    tx.get("method"),
                    tx.get("type"),
                    fetched_at,
                ))
                if cur.rowcount:
                    new_total += 1

            con.commit()

            if stop or not data.get("next_page_params"):
                break

            page_params = data["next_page_params"]
            time.sleep(0.3)

        if new_highest > last_block:
            set_meta("last_synced_block", new_highest)

        con.close()
        log.info("Incremental fetch done — %d new records", new_total)

    except Exception as e:
        error_msg = str(e)
        log.error("Incremental fetch failed: %s", e)

    finished = datetime.now(timezone.utc).isoformat()
    con2 = get_db()
    con2.execute(
        "INSERT INTO fetch_log (started_at,finished_at,new_records,status,error) VALUES (?,?,?,?,?)",
        (started, finished, new_total, "error" if error_msg else "ok", error_msg)
    )
    con2.commit()
    con2.close()
    return new_total


# ── App startup ───────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_tables()
    bulk_done = get_meta("bulk_fetch_completed_at")
    if bulk_done:
        log.info("Bulk data present (fetched %s) — starting incremental sync", bulk_done)
    else:
        log.warning("No bulk data found. Run step1_bulk_fetch.py first for full history.")

    threading.Thread(target=incremental_fetch, daemon=True).start()
    scheduler.add_job(incremental_fetch, "interval", hours=FETCH_INTERVAL, id="inc_fetch")
    scheduler.start()
    log.info("Scheduler running — incremental sync every %dh", FETCH_INTERVAL)
    yield
    scheduler.shutdown()


app = FastAPI(
    title="Lisk Token Transfer API",
    description=f"Transfer history for `{TOKEN_ADDRESS}` on Lisk. Bulk history pre-loaded, new transfers auto-synced every {FETCH_INTERVAL}h.",
    version="2.0.0",
    lifespan=lifespan,
)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/transfers", summary="List transfers with filters")
def get_transfers(
    page:         int   = Query(1,    ge=1),
    limit:        int   = Query(50,   ge=1, le=200),
    address:      str   = Query(None, description="Match from OR to"),
    from_address: str   = Query(None),
    to_address:   str   = Query(None),
    min_amount:   float = Query(None),
    max_amount:   float = Query(None),
    date_from:    str   = Query(None, description="YYYY-MM-DD"),
    date_to:      str   = Query(None, description="YYYY-MM-DD"),
    order:        str   = Query("desc", description="asc or desc"),
):
    con   = get_db()
    where = []
    args  = []

    if address:
        where.append("(LOWER(from_address)=LOWER(?) OR LOWER(to_address)=LOWER(?))")
        args += [address, address]
    if from_address:
        where.append("LOWER(from_address)=LOWER(?)")
        args.append(from_address)
    if to_address:
        where.append("LOWER(to_address)=LOWER(?)")
        args.append(to_address)
    if min_amount is not None:
        where.append("amount >= ?")
        args.append(min_amount)
    if max_amount is not None:
        where.append("amount <= ?")
        args.append(max_amount)
    if date_from:
        where.append("timestamp >= ?")
        args.append(date_from)
    if date_to:
        where.append("timestamp <= ?")
        args.append(date_to + "T23:59:59")

    w      = ("WHERE " + " AND ".join(where)) if where else ""
    sort   = "ASC" if order.lower() == "asc" else "DESC"
    offset = (page - 1) * limit

    total = con.execute(f"SELECT COUNT(*) FROM transfers {w}", args).fetchone()[0]
    rows  = con.execute(
        f"SELECT * FROM transfers {w} ORDER BY block_number {sort} LIMIT ? OFFSET ?",
        args + [limit, offset]
    ).fetchall()
    con.close()

    return {
        "total":   total,
        "page":    page,
        "limit":   limit,
        "pages":   (total + limit - 1) // limit,
        "results": [dict(r) for r in rows],
    }


@app.get("/transfers/{tx_hash}", summary="Get transfer by tx hash")
def get_transfer(tx_hash: str):
    con  = get_db()
    rows = con.execute("SELECT * FROM transfers WHERE tx_hash=?", (tx_hash,)).fetchall()
    con.close()
    if not rows:
        raise HTTPException(404, "Transaction not found")
    return [dict(r) for r in rows]


@app.get("/stats", summary="Summary stats")
def get_stats():
    con = get_db()
    total   = con.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    volume  = con.execute("SELECT SUM(amount) FROM transfers").fetchone()[0] or 0
    latest  = con.execute("SELECT MAX(timestamp) FROM transfers").fetchone()[0]
    earliest= con.execute("SELECT MIN(timestamp) FROM transfers").fetchone()[0]
    symbol  = con.execute("SELECT value FROM token_info WHERE key='symbol'").fetchone()
    top5    = con.execute("""
        SELECT from_address, COUNT(*) cnt, SUM(amount) vol
        FROM transfers WHERE from_address IS NOT NULL
        GROUP BY from_address ORDER BY cnt DESC LIMIT 5
    """).fetchall()
    last_fetch = con.execute(
        "SELECT started_at,finished_at,new_records,status FROM fetch_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    bulk_done  = get_meta("bulk_fetch_completed_at")
    last_block = get_meta("last_synced_block")
    con.close()

    return {
        "token_address":   TOKEN_ADDRESS,
        "token_symbol":    json.loads(symbol[0]) if symbol else None,
        "total_transfers": total,
        "total_volume":    round(volume, 6),
        "earliest_tx":     earliest,
        "latest_tx":       latest,
        "last_synced_block": last_block,
        "bulk_loaded_at":  bulk_done,
        "top_senders": [
            {"address": r[0], "tx_count": r[1], "volume": round(r[2] or 0, 4)}
            for r in top5
        ],
        "last_sync": dict(last_fetch) if last_fetch else None,
        "auto_sync": f"every {FETCH_INTERVAL}h",
    }


@app.get("/token", summary="Token metadata")
def get_token():
    con  = get_db()
    rows = con.execute("SELECT key,value FROM token_info").fetchall()
    con.close()
    return {r[0]: json.loads(r[1]) for r in rows}


@app.post("/sync", summary="Trigger an incremental sync now")
def trigger_sync():
    threading.Thread(target=incremental_fetch, daemon=True).start()
    return {"message": "Incremental sync triggered"}


@app.get("/sync-log", summary="Sync job history")
def sync_log(limit: int = Query(20, ge=1, le=100)):
    con  = get_db()
    rows = con.execute("SELECT * FROM fetch_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    con.close()
    return [dict(r) for r in rows]


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def home():
    return """<!DOCTYPE html>
<html><head><title>Lisk Token API</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Courier New',monospace;background:#080c10;color:#c9d1d9;padding:2.5rem;min-height:100vh}
  h1{color:#58a6ff;font-size:1.5rem;margin-bottom:.3rem}
  .sub{color:#484f58;font-size:.8rem;margin-bottom:2rem}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem}
  .card{background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:1.25rem}
  .card h2{font-size:.7rem;color:#484f58;text-transform:uppercase;letter-spacing:1px;margin-bottom:.75rem}
  .row{display:flex;align-items:center;gap:.6rem;margin:.35rem 0}
  .get{background:#0d3349;color:#58a6ff;font-size:.65rem;padding:2px 6px;border-radius:3px;font-weight:700}
  .post{background:#2d1f0e;color:#e3b341;font-size:.65rem;padding:2px 6px;border-radius:3px;font-weight:700}
  a{color:#58a6ff;text-decoration:none;font-size:.82rem}
  a:hover{text-decoration:underline}
  .badge{background:#1a2f1a;color:#3fb950;font-size:.7rem;padding:3px 9px;border-radius:10px;margin-left:.5rem}
  .docs{margin-top:2rem;padding-top:1.5rem;border-top:1px solid #21262d}
</style></head>
<body>
  <h1>⬡ Lisk Token Transfer API</h1>
  <div class="sub">Bulk history pre-loaded &nbsp;·&nbsp; incremental sync every hour &nbsp;·&nbsp; SQLite-backed</div>
  <div class="grid">
    <div class="card">
      <h2>Transfers</h2>
      <div class="row"><span class="get">GET</span><a href="/transfers">/transfers</a><span class="badge">paginated</span></div>
      <div class="row"><span class="get">GET</span><a href="/transfers?order=asc">/transfers?order=asc</a></div>
      <div class="row"><span class="get">GET</span><span style="color:#8b949e;font-size:.8rem">/transfers?address=0x...</span></div>
      <div class="row"><span class="get">GET</span><span style="color:#8b949e;font-size:.8rem">/transfers?date_from=2024-01-01</span></div>
      <div class="row"><span class="get">GET</span><span style="color:#8b949e;font-size:.8rem">/transfers/{tx_hash}</span></div>
    </div>
    <div class="card">
      <h2>Info</h2>
      <div class="row"><span class="get">GET</span><a href="/stats">/stats</a></div>
      <div class="row"><span class="get">GET</span><a href="/token">/token</a></div>
      <div class="row"><span class="get">GET</span><a href="/sync-log">/sync-log</a></div>
    </div>
    <div class="card">
      <h2>Control</h2>
      <div class="row"><span class="post">POST</span><a href="/sync">/sync</a>&nbsp;— trigger sync now</div>
    </div>
  </div>
  <div class="docs">📖 <a href="/docs">Interactive Swagger docs →</a></div>
</body></html>"""


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  Lisk Token API — starting on port", PORT)
    print("  Docs: http://localhost:{}/docs".format(PORT))
    print("=" * 55 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
