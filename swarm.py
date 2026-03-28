"""
swarm v8 — OT-driven swing bot
Strategy: Carlos / Omegavus Trend framework
Instruments: SPY + QQQ (long AND short)
Signal source: Omegavus Trend API (heatMapValue per timeframe)
Confirmation: VIX correlation + ES/NQ futures alignment
Session: 10:00 AM - 3:30 PM ET, weekdays only
"""

import os
import math
import time
import logging
from datetime import datetime, time as dtime

import pytz
import requests
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

load_dotenv()

# ── CREDENTIALS ─────────────────────────────────────────────────────────
ALPACA_KEY    = os.environ["ALPACA_KEY"]
ALPACA_SECRET = os.environ["ALPACA_SECRET"]
OT_TOKEN      = os.environ["OT_TOKEN"]

# ── CONFIG ───────────────────────────────────────────────────────────────
SYMBOLS        = ["SPY", "QQQ"]
CONFIRM_TFS    = ["M15", "M30", "M60", "M240", "D1"]   # timeframes that must align
VIX_TFS        = ["M30", "M60", "M240"]                 # VIX timeframes to check
CONFIRM_THRESH = 150        # +-150 — Carlos's alertsSettings threshold
RISK_PCT       = 0.02       # 2% account equity per trade
SCAN_SECS      = 300        # 5-minute scan cycle
SESSION_START  = dtime(10, 0)
SESSION_END    = dtime(15, 30)
ET             = pytz.timezone("America/New_York")

# Futures proxies — NQ confirms QQQ direction, ES confirms SPY
FUTURES_MAP = {"SPY": "ES", "QQQ": "NQ"}

# ── LOGGING ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler("swarm_v8_log.txt"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── OT CLIENT ────────────────────────────────────────────────────────────
OT_URL = "https://app.omegavustrend.com/api/clientUser/getSubscriptions"

def fetch_ot():
    headers = {
        "Authorization": f"Bearer {OT_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    r = requests.get(OT_URL, headers=headers, timeout=10)
    if r.status_code == 401:
        raise RuntimeError(
            "OT token expired. Log into app.omegavustrend.com, "
            "DevTools -> Network -> getSubscriptions -> Headers -> "
            "copy the Authorization Bearer value into .env OT_TOKEN"
        )
    r.raise_for_status()
    return r.json()

def parse_ot(raw):
    """Returns {name: {price, aggregate, tf: {key: heatMapValue}}}"""
    out = {}
    for group in raw:
        for inst in group["instruments"]:
            out[inst["name"]] = {
                "price":     inst["lastPrice"],
                "aggregate": inst["average"],
                "tf":        {i["key"]: i["heatMapValue"] for i in inst["indicators"]}
            }
    return out

# ── SIGNAL ENGINE ────────────────────────────────────────────────────────
def get_signal(ot, sym):
    """
    Returns 'LONG', 'SHORT', or None.
    Requires ALL confirmation timeframes past +-CONFIRM_THRESH.
    This is Carlos's full-alignment rule — partial = no trade.
    """
    if sym not in ot:
        return None
    vals = [ot[sym]["tf"].get(tf, 0) for tf in CONFIRM_TFS]
    if all(v < -CONFIRM_THRESH for v in vals):
        return "SHORT"
    if all(v > CONFIRM_THRESH for v in vals):
        return "LONG"
    return None

def vix_confirms(ot, direction):
    """
    VIX must trend OPPOSITE to the trade direction.
    SHORT trade = VIX rising (positive heatmap on VIX).
    LONG trade  = VIX falling (negative heatmap on VIX).
    """
    if "VIX" not in ot:
        return False
    vals = [ot["VIX"]["tf"].get(tf, 0) for tf in VIX_TFS]
    if direction == "SHORT":
        return all(v > 0 for v in vals)
    return all(v < 0 for v in vals)

def futures_confirms(ot, sym, direction):
    """
    ES must agree with SPY direction.
    NQ must agree with QQQ direction.
    If futures data unavailable, passes through.
    """
    fut = FUTURES_MAP.get(sym)
    if not fut or fut not in ot:
        return True
    return get_signal(ot, fut) == direction

# ── SESSION FILTER ───────────────────────────────────────────────────────
def in_session():
    """10:00 AM - 3:30 PM ET, Monday-Friday only."""
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    return SESSION_START <= now.time() <= SESSION_END

# ── ALPACA EXECUTION ─────────────────────────────────────────────────────
alpaca = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)

def get_equity():
    return float(alpaca.get_account().equity)

def get_open_position(sym):
    try:
        return alpaca.get_open_position(sym)
    except Exception:
        return None

def close_position(sym):
    try:
        alpaca.close_position(sym)
        log.info(f"EXIT   {sym}")
    except Exception as e:
        log.error(f"close {sym} failed: {e}")

def enter_long(sym, price, equity):
    notional = round(equity * RISK_PCT, 2)
    req = MarketOrderRequest(
        symbol=sym,
        notional=notional,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY
    )
    alpaca.submit_order(req)
    log.info(f"LONG   {sym}  notional=${notional:.2f}")

def enter_short(sym, price, equity):
    """
    Shorts MUST use integer qty — Alpaca rejects notional on short orders.
    Fix for v7 bug: 162 failed SHORT orders used notional= parameter.
    """
    qty = math.floor((equity * RISK_PCT) / price)
    if qty < 1:
        log.warning(f"SHORT {sym}: qty=0 at price ${price:.2f}, skipping")
        return
    req = MarketOrderRequest(
        symbol=sym,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY
    )
    alpaca.submit_order(req)
    log.info(f"SHORT  {sym}  qty={qty}  price~${price:.2f}")

# ── MAIN LOOP ────────────────────────────────────────────────────────────
# live tracks current bot-placed positions: {sym: "LONG" | "SHORT"}
live = {}

def run():
    log.info("=" * 60)
    log.info("swarm v8 started")
    log.info(f"symbols: {SYMBOLS}")
    log.info(f"confirm timeframes: {CONFIRM_TFS}  threshold: +-{CONFIRM_THRESH}")
    log.info(f"session: {SESSION_START} - {SESSION_END} ET")
    log.info(f"scan: every {SCAN_SECS}s")
    log.info("=" * 60)

    while True:
        try:
            if not in_session():
                now_et = datetime.now(ET)
                log.info(f"outside session ({now_et.strftime('%H:%M %Z')}) — sleeping 60s")
                time.sleep(60)
                continue

            # ── Fetch OT data ────────────────────────────────────────────
            ot = parse_ot(fetch_ot())
            equity = get_equity()
            log.info(f"equity=${equity:,.2f}")

            # ── Evaluate each symbol ─────────────────────────────────────
            for sym in SYMBOLS:
                signal   = get_signal(ot, sym)
                pos_now  = live.get(sym)
                pos_live = get_open_position(sym)
                agg      = ot.get(sym, {}).get("aggregate", 0)
                price    = ot.get(sym, {}).get("price", 0)

                # Sync state if position was manually closed
                if pos_live is None and pos_now:
                    log.info(f"{sym}  position closed externally — clearing state")
                    live.pop(sym, None)
                    pos_now = None

                log.info(
                    f"{sym}  agg={agg:+.0f}  signal={signal}  "
                    f"pos={pos_now}  price=${price:.2f}"
                )

                # ── EXIT logic ───────────────────────────────────────────
                # Exit when OT alignment is lost (signal no longer matches position).
                # Covers: alignment breaks, signal flips, signal goes to None (NTZ).
                if pos_now and pos_live:
                    if signal != pos_now:
                        log.info(f"{sym}  alignment lost (was {pos_now}, now {signal}) — exiting")
                        close_position(sym)
                        live.pop(sym, None)
                        continue

                # ── ENTRY logic ──────────────────────────────────────────
                # Enter when: OT fully aligned + VIX confirms + futures confirms + no position
                if signal and not pos_now:
                    vix_ok     = vix_confirms(ot, signal)
                    futures_ok = futures_confirms(ot, sym, signal)

                    if vix_ok and futures_ok:
                        if signal == "LONG":
                            enter_long(sym, price, equity)
                        else:
                            enter_short(sym, price, equity)
                        live[sym] = signal
                    else:
                        log.info(
                            f"{sym}  {signal} blocked — "
                            f"VIX={'ok' if vix_ok else 'FAIL'}  "
                            f"futures={'ok' if futures_ok else 'FAIL'}"
                        )

        except RuntimeError as e:
            log.error(str(e))
            break
        except Exception as e:
            log.error(f"scan error: {e}")

        time.sleep(SCAN_SECS)


if __name__ == "__main__":
    run()
