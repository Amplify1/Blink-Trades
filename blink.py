"""
Blink Trades Bot — OT-driven swing bot
Strategy: multi-timeframe OT alignment
Instruments: SPY + QQQ (long AND short)
Signal source: Omegavus Trend API (heatMapValue per timeframe)
Confirmation: VIX correlation + ES/NQ futures alignment
Session: 10:00 AM - 3:20 PM ET, weekdays only
Calendar: blocks 30 min before/after high-impact economic events
"""

import os
import math
import time
import logging
from datetime import datetime, timedelta, time as dtime

import pytz
import requests
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

load_dotenv()

# ── CREDENTIALS ──────────────────────────────────────────────────────────
ALPACA_KEY    = os.environ["ALPACA_KEY"]
ALPACA_SECRET = os.environ["ALPACA_SECRET"]
OT_TOKEN      = os.environ["OT_TOKEN"]

# ── PAPER vs LIVE ────────────────────────────────────────────────────────
# Set to False only when paper results are consistently profitable.
# Flipping this is the only change needed to go live.
PAPER_TRADING = True

# ── CONFIG ───────────────────────────────────────────────────────────────
SYMBOLS        = ["SPY", "QQQ"]
CONFIRM_TFS    = ["M15", "M30", "M60", "M240", "D1"]  # all must align
VIX_TFS        = ["M30", "M60", "M240"]               # VIX correlation check
CONFIRM_THRESH = 150       # +-150 OT alignment threshold
RISK_PCT       = 0.02      # 2% account equity per trade
SCAN_SECS      = 300       # 5-minute scan cycle
SESSION_START  = dtime(10, 0)
SESSION_END    = dtime(15, 20)   # close all by 3:20, flat before EOD
ET             = pytz.timezone("America/New_York")
NEWS_BUFFER    = 30        # minutes to block before/after high-impact events

# Futures proxies
FUTURES_MAP = {"SPY": "ES", "QQQ": "NQ"}

# ── LOGGING ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler("blink_trades_log.txt"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── OT CLIENT ────────────────────────────────────────────────────────────
OT_BASE = "https://app.omegavustrend.com"

def ot_headers():
    return {
        "Authorization": f"Bearer {OT_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

def fetch_ot():
    r = requests.get(f"{OT_BASE}/api/clientUser/getSubscriptions",
                     headers=ot_headers(), timeout=10)
    if r.status_code == 401:
        raise RuntimeError(
            "OT token expired. Log into app.omegavustrend.com -> "
            "DevTools -> Network -> getSubscriptions -> Headers -> "
            "copy Authorization Bearer value into .env OT_TOKEN"
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

# ── ECONOMIC CALENDAR ─────────────────────────────────────────────────────
# Fetches today's high-impact events from OT platform.
# Blocks new entries NEWS_BUFFER minutes before and after each event.
# Existing positions are held through events — only new entries blocked.

def fetch_calendar():
    """Returns list of high-impact event times (ET) for today."""
    now_et  = datetime.now(ET)
    day_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now_et.replace(hour=23, minute=59, second=59, microsecond=0)

    from_iso = day_start.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_iso   = day_end.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    try:
        r = requests.get(
            f"{OT_BASE}/api/getEconomicCalendarItems",
            params={"from": from_iso, "to": to_iso},
            headers=ot_headers(),
            timeout=10
        )
        r.raise_for_status()
        events = r.json()
        high_impact = []
        for e in events:
            impact = str(e.get("impact", "")).lower()
            if "high" in impact or e.get("impactLevel", 0) >= 3:
                event_time_str = e.get("date") or e.get("time") or e.get("eventTime")
                if event_time_str:
                    try:
                        event_dt = datetime.fromisoformat(
                            event_time_str.replace("Z", "+00:00")
                        ).astimezone(ET)
                        high_impact.append(event_dt)
                        log.info(f"CALENDAR  high-impact event: {e.get('name', 'unknown')} at {event_dt.strftime('%H:%M ET')}")
                    except Exception:
                        pass
        return high_impact
    except Exception as e:
        log.warning(f"calendar fetch failed: {e} — proceeding without calendar gate")
        return []

def calendar_ok(high_impact_events):
    """Returns False if within NEWS_BUFFER minutes of any high-impact event."""
    now_et = datetime.now(ET)
    buffer = timedelta(minutes=NEWS_BUFFER)
    for event_dt in high_impact_events:
        if abs(now_et - event_dt) <= buffer:
            log.info(f"CALENDAR  blocked — within {NEWS_BUFFER}min of high-impact event at {event_dt.strftime('%H:%M ET')}")
            return False
    return True

# ── SIGNAL ENGINE ─────────────────────────────────────────────────────────
def get_signal(ot, sym):
    """
    Returns 'LONG', 'SHORT', or None.
    Requires ALL confirmation timeframes past +-CONFIRM_THRESH.
    Full alignment required — partial = no trade (NTZ).
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
    VIX must trend OPPOSITE to trade direction.
    SHORT = VIX rising (positive heatmap).
    LONG  = VIX falling (negative heatmap).
    """
    if "VIX" not in ot:
        return False
    vals = [ot["VIX"]["tf"].get(tf, 0) for tf in VIX_TFS]
    if direction == "SHORT":
        return all(v > 0 for v in vals)
    return all(v < 0 for v in vals)

def futures_confirms(ot, sym, direction):
    """
    ES must agree with SPY. NQ must agree with QQQ.
    Passes through if futures data unavailable.
    """
    fut = FUTURES_MAP.get(sym)
    if not fut or fut not in ot:
        return True
    return get_signal(ot, fut) == direction

# ── SESSION FILTER ────────────────────────────────────────────────────────
def in_session():
    """10:00 AM - 3:20 PM ET, weekdays only."""
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    return SESSION_START <= now.time() <= SESSION_END

def near_session_end():
    """True after 3:15 PM — close all positions, go flat before EOD."""
    now = datetime.now(ET)
    return now.time() >= dtime(15, 15)

# ── ALPACA EXECUTION ──────────────────────────────────────────────────────
alpaca = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=PAPER_TRADING)

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

def close_all():
    """Close every open position — called at session end."""
    for sym in list(live.keys()):
        close_position(sym)
        live.pop(sym, None)
    log.info("EOD  all positions closed")

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
    """Shorts use integer qty — Alpaca rejects notional on short orders."""
    qty = math.floor((equity * RISK_PCT) / price)
    if qty < 1:
        log.warning(f"SHORT {sym}: qty=0 at ${price:.2f}, skipping")
        return
    req = MarketOrderRequest(
        symbol=sym,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY
    )
    alpaca.submit_order(req)
    log.info(f"SHORT  {sym}  qty={qty}  price~${price:.2f}")

# ── MAIN LOOP ─────────────────────────────────────────────────────────────
live = {}   # {sym: "LONG" | "SHORT"}
eod_closed = False

def run():
    global eod_closed

    mode = "PAPER" if PAPER_TRADING else "LIVE"
    log.info("=" * 60)
    log.info(f"Blink Trades Bot started  [{mode}]")
    log.info(f"symbols: {SYMBOLS}")
    log.info(f"confirm timeframes: {CONFIRM_TFS}  threshold: +-{CONFIRM_THRESH}")
    log.info(f"session: {SESSION_START} - {SESSION_END} ET")
    log.info(f"news buffer: +-{NEWS_BUFFER} min around high-impact events")
    log.info(f"scan: every {SCAN_SECS}s")
    log.info("=" * 60)

    # Fetch today's high-impact events once at startup
    high_impact_events = fetch_calendar()
    calendar_day = datetime.now(ET).date()

    while True:
        try:
            now_et = datetime.now(ET)

            # Refresh calendar if day rolled over
            if now_et.date() != calendar_day:
                high_impact_events = fetch_calendar()
                calendar_day = now_et.date()
                eod_closed = False

            # ── EOD close ────────────────────────────────────────────────
            if near_session_end() and live and not eod_closed:
                log.info("EOD  approaching session end — closing all positions")
                close_all()
                eod_closed = True

            if not in_session():
                log.info(f"outside session ({now_et.strftime('%H:%M %Z')}) — sleeping 60s")
                time.sleep(60)
                continue

            # ── Fetch OT data ─────────────────────────────────────────────
            ot     = parse_ot(fetch_ot())
            equity = get_equity()
            cal_ok = calendar_ok(high_impact_events)
            log.info(f"equity=${equity:,.2f}  calendar={'ok' if cal_ok else 'BLOCKED'}")

            # ── Evaluate each symbol ──────────────────────────────────────
            for sym in SYMBOLS:
                signal   = get_signal(ot, sym)
                pos_now  = live.get(sym)
                pos_live = get_open_position(sym)
                agg      = ot.get(sym, {}).get("aggregate", 0)
                price    = ot.get(sym, {}).get("price", 0)

                # Sync if position was manually closed
                if pos_live is None and pos_now:
                    log.info(f"{sym}  closed externally — clearing state")
                    live.pop(sym, None)
                    pos_now = None

                log.info(
                    f"{sym}  agg={agg:+.0f}  signal={signal}  "
                    f"pos={pos_now}  price=${price:.2f}"
                )

                # ── EXIT ─────────────────────────────────────────────────
                # Exit when OT alignment breaks — signal changed or went neutral.
                if pos_now and pos_live:
                    if signal != pos_now:
                        log.info(f"{sym}  alignment lost ({pos_now} -> {signal}) — exiting")
                        close_position(sym)
                        live.pop(sym, None)
                        continue

                # ── ENTRY ─────────────────────────────────────────────────
                # All 5 gates must be green: OT aligned + VIX + futures + session + calendar
                if signal and not pos_now and cal_ok:
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
