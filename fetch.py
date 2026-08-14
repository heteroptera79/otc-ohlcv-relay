"""Snapshot OHLCV from broker for a fixed pair set and write to data/*.jsonl."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent))
from po_client.client import AsyncPocketOptionClient  # noqa: E402

PAIRS: list[tuple[str, str]] = [
    ("EUR/USD", "EURUSD"),
    ("GBP/USD", "GBPUSD"),
    ("USD/JPY", "USDJPY"),
    ("USD/CHF", "USDCHF"),
    ("USD/CAD", "USDCAD"),
    ("AUD/USD", "AUDUSD"),
    ("EUR/GBP", "EURGBP"),
    ("EUR/JPY", "EURJPY"),
    ("GBP/JPY", "GBPJPY"),
    ("XAU/USD", "XAUUSD"),
    ("NZD/USD", "NZDUSD"),
]

INTERVALS: list[tuple[str, int, int]] = [
    ("1min", 60, 120),
    ("5min", 300, 80),
]

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CONNECT_TIMEOUT_S = 20.0
FETCH_TIMEOUT_S = 15.0
POST_SUBSCRIBE_WAIT_S = 3.0
INTER_FETCH_SLEEP_S = 0.2
TOTAL_FETCH_BUDGET_S = 180.0
MAX_CONSECUTIVE_TIMEOUTS = 4


def _to_asset(alias: str) -> str:
    return f"{alias}_otc"


def _bars_to_records(candles) -> list[dict]:
    out: list[dict] = []
    for c in candles or []:
        ts = getattr(c, "time", None) or getattr(c, "timestamp", None)
        if ts is None:
            continue
        if isinstance(ts, datetime):
            ts_iso = ts.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        else:
            ts_iso = str(ts)
        try:
            out.append({
                "ts": ts_iso,
                "o": float(c.open),
                "h": float(c.high),
                "l": float(c.low),
                "c": float(c.close),
            })
        except (AttributeError, TypeError, ValueError) as e:
            logger.warning(f"skip malformed candle: {e}")
    return out


async def _subscribe_all(client, assets: list[str]) -> int:
    sent = 0
    for asset in assets:
        for _label, period_s, _bars in INTERVALS:
            msg = f'42{json.dumps(["changeSymbol", {"asset": asset, "period": period_s}])}'
            try:
                ok = await client.send_message(msg)
                if ok:
                    sent += 1
            except Exception as e:
                logger.warning(f"subscribe failed {asset} p={period_s}: {e}")
    logger.info(f"subscribed: {sent}/{len(assets) * len(INTERVALS)} tuples")
    return sent


async def _fetch_one(client, asset: str, period: int, bars: int) -> tuple[list[dict], float]:
    t0 = time.monotonic()
    try:
        result = await asyncio.wait_for(
            client.get_candles(asset, timeframe=period, count=bars),
            timeout=FETCH_TIMEOUT_S,
        )
        return _bars_to_records(result), time.monotonic() - t0
    except asyncio.TimeoutError:
        return [], time.monotonic() - t0
    except Exception as e:
        logger.warning(f"fetch error: {asset} p={period}: {e}")
        return [], time.monotonic() - t0


async def main() -> int:
    ssid = os.environ.get("PO_SSID", "").strip()
    if not ssid:
        logger.error("PO_SSID env var missing")
        return 2

    started = time.monotonic()
    client = AsyncPocketOptionClient(ssid=ssid, is_demo=False)

    try:
        ok = await asyncio.wait_for(client.connect(), timeout=CONNECT_TIMEOUT_S)
    except asyncio.TimeoutError:
        logger.error(f"connect timeout ({CONNECT_TIMEOUT_S}s)")
        return 3
    if not ok:
        logger.error("connect returned False")
        return 4

    logger.info(f"connected in {time.monotonic() - started:.1f}s (uid={getattr(client, 'uid', '?')})")

    assets = [_to_asset(alias) for _display, alias in PAIRS]
    await _subscribe_all(client, assets)

    logger.info(f"waiting {POST_SUBSCRIBE_WAIT_S}s for PO to open asset sessions...")
    await asyncio.sleep(POST_SUBSCRIBE_WAIT_S)

    fetch_started = time.monotonic()
    written = 0
    failed = 0
    consecutive_timeouts = 0
    aborted = False

    for _display, alias in PAIRS:
        if aborted:
            break
        asset = _to_asset(alias)
        for label, period_s, bars in INTERVALS:
            elapsed_fetch = time.monotonic() - fetch_started
            if elapsed_fetch > TOTAL_FETCH_BUDGET_S:
                logger.warning(f"total fetch budget {TOTAL_FETCH_BUDGET_S}s exceeded — stopping")
                aborted = True
                break
            records, took = await _fetch_one(client, asset, period_s, bars)
            if not records:
                failed += 1
                consecutive_timeouts += 1
                logger.warning(f"empty: {alias}_{label} took={took:.1f}s (streak={consecutive_timeouts})")
                if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                    logger.error(f"{MAX_CONSECUTIVE_TIMEOUTS} consecutive empty — aborting cycle")
                    aborted = True
                    break
                await asyncio.sleep(INTER_FETCH_SLEEP_S)
                continue
            consecutive_timeouts = 0
            snapshot = {
                "asset": alias,
                "interval": label,
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
                "bars": records,
            }
            out_path = DATA_DIR / f"{alias}_{label}.jsonl"
            out_path.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
            written += 1
            logger.info(f"wrote {alias}_{label}: {len(records)} bars took={took:.1f}s")
            await asyncio.sleep(INTER_FETCH_SLEEP_S)

    try:
        await asyncio.wait_for(client.disconnect(), timeout=5.0)
    except Exception:
        pass

    elapsed = time.monotonic() - started
    logger.info(f"done in {elapsed:.1f}s | written={written} failed={failed} aborted={aborted}")
    return 0 if written else 5


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
