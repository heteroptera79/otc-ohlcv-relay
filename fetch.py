"""Snapshot OHLCV from broker for a fixed pair set and write to data/*.jsonl.
# trigger v1

Runs inside a GitHub Actions job. Reads BROKER_SSID from env, connects once,
pulls M1 and M5 candles per pair, writes one JSON snapshot per (pair, interval)
into data/<PAIR>_<INTERVAL>.jsonl (overwrite — always latest N bars).

Contract kept intentionally narrow: consumer downloads raw file via HTTPS and
parses it. No auth on read side (public repo).
"""
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

# ── pair set — MUST match services/signal_engine/config.py in the private repo.
# 11 forex OTC pairs the engine actually trades. Keep identical order & names.
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
    # (label, period_seconds, bars) — matches engine.cfg (M1×100 + M5×60)
    ("1min", 60, 120),   # +20 buffer over engine's fetch_bars=100
    ("5min", 300, 80),   # +20 buffer over engine's mtf_bars=60
]

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CONNECT_TIMEOUT_S = 20.0
FETCH_TIMEOUT_S = 25.0


def _to_asset(alias: str) -> str:
    """EURUSD -> EURUSD_otc (OTC-only, matches po_fetcher._to_asset contract)."""
    return f"{alias}_otc"


def _bars_to_records(candles) -> list[dict]:
    """Normalize whatever chipa returns into [{ts, open, high, low, close}, ...]."""
    out: list[dict] = []
    for c in candles or []:
        # chipa Candle model: .time (datetime), .open, .high, .low, .close
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


async def _fetch_one(client, asset: str, period: int, bars: int) -> list[dict]:
    try:
        result = await asyncio.wait_for(
            client.get_candles(asset, timeframe=period, count=bars),
            timeout=FETCH_TIMEOUT_S,
        )
        return _bars_to_records(result)
    except asyncio.TimeoutError:
        logger.warning(f"fetch timeout: {asset} p={period} bars={bars}")
        return []
    except Exception as e:
        logger.warning(f"fetch failed: {asset} p={period}: {e}")
        return []


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

    logger.info(f"connected in {time.monotonic() - started:.1f}s")

    written = 0
    failed = 0
    for _display, alias in PAIRS:
        asset = _to_asset(alias)
        for label, period_s, bars in INTERVALS:
            records = await _fetch_one(client, asset, period_s, bars)
            if not records:
                failed += 1
                continue
            snapshot = {
                "asset": alias,
                "interval": label,
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
                "bars": records,
            }
            out_path = DATA_DIR / f"{alias}_{label}.jsonl"
            out_path.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
            written += 1

    try:
        await asyncio.wait_for(client.disconnect(), timeout=5.0)
    except Exception:
        pass

    elapsed = time.monotonic() - started
    logger.info(f"done in {elapsed:.1f}s | written={written} failed={failed}")
    return 0 if written else 5


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
