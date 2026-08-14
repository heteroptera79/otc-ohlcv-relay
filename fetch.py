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

# tf label as string — matches production po_fetcher.py which passes "1m"/"5m"
# to chipa (not int seconds). Kept identical to avoid protocol drift.
INTERVALS: list[tuple[str, str, int, int]] = [
    # (json_label, chipa_tf, period_seconds_for_subscribe, bars)
    ("1min", "1m", 60, 120),
    ("5min", "5m", 300, 80),
]

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CONNECT_TIMEOUT_S = 25.0
FETCH_TIMEOUT_S = 20.0
WARMUP_AFTER_SUBSCRIBE_S = 30.0
INTER_FETCH_SLEEP_S = 0.3
TOTAL_FETCH_BUDGET_S = 240.0
MAX_CONSECUTIVE_FAILURES = 6


def _to_asset(alias: str) -> str:
    return f"{alias}_otc"


def _candles_to_records(candles) -> list[dict]:
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
    total = 0
    for asset in assets:
        for _, _, period_s, _ in INTERVALS:
            total += 1
            msg = f'42{json.dumps(["changeSymbol", {"asset": asset, "period": period_s}])}'
            try:
                ok = await client.send_message(msg)
                if ok:
                    sent += 1
            except Exception as e:
                logger.warning(f"subscribe failed {asset} p={period_s}: {e}")
    logger.info(f"subscribed: {sent}/{total} (asset, period) tuples")
    return sent


async def _fetch_one_with_retry(client, asset: str, tf_str: str, bars: int) -> tuple[list[dict], float, int]:
    """Try up to 3 times. Uses get_candles_dataframe with explicit end_time —
    same call shape as production po_fetcher.raw_fetch_asset that works on VPS.
    """
    total_t0 = time.monotonic()
    for attempt in (1, 2, 3):
        try:
            end_time = datetime.now()
            df = await asyncio.wait_for(
                client.get_candles_dataframe(asset, tf_str, bars, end_time),
                timeout=FETCH_TIMEOUT_S,
            )
            if df is not None and not df.empty:
                # convert df rows back to list[Candle-like]
                records: list[dict] = []
                for ts, row in df.iterrows():
                    ts_norm = ts
                    if hasattr(ts_norm, "to_pydatetime"):
                        ts_norm = ts_norm.to_pydatetime()
                    if isinstance(ts_norm, datetime):
                        if ts_norm.tzinfo is not None:
                            ts_norm = ts_norm.astimezone(timezone.utc).replace(tzinfo=None)
                        ts_iso = ts_norm.isoformat(timespec="seconds")
                    else:
                        ts_iso = str(ts_norm)
                    records.append({
                        "ts": ts_iso,
                        "o": float(row["open"]),
                        "h": float(row["high"]),
                        "l": float(row["low"]),
                        "c": float(row["close"]),
                    })
                return records, time.monotonic() - total_t0, attempt
            # empty df — retry
            await asyncio.sleep(1.0 * attempt)
        except asyncio.TimeoutError:
            await asyncio.sleep(1.0 * attempt)
        except Exception as e:
            logger.warning(f"fetch error {asset} {tf_str} attempt {attempt}: {e}")
            await asyncio.sleep(1.0 * attempt)
    return [], time.monotonic() - total_t0, 3


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

    connect_took = time.monotonic() - started
    ws = getattr(client, "_websocket", None)
    url = getattr(ws, "url", None) or getattr(ws, "_url", None) or "?"
    logger.info(f"connected in {connect_took:.1f}s (uid={getattr(client, 'uid', '?')}, url={url})")

    assets = [_to_asset(alias) for _display, alias in PAIRS]
    await _subscribe_all(client, assets)

    logger.info(f"warm-up: waiting {WARMUP_AFTER_SUBSCRIBE_S}s for PO to start streaming...")
    await asyncio.sleep(WARMUP_AFTER_SUBSCRIBE_S)

    fetch_started = time.monotonic()
    written = 0
    failed = 0
    consecutive_failures = 0
    aborted = False

    for _display, alias in PAIRS:
        if aborted:
            break
        asset = _to_asset(alias)
        for label, tf_str, _period_s, bars in INTERVALS:
            elapsed_fetch = time.monotonic() - fetch_started
            if elapsed_fetch > TOTAL_FETCH_BUDGET_S:
                logger.warning(f"total fetch budget {TOTAL_FETCH_BUDGET_S}s exceeded — stopping")
                aborted = True
                break
            records, took, attempts = await _fetch_one_with_retry(client, asset, tf_str, bars)
            if not records:
                failed += 1
                consecutive_failures += 1
                logger.warning(f"empty: {alias}_{label} took={took:.1f}s attempts={attempts} streak={consecutive_failures}")
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"{MAX_CONSECUTIVE_FAILURES} consecutive fails — aborting")
                    aborted = True
                    break
                await asyncio.sleep(INTER_FETCH_SLEEP_S)
                continue
            consecutive_failures = 0
            snapshot = {
                "asset": alias,
                "interval": label,
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
                "bars": records,
            }
            out_path = DATA_DIR / f"{alias}_{label}.jsonl"
            out_path.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
            written += 1
            logger.info(f"wrote {alias}_{label}: {len(records)} bars took={took:.1f}s attempts={attempts}")
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
