"""Long-running stream capture: subscribe to live tick feed, aggregate ticks
into 1-min OHLC bars, merge with existing rolling window (200 bars per asset)
and commit.

PO's history endpoint (loadHistoryPeriod) is silently blocked for fresh WS
sessions from cloud IPs — anti-scraping. But the tick stream (stream_update)
DOES flow. So we capture ticks live and build OHLC ourselves.

Runs ~4 minutes per invocation. Cron every 5 min → continuous coverage with
no gaps thanks to rolling-window merge.

M5 bars are NOT written here — the local github_relay_fetcher.py builds them
from M1 rows on the fly. Halves storage + workflow complexity.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CONNECT_TIMEOUT_S = 25.0
CAPTURE_DURATION_S = 240.0  # 4 minutes live capture per run
ROLLING_WINDOW_BARS = 200   # ~3.3h of M1 — covers M5×60 aggregation


def _to_asset(alias: str) -> str:
    return f"{alias}_otc"


def _asset_to_alias(asset: str) -> str:
    return asset[:-4] if asset.endswith("_otc") else asset


def _floor_minute(ts_epoch: float) -> int:
    """Snap epoch seconds down to the start of its minute."""
    return int(ts_epoch // 60 * 60)


# in-memory OHLC accumulator per asset — reset per run
# key: alias, value: {minute_epoch: {"o", "h", "l", "c", "last_ts"}}
_ohlc_agg: dict[str, dict[int, dict[str, float]]] = {}
_tick_count = 0
_stream_event_count = 0


async def _on_stream_update(data: Any) -> None:
    """PO tick feed handler. Aggregates ticks into per-minute OHLC bars.

    Format observed in the wild: `[[asset, ts_sec, price, ...], ...]` per frame.
    Robust to slight shape drift.
    """
    global _tick_count, _stream_event_count
    _stream_event_count += 1
    try:
        frames = data if isinstance(data, list) else [data]
        for f in frames:
            if not isinstance(f, (list, tuple)) or len(f) < 3:
                continue
            asset_raw, ts_raw, price_raw = f[0], f[1], f[2]
            if not isinstance(asset_raw, str) or not asset_raw.endswith("_otc"):
                continue
            alias = _asset_to_alias(asset_raw)
            try:
                ts = float(ts_raw)
                price = float(price_raw)
            except (TypeError, ValueError):
                continue
            minute = _floor_minute(ts)
            bucket = _ohlc_agg.setdefault(alias, {})
            bar = bucket.get(minute)
            if bar is None:
                bucket[minute] = {"o": price, "h": price, "l": price, "c": price, "last_ts": ts}
            else:
                if price > bar["h"]:
                    bar["h"] = price
                if price < bar["l"]:
                    bar["l"] = price
                if ts >= bar["last_ts"]:
                    bar["c"] = price
                    bar["last_ts"] = ts
            _tick_count += 1
    except Exception as e:
        logger.warning(f"stream_update handler error: {e}")


def _load_existing_bars(alias: str) -> list[dict]:
    """Read previous rolling window (if any). Missing/corrupt → empty list."""
    path = DATA_DIR / f"{alias}_1min.jsonl"
    if not path.exists():
        return []
    try:
        snap = json.loads(path.read_text(encoding="utf-8"))
        bars = snap.get("bars") or []
        return [b for b in bars if isinstance(b, dict) and "ts" in b]
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"failed to load existing {alias}: {e}")
        return []


def _merge_and_write(alias: str, new_bars: list[dict]) -> int:
    """Merge new M1 bars into rolling window. Returns final bar count."""
    existing = _load_existing_bars(alias)
    by_ts: dict[str, dict] = {b["ts"]: b for b in existing}
    # new bars override existing at same minute (freshest tick set)
    for b in new_bars:
        by_ts[b["ts"]] = b
    merged = sorted(by_ts.values(), key=lambda b: b["ts"])
    merged = merged[-ROLLING_WINDOW_BARS:]
    snapshot = {
        "asset": alias,
        "interval": "1min",
        "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
        "bars": merged,
    }
    out_path = DATA_DIR / f"{alias}_1min.jsonl"
    out_path.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
    return len(merged)


def _agg_to_records(alias: str) -> list[dict]:
    """Convert this run's in-memory OHLC accumulator to sorted list[dict].

    Skips the currently-forming minute (last one) — it's incomplete.
    """
    bucket = _ohlc_agg.get(alias, {})
    if not bucket:
        return []
    minutes = sorted(bucket.keys())
    now_min = _floor_minute(time.time())
    out: list[dict] = []
    for m in minutes:
        if m >= now_min:
            continue  # skip incomplete current minute
        bar = bucket[m]
        ts_iso = datetime.fromtimestamp(m, tz=timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        out.append({
            "ts": ts_iso,
            "o": round(bar["o"], 6),
            "h": round(bar["h"], 6),
            "l": round(bar["l"], 6),
            "c": round(bar["c"], 6),
        })
    return out


async def _subscribe_all(client, assets: list[str]) -> int:
    sent = 0
    for asset in assets:
        # period=60 → subscribe to M1 candle stream. Sufficient for tick feed.
        msg = f'42{json.dumps(["changeSymbol", {"asset": asset, "period": 60}])}'
        try:
            ok = await client.send_message(msg)
            if ok:
                sent += 1
        except Exception as e:
            logger.warning(f"subscribe failed {asset}: {e}")
    logger.info(f"subscribed: {sent}/{len(assets)} assets")
    return sent


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

    # Wire our stream handler BEFORE subscribing
    try:
        client.add_event_callback("stream_update", _on_stream_update)
    except Exception as e:
        logger.error(f"failed to wire stream handler: {e}")

    assets = [_to_asset(alias) for _display, alias in PAIRS]
    await _subscribe_all(client, assets)

    logger.info(f"capturing ticks for {CAPTURE_DURATION_S:.0f}s...")
    capture_start = time.monotonic()
    last_progress = capture_start
    while time.monotonic() - capture_start < CAPTURE_DURATION_S:
        await asyncio.sleep(15)
        now = time.monotonic()
        # progress log every 15s
        assets_with_data = sum(1 for a in _ohlc_agg if _ohlc_agg[a])
        logger.info(
            f"progress: elapsed={now - capture_start:.0f}s "
            f"stream_events={_stream_event_count} ticks={_tick_count} "
            f"assets_with_ticks={assets_with_data}/{len(assets)}"
        )
        last_progress = now

    logger.info("capture window closed — aggregating and writing...")

    written_assets = 0
    total_bars_out = 0
    for _display, alias in PAIRS:
        records = _agg_to_records(alias)
        if not records:
            logger.warning(f"no completed bars for {alias} this run — skip write")
            continue
        n = _merge_and_write(alias, records)
        written_assets += 1
        total_bars_out += len(records)
        logger.info(f"merged {alias}: +{len(records)} new bars → rolling={n}")

    try:
        await asyncio.wait_for(client.disconnect(), timeout=5.0)
    except Exception:
        pass

    elapsed = time.monotonic() - started
    logger.info(
        f"done in {elapsed:.1f}s | stream_events={_stream_event_count} "
        f"ticks={_tick_count} assets_written={written_assets}/{len(PAIRS)} "
        f"new_bars_total={total_bars_out}"
    )
    return 0 if written_assets else 5


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
