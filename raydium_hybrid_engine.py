"""Hybrid sequential + event-driven single-position trading engine.

This module listens for Raydium pool initialization events from Solana logs,
enriches pool candidates with lightweight metrics, scores opportunities, and
manages a single simulated position using TP/SL rules.

Notes:
- This is a research/simulation scaffold, not production trading software.
- External API integrations are intentionally minimal with stubs where noted.
"""

from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime, timezone
from typing import AsyncGenerator

import aiohttp
import numpy as np

# ============================================
# CONFIG
# ============================================

CAPITAL_PER_TRADE = 10.0
TP_PERCENT = 0.25
SL_PERCENT = -0.15
LEARNING_RATE = 0.05

# Solana / Raydium endpoints
SOLANA_WS_URL = "wss://api.mainnet-beta.solana.com"  # or Helius / QuickNode WS
SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
RAYDIUM_AMM_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"  # Raydium AMM v4

# Public APIs
JUPITER_PRICE_API = "https://price.jup.ag/v4/price?ids={mint}"

# ============================================
# GLOBAL STATE
# ============================================

state = "HUNTING"
active_trade: dict | None = None
weights = {
    "volume": 1.2,
    "dev_clean": 1.5,
    "liquidity": 1.0,
    "holder_quality": 1.3,
}
seen_pools: set[str] = set()
last_seen_signature: str | None = None


# ============================================
# RAYDIUM LIVE EVENT STREAM (WebSocket)
# ============================================

async def raydium_ws_stream(
    session: aiohttp.ClientSession,
) -> AsyncGenerator[tuple[str, list[str]], None]:
    """Subscribe to Raydium AMM program logs and emit initialize2 signatures."""
    subscribe_msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [{"mentions": [RAYDIUM_AMM_PROGRAM]}, {"commitment": "confirmed"}],
    }

    print(f"[{ts()}] 🔌 Connecting to Solana WebSocket...")

    while True:
        try:
            async with session.ws_connect(SOLANA_WS_URL, heartbeat=30) as ws:
                await ws.send_json(subscribe_msg)
                print(f"[{ts()}] ✅ Subscribed to Raydium AMM logs")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)

                        # Skip subscription confirmation payload
                        if "result" in data:
                            continue

                        try:
                            value = data["params"]["result"]["value"]
                            logs = value.get("logs", [])
                            sig = value.get("signature", "")

                            if sig and any("initialize2" in log for log in logs):
                                yield sig, logs

                        except (KeyError, TypeError):
                            continue

                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        print(f"[{ts()}] ⚠️ WS closed/error – reconnecting in 5s")
                        break

        except Exception as exc:  # noqa: BLE001 - resilient stream loop
            print(f"[{ts()}] ❌ WS error: {exc} – retrying in 5s")
            await asyncio.sleep(5)


# ============================================
# HTTP FALLBACK STREAM
# ============================================

async def raydium_http_fallback_stream(
    session: aiohttp.ClientSession,
) -> AsyncGenerator[tuple[str, list[str]], None]:
    """Poll Raydium program signatures and inspect logs as fallback when WS fails."""
    global last_seen_signature

    print(f"[{ts()}] 🔁 Starting HTTP fallback polling")

    while True:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [RAYDIUM_AMM_PROGRAM, {"limit": 20}],
        }
        try:
            async with session.post(
                SOLANA_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as response:
                data = await response.json()
                signatures = data.get("result", [])

            new_sigs: list[str] = []
            for entry in signatures:
                sig = entry.get("signature")
                if not sig:
                    continue
                if last_seen_signature and sig == last_seen_signature:
                    break
                new_sigs.append(sig)

            if signatures:
                last_seen_signature = signatures[0].get("signature")

            for sig in reversed(new_sigs):
                logs = await fetch_tx_logs(session, sig)
                if logs and any("initialize2" in line for line in logs):
                    yield sig, logs

        except Exception as exc:  # noqa: BLE001
            print(f"[{ts()}] ⚠️ HTTP fallback error: {exc}")

        await asyncio.sleep(3)


async def merged_event_signature_stream(
    session: aiohttp.ClientSession,
) -> AsyncGenerator[tuple[str, list[str]], None]:
    """Prefer WS stream, fail over to HTTP poller after repeated WS failures."""
    ws_failures = 0
    while True:
        try:
            async for sig, logs in raydium_ws_stream(session):
                ws_failures = 0
                yield sig, logs
            ws_failures += 1
        except Exception:
            ws_failures += 1

        if ws_failures >= 2:
            async for sig, logs in raydium_http_fallback_stream(session):
                yield sig, logs


# ============================================
# TRANSACTION ENRICHMENT (RPC)
# ============================================

async def fetch_tx_accounts(session: aiohttp.ClientSession, signature: str) -> list[str]:
    """Return account keys involved in a transaction."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}],
    }
    try:
        async with session.post(
            SOLANA_RPC_URL,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as response:
            data = await response.json()
            return data["result"]["transaction"]["message"]["accountKeys"]
    except Exception:
        return []


async def fetch_tx_logs(session: aiohttp.ClientSession, signature: str) -> list[str]:
    """Fetch transaction logs for a signature via RPC."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}],
    }
    try:
        async with session.post(
            SOLANA_RPC_URL,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as response:
            data = await response.json()
            return data["result"]["meta"].get("logMessages", [])
    except Exception:
        return []


async def fetch_token_price(session: aiohttp.ClientSession, mint: str) -> float | None:
    """Get USD price from Jupiter aggregator."""
    try:
        async with session.get(
            JUPITER_PRICE_API.format(mint=mint),
            timeout=aiohttp.ClientTimeout(total=5),
        ) as response:
            data = await response.json()
            return data["data"].get(mint, {}).get("price")
    except Exception:
        return None


async def fetch_pool_metrics(session: aiohttp.ClientSession, signature: str) -> dict | None:
    """Build a candidate metrics dict from on-chain context and stub enrichments."""
    accounts = await fetch_tx_accounts(session, signature)
    if not accounts:
        return None

    # Raydium initialize layout frequently places LP mint near index 4.
    lp_mint = accounts[4] if len(accounts) > 4 else accounts[0]

    if lp_mint in seen_pools:
        return None
    seen_pools.add(lp_mint)

    metrics = {
        "volume": _normalize(random.uniform(500, 200_000), 0, 200_000),
        "dev_clean": random.uniform(0, 1),
        "liquidity": _normalize(random.uniform(500, 50_000), 0, 50_000),
        "holder_quality": random.uniform(0, 1),
    }

    price = await fetch_token_price(session, lp_mint)

    return {
        "lp_mint": lp_mint,
        "signature": signature,
        "price_usd": price,
        "metrics": metrics,
    }


def _normalize(val: float, lo: float, hi: float) -> float:
    return max(0.0, min(1.0, (val - lo) / (hi - lo)))


# ============================================
# EVENT STREAM (yield enriched metrics)
# ============================================

async def event_stream(session: aiohttp.ClientSession) -> AsyncGenerator[dict, None]:
    """Wrap raw feed into enriched pool candidates."""
    async for sig, _logs in merged_event_signature_stream(session):
        pool = await fetch_pool_metrics(session, sig)
        if pool:
            print(
                f"[{ts()}] 📡 New pool: {pool['lp_mint'][:8]}… | "
                f"price={pool['price_usd']} | metrics={fmt(pool['metrics'])}"
            )
            yield pool


# ============================================
# SCORING + PROBABILITY ENGINE
# ============================================


def score_token(metrics: dict) -> float:
    return sum(weights.get(k, 0.0) * v for k, v in metrics.items())


def probability_from_score(score: float) -> float:
    return float(1 / (1 + np.exp(-score)))


# ============================================
# KELLY (scaled to $10)
# ============================================


def position_size(prob: float, reward_multiple: float = 5.0) -> float:
    b = reward_multiple - 1
    q = 1 - prob
    kelly = (b * prob - q) / b
    kelly = max(min(kelly, 1.0), 0.0)
    return CAPITAL_PER_TRADE * kelly


# ============================================
# LEARNING ENGINE
# ============================================


def update_weights(metrics: dict, pnl: float) -> None:
    global weights
    for key, value in metrics.items():
        weights[key] += LEARNING_RATE * pnl * value
    print(f"[{ts()}] 🧠 Weights updated → {fmt(weights)}")


# ============================================
# ADAPTIVE EXIT MANAGER
# ============================================


async def manage_position(entry_price: float) -> None:
    """Simulate active trade management until an exit rule is hit."""
    global state, active_trade

    print(f"[{ts()}] 📊 Monitoring position | entry={entry_price:.6f}")

    while state == "IN_POSITION" and active_trade:
        await asyncio.sleep(2)

        price_change = random.uniform(-0.05, 0.08)
        current_price = active_trade["price"] * (1 + price_change)
        pnl = (current_price - entry_price) / entry_price

        print(f"[{ts()}]   PnL={pnl:+.2%}  price={current_price:.6f}")

        if pnl >= TP_PERCENT:
            print(f"[{ts()}] 🎯 TAKE PROFIT  {pnl:+.2%}")
            close_trade(current_price)
            return

        if pnl <= SL_PERCENT:
            print(f"[{ts()}] 🛑 STOP LOSS    {pnl:+.2%}")
            close_trade(current_price)
            return

        if random.random() < 0.05:
            print(f"[{ts()}] ⚠️  LIQUIDITY EXIT")
            close_trade(current_price)
            return


# ============================================
# CLOSE TRADE
# ============================================


def close_trade(exit_price: float) -> None:
    """Close current simulated trade, update online model, reset state."""
    global state, active_trade

    if not active_trade:
        return

    pnl = (exit_price - active_trade["price"]) / active_trade["price"]
    print(
        f"[{ts()}] 💰 Trade closed | PnL={pnl:+.2%} | "
        f"mint={active_trade['lp_mint'][:8]}…"
    )

    update_weights(active_trade["metrics"], pnl)

    state = "HUNTING"
    active_trade = None


# ============================================
# MAIN BRAIN LOOP
# ============================================


async def brain() -> None:
    """Single-position event-driven strategy controller."""
    global state, active_trade

    async with aiohttp.ClientSession() as session:
        async for pool in event_stream(session):
            if state != "HUNTING":
                continue

            metrics = pool["metrics"]
            score = score_token(metrics)
            prob = probability_from_score(score)
            size = position_size(prob)

            print(f"[{ts()}] 🔍 Score={score:.3f} | Prob={prob:.2f} | Size=${size:.2f}")

            if prob > 0.65 and size >= 1.0:
                state = "IN_POSITION"
                active_trade = {
                    "price": pool["price_usd"] or 1.0,
                    "lp_mint": pool["lp_mint"],
                    "metrics": metrics,
                }
                print(
                    f"[{ts()}] 🚀 BUY  {pool['lp_mint'][:8]}…  "
                    f"Prob={prob:.2f}  Size=${size:.2f}"
                )
                asyncio.create_task(manage_position(active_trade["price"]))


# ============================================
# HELPERS
# ============================================


def ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def fmt(values: dict) -> str:
    return " | ".join(f"{key}={value:.3f}" for key, value in values.items())


# ============================================
# ENTRY
# ============================================

if __name__ == "__main__":
    print("=" * 60)
    print("  HYBRID ENGINE  |  Live Raydium Stream  |  $10 capital")
    print("=" * 60)
    try:
        asyncio.run(brain())
    except KeyboardInterrupt:
        print(f"\n[{ts()}] Stopped.")
