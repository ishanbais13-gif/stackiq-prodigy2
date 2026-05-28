from __future__ import annotations

import math
import os
import time
import logging
import threading
from typing import Any, Dict, List, Optional

log = logging.getLogger("stackiq")

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
_PREMOVER_CACHE: Dict[str, Any] = {"ts": 0.0, "results": [], "scanned": 0}
_PREMOVER_CACHE_TTL = 3600.0  # 1 hour — re-scan after market builds new setup
_PREMOVER_LOCK = threading.Lock()

# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _sf(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _clamp(v: Any, lo: float, hi: float) -> float:
    x = _sf(v)
    if x is None:
        return float(lo)
    return max(float(lo), min(float(hi), x))


def _mean(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(vals) / len(vals)


def _extract(bars: List[Dict], key: str) -> List[float]:
    out: List[float] = []
    for b in bars or []:
        v = _sf(b.get(key) if isinstance(b, dict) else None)
        if v is not None:
            out.append(v)
    return out


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> Optional[float]:
    if len(closes) < period + 2:
        return None
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
           for i in range(1, len(closes))]
    if not trs:
        return None
    tail = trs[-period:] if len(trs) >= period else trs
    return _mean(tail)


# ---------------------------------------------------------------------------
# Universe builder
# ---------------------------------------------------------------------------

_SMALLCAP_SEED: List[str] = [
    # Known volatile small-caps that tend to make big moves
    "SOUN","BBAI","GFAI","AITX","NVTS","LAZR","LIDR","OUST","VLDR","MVIS",
    "CENN","FFIE","NKLA","WKHS","RIDE","GOEV","SOLO","AYRO","KNDI","BEEM",
    "BLNK","CHPT","EVGO","SES","PTRA","XOS","IDEX","AMTX","GEVO","BTCS",
    "VERB","ILUS","XELA","CLOV","WTRH","NNDM","AEYE","AEVA","OPAL","ATNF",
    "BFRI","CYTH","DARE","DFLI","EDSA","FBIO","FREQ","FWBI","GBOX","GFAI",
    "HLBZ","HOLO","IMPP","INDO","INPX","JAGX","JAKK","JCSE","KALI","KAVL",
    "KTTA","LGHL","LIQT","LITM","LKCO","LMND","LODE","LRFC","LTBR","LTRX",
    "MAIA","MASS","MBIO","MGOL","MGRX","MIGI","MIST","MKTY","MMAT","MMTLP",
    "MRIN","MRKR","MTTR","MULN","MVST","MYNZ","NEON","NFYS","NKTR","NODK",
    "NPAB","NRGV","NVVE","NXTP","OCUP","OGEN","OIIM","OPFI","OPGN","OPHC",
    "ORPH","OSAT","OSUR","OWVI","PALT","PASG","PAYS","PECK","PHIO","PHUN",
    "PIXY","PLAG","PRFX","PROP","PRPO","PRQR","PRTG","PRTS","PRTY","PRUX",
    "PRZE","PSIX","PSTV","PTIX","PTLO","PTSI","PTVE","PUBM","PULM","PVBC",
    "PVNC","PXMD","PYPD","PYXS","QBTS","QIPT","QLGN","QMMM","QNRX","QNST",
    "QPAG","QQQS","QRTEP","QRTX","QUBT","RBBN","RBCN","RCAT","RCFA","RCKT",
    "RDUS","RDVT","REAX","REED","RELI","RENT","REPL","RETO","RGLS","RGTI",
    "RIBT","RICK","RKDA","RLAY","RLMD","RLTY","RLYB","RMNI","RMTX","RNAC",
    "RNAZ","RNLX","RNRG","RNSN","RNST","RNVZ","RNWK","ROCC","ROCL","ROGE",
    "ROLL","ROMN","ROMX","ROPE","RPRX","RRAC","RRBI","RRIF","RRMX","RSSS",
    "RTPX","RUBY","RUNN","RVLP","RVMD","RVNC","RVPH","RVYL","RWLK","RYDE",
    # More known names
    "ACRV","ACST","ACXM","ADGM","ADIL","ADITXT","ADMA","ADMT","ADNK","ADOC",
    "AEAC","AEHR","AEIS","AEMD","AENZ","AERI","AEYE","AFAR","AFBI","AFCG",
    "AFIB","AFMD","AFRI","AGBA","AGFY","AGIL","AGMH","AGMS","AGNCM","AGNCO",
    "AGRI","AGRO","AGRX","AGTI","AGUS","AGYS","AHCO","AHED","AHPI","AHRN",
    "AIHS","AIIM","AIOT","AIRC","AIRG","AIRI","AIRJ","AIRS","AIRSP","AIRT",
    "AITO","AIXI","AIXI","AIYO","AIZN","AJAX","AKBA","AKLI","AKRO","AKTS",
    "AKTX","AKUS","AKVB","AKYA","ALBT","ALCE","ALCX","ALDX","ALEC","ALEE",
    "ALFI","ALGM","ALGS","ALGT","ALID","ALIM","ALIO","ALKT","ALLT","ALMD",
    "ALNT","ALNY","ALOR","ALOT","ALPA","ALPN","ALPP","ALRM","ALRN","ALRS",
    "ALSN","ALSP","ALTI","ALTM","ALTN","ALTO","ALTR","ALTU","ALTV","ALTX",
    # High-momentum small-caps with frequent big moves
    "IONQ","ARQQ","QUBT","QBTS","RGTI","BTBT","CLSK","HUT","CIFR","BTDR",
    "WULF","IREN","CORZ","MARA","RIOT","HIVE","SMLR","CLRB","MOGO","DGLY",
    "SHIP","TOPS","GASS","EDRY","FREE","SALT","GLBS","DCGO","PSHG","IMVT",
    "ACHR","JOBY","LILM","EHANG","EVTL","ARCHER","BLADE","SKYX","SATL","ASTS",
    "LPSN","BBBY","CRIS","CYAD","EDTK","ENVX","EVEX","EVER","EVEX","FAZE",
    "FBRX","FCUV","FDOC","FENC","FFIE","FGEN","FGNA","FGNN","FGNO","FGNZ",
    "FHLT","FHTX","FIAC","FIAM","FIFW","FIGI","FIHL","FIII","FILC","FILO",
    "FINV","FIXX","FKWL","FLAB","FLCN","FLFV","FLGT","FLGX","FLNT","FLOC",
    "FLUX","FMAC","FMBH","FMCC","FMNB","FMST","FNAM","FNCH","FNCX","FNGR",
    "FNRN","FNVT","FOAM","FOCS","FOLD","FOLO","FONR","FORL","FORM","FORR",
]


def build_smallcap_universe(
    scan_universe: List[str],
    max_candidates: int = 400,
) -> List[str]:
    """Filter scan universe down to small-cap candidates ($1-$20, liquid).

    Uses Alpaca snapshots to filter by price and volume.
    Returns up to max_candidates symbols sorted by dollar volume descending.
    """
    from data_fetcher import get_snapshots_batch

    # Merge scan universe with seed small-caps, dedup
    combined = list(dict.fromkeys(list(scan_universe) + _SMALLCAP_SEED))
    clean = [s for s in combined if s and "." not in s and len(s) <= 6]

    log.info(f"premover_universe: fetching snapshots for {len(clean)} candidates")

    candidates: List[tuple] = []  # (dollar_vol, symbol)
    chunk_size = 200

    for i in range(0, len(clean), chunk_size):
        chunk = clean[i: i + chunk_size]
        try:
            snaps = get_snapshots_batch(chunk) or {}
        except Exception as e:
            log.warning(f"premover_universe: snapshot fetch error: {e}")
            continue

        for sym, snap in snaps.items():
            if not isinstance(snap, dict):
                continue
            try:
                db = snap.get("dailyBar") or snap.get("day") or {}
                lt = snap.get("latestTrade") or snap.get("latestQuote") or {}

                price = _sf(db.get("c") or db.get("vw") or lt.get("p") or lt.get("ap"))
                vol = _sf(db.get("v"))

                if price is None or vol is None:
                    continue
                if price < 1.0 or price > 20.0:
                    continue

                dollar_vol = price * vol
                if dollar_vol < 200_000:  # min $200k daily dollar volume
                    continue

                candidates.append((dollar_vol, sym))
            except Exception:
                continue

    # Sort by dollar volume descending — most liquid first
    candidates.sort(key=lambda x: x[0], reverse=True)
    result = [sym for _, sym in candidates[:max_candidates]]
    log.info(f"premover_universe: {len(result)} small-caps passed price+volume filter")
    return result


# ---------------------------------------------------------------------------
# Alpaca news check (batch, no Polygon to avoid rate limits)
# ---------------------------------------------------------------------------

def _check_news_alpaca(symbols: List[str]) -> Dict[str, bool]:
    """Return {symbol: True} for any symbol with news in last 3 days."""
    import requests as _req

    has_news: Dict[str, bool] = {}
    api_key = (os.getenv("ALPACA_API_KEY") or "").strip()
    secret = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    if not api_key or not secret:
        return has_news

    cutoff = int(time.time()) - 3 * 86400
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret}
    url = "https://data.alpaca.markets/v1beta1/news"

    for i in range(0, len(symbols), 10):
        chunk = symbols[i: i + 10]
        try:
            resp = _req.get(
                url,
                headers=headers,
                params={
                    "symbols": ",".join(chunk),
                    "limit": 10,
                    "sort": "desc",
                },
                timeout=8,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            for article in data.get("news") or []:
                for sym in article.get("symbols") or []:
                    s = str(sym).upper().strip()
                    if s in chunk:
                        has_news[s] = True
        except Exception as e:
            log.debug(f"premover_news: chunk error: {e}")
            continue

    return has_news


# ---------------------------------------------------------------------------
# Scorer
# ---------------------------------------------------------------------------

def _score_symbol(
    symbol: str,
    snap: Dict[str, Any],
    bars: List[Dict[str, Any]],
    spy_closes: List[float],
    has_news: bool,
) -> Dict[str, Any]:
    """Score a single symbol 0-100 for pre-mover potential."""
    signals: Dict[str, Any] = {}
    score = 0.0

    highs = _extract(bars, "h")
    lows = _extract(bars, "l")
    closes = _extract(bars, "c")
    vols = _extract(bars, "v")

    db = snap.get("dailyBar") or snap.get("day") or {}
    lt = snap.get("latestTrade") or snap.get("latestQuote") or {}
    price = _sf(db.get("c") or db.get("vw") or lt.get("p") or lt.get("ap"))

    # --- 1) Volume surge (30 pts) ---
    try:
        cur_vol = _sf(db.get("v"))
        if cur_vol is None and vols:
            cur_vol = vols[-1]
        if cur_vol and len(vols) >= 6:
            avg20 = _mean(vols[-21:-1] if len(vols) >= 21 else vols[:-1]) or 1.0
            ratio = cur_vol / max(avg20, 1.0)
            # 2x = 20pts, 3x = 30pts, scales linearly
            vol_pts = _clamp((ratio - 1.0) / 2.0 * 30.0, 0.0, 30.0)
            score += vol_pts
            signals["vol_surge"] = {"pts": round(vol_pts, 1), "ratio": round(ratio, 2)}
    except Exception:
        pass

    # --- 2) ATR compression (25 pts) ---
    try:
        if len(closes) >= 25:
            atr5 = _atr(highs, lows, closes, 5)
            atr20 = _atr(highs, lows, closes, 20)
            if atr5 and atr20 and atr20 > 0:
                ratio_atr = atr5 / atr20
                # Tight coil: ratio < 0.75 → max pts; ratio > 1.25 → 0 pts
                compress_pts = _clamp((1.25 - ratio_atr) / 0.5 * 25.0, 0.0, 25.0)
                score += compress_pts
                signals["atr_compression"] = {"pts": round(compress_pts, 1), "atr5_atr20": round(ratio_atr, 3)}
    except Exception:
        pass

    # --- 3) Close near high of day (15 pts) ---
    try:
        day_h = _sf(db.get("h"))
        day_l = _sf(db.get("l"))
        day_c = _sf(db.get("c"))
        if day_h is not None and day_l is not None and day_c is not None:
            rng = day_h - day_l
            if rng > 0:
                pos = (day_c - day_l) / rng  # 0=low of day, 1=high of day
                close_pts = _clamp(pos * 15.0, 0.0, 15.0)
                score += close_pts
                signals["close_strength"] = {"pts": round(close_pts, 1), "position_in_range": round(pos, 3)}
    except Exception:
        pass

    # --- 4) Proximity to 20-day high (15 pts) ---
    try:
        if len(closes) >= 20 and price is not None:
            high_20d = max(highs[-20:]) if len(highs) >= 20 else None
            if high_20d and high_20d > 0:
                pct_from_high = (high_20d - price) / high_20d
                # Within 3% = 15pts, within 10% = 7pts, beyond = 0
                breakout_pts = _clamp((0.10 - pct_from_high) / 0.10 * 15.0, 0.0, 15.0)
                score += breakout_pts
                signals["near_high"] = {"pts": round(breakout_pts, 1), "pct_from_20d_high": round(pct_from_high * 100, 2)}
    except Exception:
        pass

    # --- 5) News catalyst (10 pts) ---
    if has_news:
        score += 10.0
        signals["news_catalyst"] = {"pts": 10.0}

    # --- 6) RS vs SPY 3-day (5 pts) ---
    try:
        if len(closes) >= 4 and len(spy_closes) >= 4:
            stock_r3 = (closes[-1] - closes[-4]) / max(abs(closes[-4]), 0.01)
            spy_r3 = (spy_closes[-1] - spy_closes[-4]) / max(abs(spy_closes[-4]), 0.01)
            alpha = stock_r3 - spy_r3
            rs_pts = _clamp((alpha + 0.02) / 0.06 * 5.0, 0.0, 5.0)
            score += rs_pts
            signals["rs_vs_spy"] = {"pts": round(rs_pts, 1), "alpha_3d_pct": round(alpha * 100, 2)}
    except Exception:
        pass

    # Entry zone and invalidation
    entry_zone: Optional[str] = None
    invalidation: Optional[str] = None
    try:
        if price is not None:
            atr5_val = None
            if len(closes) >= 7:
                atr5_val = _atr(highs, lows, closes, 5)
            if atr5_val and atr5_val > 0:
                entry_zone = f"${round(price, 2)} – ${round(price * 1.05, 2)}"
                invalidation = f"${round(price - atr5_val * 1.5, 2)}"
            else:
                entry_zone = f"${round(price, 2)}"
                invalidation = f"${round(price * 0.92, 2)}"
    except Exception:
        pass

    return {
        "symbol": symbol,
        "score": round(_clamp(score, 0.0, 100.0), 1),
        "price": price,
        "signals": signals,
        "entry_zone": entry_zone,
        "invalidation": invalidation,
    }


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def run_premover_scan(
    scan_universe: List[str],
    max_results: int = 20,
    news_top_k: int = 50,
    max_seconds: float = 300.0,
) -> Dict[str, Any]:
    """Scan for pre-movers. Returns top candidates sorted by score desc.

    Args:
        scan_universe: Full universe from get_scan_universe() in app.py.
        max_results: How many top results to return.
        news_top_k: Check news for top-N candidates by volume surge.
        max_seconds: Hard timeout.
    """
    from data_fetcher import get_snapshots_batch, get_bars_batch

    t0 = time.time()

    # --- Step 1: Build small-cap universe ---
    universe = build_smallcap_universe(scan_universe, max_candidates=400)
    if not universe:
        return {"results": [], "scanned": 0, "elapsed": 0.0, "error": "empty_universe"}

    if time.time() - t0 > max_seconds * 0.2:
        log.warning("premover_scan: timeout hit during universe build")

    # --- Step 2: Fetch snapshots for price/volume pre-filter ---
    log.info(f"premover_scan: fetching snapshots for {len(universe)} candidates")
    snapmap: Dict[str, Any] = {}
    for i in range(0, len(universe), 200):
        if time.time() - t0 > max_seconds * 0.4:
            log.warning("premover_scan: timeout hit during snapshot fetch")
            break
        chunk = universe[i: i + 200]
        try:
            chunk_snaps = get_snapshots_batch(chunk) or {}
            snapmap.update(chunk_snaps)
        except Exception as e:
            log.warning(f"premover_scan: snapshot error: {e}")

    # --- Step 3: Volume-surge pre-filter to top 80-150 ---
    vol_scored: List[tuple] = []  # (vol_ratio, symbol, snap)
    for sym in universe:
        snap = snapmap.get(sym)
        if not isinstance(snap, dict):
            continue
        db = snap.get("dailyBar") or snap.get("day") or {}
        price = _sf(db.get("c") or db.get("vw"))
        if price is None or price < 1.0 or price > 20.0:
            continue
        vol = _sf(db.get("v"))
        if not vol or vol <= 0:
            continue
        # Rough volume surge proxy using snapshot prev close volume if available
        prev_vol = _sf(db.get("pv") or db.get("vw"))
        ratio = 1.0
        if prev_vol and prev_vol > 0:
            ratio = vol / prev_vol
        vol_scored.append((ratio, sym, snap))

    vol_scored.sort(key=lambda x: x[0], reverse=True)
    top_candidates = [(sym, snap) for _, sym, snap in vol_scored[:150]]
    log.info(f"premover_scan: {len(top_candidates)} candidates after vol-surge pre-filter")

    if not top_candidates:
        return {"results": [], "scanned": 0, "elapsed": round(time.time() - t0, 2), "error": "no_candidates"}

    # --- Step 4: Fetch daily bars for scoring ---
    syms_to_score = [s for s, _ in top_candidates]
    bars_map: Dict[str, List[Dict]] = {}
    log.info(f"premover_scan: fetching daily bars for {len(syms_to_score)} symbols")

    for i in range(0, len(syms_to_score), 100):
        if time.time() - t0 > max_seconds * 0.7:
            log.warning("premover_scan: timeout hit during bars fetch")
            break
        chunk = syms_to_score[i: i + 100]
        try:
            chunk_bars = get_bars_batch(chunk, "1Day", 30) or {}
            bars_map.update(chunk_bars)
        except Exception as e:
            log.warning(f"premover_scan: bars error: {e}")

    # SPY bars for RS comparison
    spy_closes: List[float] = []
    try:
        spy_data = get_bars_batch(["SPY"], "1Day", 30)
        spy_bars = spy_data.get("SPY") or []
        spy_closes = [b["c"] for b in spy_bars if isinstance(b, dict) and _sf(b.get("c")) is not None]
    except Exception:
        pass

    # --- Step 5: Check news for top-N by vol ratio ---
    news_syms = [s for s, _ in top_candidates[:news_top_k]]
    has_news_map: Dict[str, bool] = {}
    if news_syms and (time.time() - t0 < max_seconds * 0.85):
        try:
            log.info(f"premover_scan: checking news for {len(news_syms)} symbols")
            has_news_map = _check_news_alpaca(news_syms)
        except Exception as e:
            log.warning(f"premover_scan: news check error: {e}")

    # --- Step 6: Score ---
    results: List[Dict[str, Any]] = []
    for sym, snap in top_candidates:
        bars = bars_map.get(sym) or []
        result = _score_symbol(
            symbol=sym,
            snap=snap,
            bars=bars,
            spy_closes=spy_closes,
            has_news=has_news_map.get(sym, False),
        )
        if result["score"] >= 20.0:  # minimum quality threshold
            results.append(result)

    results.sort(key=lambda x: x["score"], reverse=True)
    elapsed = round(time.time() - t0, 2)

    log.info(
        f"premover_scan: done | candidates={len(top_candidates)} scored={len(results)} "
        f"top={results[0]['symbol'] if results else 'none'} elapsed={elapsed}s"
    )

    return {
        "results": results[:max_results],
        "scanned": len(top_candidates),
        "elapsed": elapsed,
        "ts": time.time(),
    }


# ---------------------------------------------------------------------------
# Cache accessor
# ---------------------------------------------------------------------------

def get_cached_premover_results() -> Dict[str, Any]:
    with _PREMOVER_LOCK:
        return dict(_PREMOVER_CACHE)


def set_cached_premover_results(scan_result: Dict[str, Any]) -> None:
    with _PREMOVER_LOCK:
        _PREMOVER_CACHE["ts"] = float(scan_result.get("ts") or time.time())
        _PREMOVER_CACHE["results"] = list(scan_result.get("results") or [])
        _PREMOVER_CACHE["scanned"] = int(scan_result.get("scanned") or 0)


def premover_cache_is_fresh() -> bool:
    with _PREMOVER_LOCK:
        ts = float(_PREMOVER_CACHE.get("ts") or 0.0)
    return (time.time() - ts) < _PREMOVER_CACHE_TTL
