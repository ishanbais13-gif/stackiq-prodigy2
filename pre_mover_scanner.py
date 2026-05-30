from __future__ import annotations

import math
import os
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

log = logging.getLogger("stackiq")

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
_PREMOVER_CACHE: Dict[str, Any] = {"ts": 0.0, "results": [], "scanned": 0}
_PREMOVER_CACHE_TTL = 3600.0
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

# Stable mid/large-caps that flood the results on high-vol days but will never
# produce 50%+ explosive moves. Excluded regardless of volume.
_LARGECAP_STABLE_EXCLUDE: Set[str] = {
    "LYFT", "UBER", "DASH", "ABNB",           # ride-share / gig economy
    "HBAN", "FNB", "RKT", "OWL", "MUFG",      # banks / financials
    "FITB", "KEY", "CFG", "ZION", "MTB",       # regional banks
    "PCG", "AES", "RUN", "NRG", "VST",         # utilities / solar infra
    "COLD",                                     # REIT
    "INFY", "WIT", "CTSH",                     # large IT services
    "KVUE", "ABEV", "DEO",                      # consumer staples
    "LUMN", "T", "VZ",                          # telecom
    "F", "GM", "STLA",                          # legacy auto (big floats)
    "OWL", "BXSL", "ARCC", "MAIN",             # BDCs / asset managers
}

_SMALLCAP_SEED: List[str] = [
    # --- Existing small-caps / momentum names ---
    "SOUN","BBAI","GFAI","AITX","NVTS","LAZR","LIDR","OUST","VLDR","MVIS",
    "CENN","FFIE","NKLA","WKHS","RIDE","GOEV","SOLO","AYRO","KNDI","BEEM",
    "BLNK","CHPT","EVGO","SES","PTRA","XOS","IDEX","AMTX","GEVO","BTCS",
    "VERB","ILUS","XELA","CLOV","WTRH","NNDM","AEYE","AEVA","OPAL","ATNF",
    "BFRI","CYTH","DARE","DFLI","EDSA","FBIO","FREQ","FWBI","GBOX",
    "HLBZ","HOLO","IMPP","INDO","INPX","JAGX","JAKK","JCSE","KALI","KAVL",
    "ACHR","JOBY","LILM","EHANG","EVTL","BLADE","SKYX","SATL","ASTS","MNTS",
    "IONQ","ARQQ","QUBT","QBTS","RGTI","BTBT","CLSK","HUT","CIFR","BTDR",
    "WULF","IREN","CORZ","MARA","RIOT","HIVE","SMLR","CLRB","MOGO","DGLY",
    "SHIP","TOPS","GASS","EDRY","FREE","SALT","GLBS","DCGO","PSHG","IMVT",
    "LPSN","CRIS","CYAD","EDTK","ENVX","EVER","EVEX","FAZE","FBRX","FCUV",
    "UAMY","DNN","MP","LTHM","SQM","LAC","PLL","SGML","ALTM","NOVL",
    "SNAP","PLUG","LUMN","MPT","VALE","CNH","GGB","WTI","CCC","BBD",
    "TSLL","TSLG","VTIX","AEMD","LYG",
    "RXT","AVPT","NKTR","OPEN","PRPL","SKLZ","SDC","BARK",
    "SPCE","NRDY","GETY","ATXI","CLFD","AVDX","HIMS","TDOC","SKIN",
    "MAPS","MNTV","VIEW","TASK","TALK","PAYO","RELY",
    "ACMR","APLD","AQMS","AQST","ARBE","ARCT","ARDX","AREC",
    # --- True penny stocks / sub-$1 known surge candidates ---
    # Biotech pennies (FDA catalysts, clinical results — massive vol spikes)
    "TNXP","NURO","TRVI","AMPIO","CLRB","MTNB","RCON","SIGA","TRVN","OCGN",
    "ADXS","AGTX","AHNR","AIKI","AIMD","ALIM","ALLK","ALLT","ALNY","ALRM",
    "ANTE","APRE","APVO","AQXP","ARBB","ARHS","ARILD","ARIS","ARKA","ARKO",
    "ABOS","ABCL","ABIO","ABLV","ABTX","ACAB","ACCD","ACEL","ACET","ACLX",
    "ACNB","ACOM","ACRV","ACST","ACVA","ACXP","ADAP","ADCT","ADEA","ADIL",
    "INKW","NLSP","BRBT","MFON","GXAI","AIXI","IMCC","LRND","HYMC","CGNX",
    # Shipping / tanker pennies (sector rotation plays)
    "CTRM","SBLK","GOGL","DSSI","EGLE","GSIT","GURE","HROW","IMVT","INDO",
    "INSW","ESEA","PRSM","RRBI","SHIP","TOPS","EDRY","FREE","PSHG",
    # Meme / high-short-interest penny stocks
    "BBBY","NAKD","EXPR","SPRT","BNED","HCDI","MMAT","PHUN","BBIG","ATER",
    "CODA","HLTH","TPVG","TTCF","VISL","VVPR","WIMI","XBIO","XERS","XFOR",
    # Cannabis pennies (heavy promotional, short-notice spikes)
    "SNDL","TLRY","GRWG","MSOS","CRLBF","GTBIF","TCNNF","ACB","APHA","CGC",
    "HEXO","OGI","KERN","IIPR","CURLF","FFNTF","GMVHF","HRVOF","PLNHF",
    # Mining / resource pennies (news-driven 100%+ moves)
    "AG","EXK","PAAS","MAG","SILV","GPL","GATO","HL","CDE","USAS",
    "MDNA","MNRL","MTB","MTRN","MTRX","MVA","MVBF","MVST","MWEI","MWIN",
    "KERN","KINS","KLXE","KMDA","KNDI","KNTE","KOSS","KRMD","KRTX","KRYS",
    # Biotech catalyst watchlist (phase 2/3 readout names)
    "ADMA","ADMP","ADMS","ADNC","ADPT","ADSE","ADTX","ADUS","ADVM","ADXN",
    "CMDX","CMPI","CMPS","CMRA","CMRX","CMTL","CNDT","CNET","CNEY","CNFI",
    "APRE","APRO","APRN","APRT","APTV","APVO","APWC","APXI","APYX","AQMS",
    "AEYE","AEZS","AFCG","AFIB","AFMD","AFRI","AFTR","AGBA","AGCB","AGFY",
    "IMUX","IMNM","IMNN","IMOS","IMPL","IMPX","IMRA","IMRN","IMRS","IMTX",
    # OTC-adjacent listed pennies (exchange-listed, Alpaca-accessible)
    "AMTD","AMTX","AMWL","AMXT","ANAB","ANAC","ANDE","ANEB","ANGH","ANIK",
    "GFAI","GFOR","GFSO","GGAA","GGAL","GGEN","GGRW","GGUS","GHAI","GHIX",
    "ABIO","ABCL","ABUS","ABVC","ABVX","ACAX","ACBA","ACCD","ACEL","ACET",
    # Reverse-split survivors / former meme stocks that still trade
    "PHUN","TTOO","OUST","MMAT","NKTX","NVAX","SAVA","AGEN","ACRS","ACST",
    "HTOO","HYLN","HYMC","HYPR","HYSR","HZPT","ICCM","ICCC","ICCH","ICDX",
    # Speculative tech / AI pennies
    "RNXT","RNWK","RNXT","ROBJ","ROBR","ROBT","ROCG","ROCK","RCRT","RCUS",
    "PAVS","PAVM","PAVI","PAVS","PAWZ","PAYS","PBAX","PBFS","PBHC","PBIP",
    # Shipping/tanker additional
    "PANL","PANW","PAOP","PAQC","PARAA","PARAF","PARD","PARR","PATK","PAVM",
]


def build_smallcap_universe(
    scan_universe: List[str],
    max_candidates: int = 600,
) -> List[str]:
    """Filter scan universe + seed down to candidates ($0.10-$20, liquid).

    Penny stocks ($0.10-$1): min $50k daily dollar volume (they trade thin).
    Small-caps ($1-$20): min $200k daily dollar volume.
    """
    from data_fetcher import get_snapshots_batch

    combined = list(dict.fromkeys(list(scan_universe) + _SMALLCAP_SEED))
    clean = [s for s in combined if s and "." not in s and len(s) <= 6]

    log.info(f"premover_universe: fetching snapshots for {len(clean)} candidates")

    candidates: List[tuple] = []
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
                # Hard exclusion: known stable mid/large-caps
                if sym in _LARGECAP_STABLE_EXCLUDE:
                    continue
                db = snap.get("dailyBar") or snap.get("day") or {}
                lt = snap.get("latestTrade") or snap.get("latestQuote") or {}
                price = _sf(db.get("c") or db.get("vw") or lt.get("p") or lt.get("ap"))
                vol = _sf(db.get("v"))
                if price is None or vol is None:
                    continue
                if price < 0.10 or price > 20.0:
                    continue
                dollar_vol = price * vol
                # Tiered liquidity floor: pennies need less, small-caps need more
                min_dvol = 50_000 if price < 1.0 else 200_000
                if dollar_vol < min_dvol:
                    continue
                # Normal-vol filter: prevDailyBar.v (Alpaca) or dailyBar.v (Polygon)
                # both represent a complete trading day's volume — use as proxy for
                # "normal" size. Stocks with >$30M normal dollar vol are large-caps.
                prev_db = snap.get("prevDailyBar") or {}
                prev_vol = _sf(prev_db.get("v"))
                normal_vol = prev_vol if prev_vol is not None else vol
                if normal_vol and price * normal_vol > 30_000_000:
                    continue
                candidates.append((dollar_vol, sym))
            except Exception:
                continue

    candidates.sort(key=lambda x: x[0], reverse=True)
    result = [sym for _, sym in candidates[:max_candidates]]
    penny_count = sum(1 for sym in result if True)  # count done at scoring
    log.info(f"premover_universe: {len(result)} candidates passed price+volume filter (incl. pennies <$1)")
    return result


# ---------------------------------------------------------------------------
# Float + short interest (Yahoo Finance — free, no key needed)
# ---------------------------------------------------------------------------

def _get_float_short_data(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch float shares and short interest % from Yahoo Finance.

    Returns {SYM: {"float_shares": int, "short_pct_float": float, "short_ratio": float}}
    """
    import requests as _req

    result: Dict[str, Dict[str, Any]] = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; stackiq-scanner/1.0)",
        "Accept": "application/json",
    }

    for sym in symbols:
        try:
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}"
            resp = _req.get(
                url,
                params={"modules": "defaultKeyStatistics"},
                headers=headers,
                timeout=6,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            stats = (
                data.get("quoteSummary", {})
                .get("result", [{}])[0]
                .get("defaultKeyStatistics", {})
            )
            float_shares = _sf((stats.get("floatShares") or {}).get("raw"))
            shares_short = _sf((stats.get("sharesShort") or {}).get("raw"))
            short_pct = _sf((stats.get("shortPercentOfFloat") or {}).get("raw"))
            short_ratio = _sf((stats.get("shortRatio") or {}).get("raw"))

            if float_shares or short_pct:
                result[sym] = {
                    "float_shares": float_shares,
                    "short_pct_float": float(short_pct * 100) if short_pct else None,
                    "short_ratio": short_ratio,
                    "shares_short": shares_short,
                }
            time.sleep(0.12)  # be polite to Yahoo
        except Exception as e:
            log.debug(f"float_short: {sym} error: {e}")
            continue

    log.info(f"float_short: got data for {len(result)}/{len(symbols)} symbols")
    return result


# ---------------------------------------------------------------------------
# SEC EDGAR 8-K catalyst scanner (free public API)
# ---------------------------------------------------------------------------

def _get_sec_8k_filers(lookback_days: int = 2) -> Set[str]:
    """Return set of tickers that filed an 8-K in the last N days.

    Uses SEC EDGAR full-text search API — completely free, no key needed.
    8-K = material corporate event (earnings beat, contract, deal, FDA, etc.)
    """
    import requests as _req
    import xml.etree.ElementTree as _ET

    filers: Set[str] = set()
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # SEC EDGAR RSS feed of recent 8-K filings (public, no auth needed)
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        resp = _req.get(
            url,
            params={
                "action": "getcurrent",
                "type": "8-K",
                "dateb": "",
                "owner": "include",
                "count": "100",
                "output": "atom",
            },
            headers={"User-Agent": "stackiq-scanner contact@stackiq.ai"},
            timeout=10,
        )
        if resp.status_code != 200:
            return filers

        root = _ET.fromstring(resp.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            title = (entry.findtext("atom:title", default="", namespaces=ns) or "").upper()
            # Title format: "8-K - COMPANY NAME (TICKER) (CIK)"
            # Extract ticker from parentheses
            parts = title.split("(")
            for part in parts[1:]:  # skip first split (before first paren)
                tok = part.split(")")[0].strip()
                if tok and 1 <= len(tok) <= 5 and tok.isalpha():
                    filers.add(tok)
                    break
    except Exception as e:
        log.debug(f"sec_8k: fetch error: {e}")

    log.info(f"sec_8k: {len(filers)} companies filed 8-K recently")
    return filers


# ---------------------------------------------------------------------------
# Alpaca news check
# ---------------------------------------------------------------------------

def _check_news_alpaca(symbols: List[str]) -> Dict[str, bool]:
    """Return {symbol: True} for any symbol with news in last 3 days."""
    import requests as _req

    has_news: Dict[str, bool] = {}
    api_key = (os.getenv("ALPACA_API_KEY") or "").strip()
    secret = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    if not api_key or not secret:
        return has_news

    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret}
    url = "https://data.alpaca.markets/v1beta1/news"

    for i in range(0, len(symbols), 10):
        chunk = symbols[i: i + 10]
        try:
            resp = _req.get(
                url,
                headers=headers,
                params={"symbols": ",".join(chunk), "limit": 10, "sort": "desc"},
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
# Scorer — upgraded with float-adjusted volume + squeeze detection
# ---------------------------------------------------------------------------

def _score_symbol(
    symbol: str,
    snap: Dict[str, Any],
    bars: List[Dict[str, Any]],
    spy_closes: List[float],
    has_news: bool,
    float_data: Optional[Dict[str, Any]] = None,
    has_8k: bool = False,
    learned_weights: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    # learned_weights: multipliers from brain.get_learned_weights()
    # e.g. {"quiet_accumulation": 1.8, "vol_surge": 0.6, ...}
    lw = learned_weights or {}
    """Score a single symbol 0-100 for PRE-surge potential (the day BEFORE it runs).

    Key philosophy: we want volume building WITHOUT price already exploding.
    A stock up 50% today already ran. We want the setup, not the move.

    Scoring breakdown (max ~150 with all signals, normalized to 100):
      Quiet accumulation            25 pts   (vol building, price NOT yet moving — the pre-surge signal)
      Float-adjusted volume         30 pts   (vol/float — coil tightening)
      Short squeeze potential       20 pts   (short% × vol surge)
      ATR compression               20 pts   (spring loading)
      Near breakout level           10 pts   (within 5% of 20d high)
      8-K catalyst / news           15 pts   (event trigger)
      Micro-float bonus             10 pts   (sub-$1 + <5M float = math favor)
      RS vs SPY                      5 pts
      Already-running penalty      -25 pts   (up >15% today = too late, move started)
    """
    signals: Dict[str, Any] = {}
    raw_score = 0.0

    highs = _extract(bars, "h")
    lows = _extract(bars, "l")
    closes = _extract(bars, "c")
    vols = _extract(bars, "v")

    db = snap.get("dailyBar") or snap.get("day") or {}
    lt = snap.get("latestTrade") or snap.get("latestQuote") or {}
    price = _sf(db.get("c") or db.get("vw") or lt.get("p") or lt.get("ap"))

    day_open = _sf(db.get("o"))
    day_close = _sf(db.get("c") or db.get("vw"))
    prev_close = closes[-2] if len(closes) >= 2 else None

    cur_vol: Optional[float] = _sf(db.get("v"))
    if cur_vol is None and vols:
        cur_vol = vols[-1]

    avg20: Optional[float] = None
    vol_ratio: Optional[float] = None
    if cur_vol and len(vols) >= 6:
        avg20 = _mean(vols[-21:-1] if len(vols) >= 21 else vols[:-1]) or 1.0
        vol_ratio = cur_vol / max(avg20, 1.0)

    # Intraday move: open→close from the SAME snapshot (avoids bars/snapshot mismatch)
    intraday_chg: Optional[float] = None
    try:
        if day_open and day_open > 0 and day_close:
            intraday_chg = (day_close - day_open) / day_open
    except Exception:
        pass

    # Day-over-day change: today's close vs yesterday's close from bars
    today_chg_pct: Optional[float] = None
    try:
        if prev_close and prev_close > 0 and day_close:
            today_chg_pct = (day_close - prev_close) / prev_close
    except Exception:
        pass

    # --- ALREADY-RUNNING PENALTY: if up >15% today, the move already started ---
    try:
        chg = today_chg_pct if today_chg_pct is not None else intraday_chg
        if chg is not None and chg > 0.15:
            penalty = _clamp((chg - 0.15) / 0.35 * 25.0, 0.0, 25.0)
            raw_score -= penalty
            signals["already_running"] = {"penalty": round(-penalty, 1), "today_chg_pct": round(chg * 100, 1)}
    except Exception:
        pass

    # --- 1) Quiet accumulation (25 pts) — THE PRE-SURGE SIGNAL ---
    # Volume is 1.5-4x normal BUT price change today is small (<5%).
    # This is smart money loading before the move. This is what you see
    # the day before RXT, CLOV, JOBY ran. Volume with no price action = coiling.
    try:
        if vol_ratio is not None and today_chg_pct is not None:
            price_move_abs = abs(today_chg_pct)
            if vol_ratio >= 1.5 and price_move_abs < 0.05:
                # Log-scale so 5x vol ≠ 50x vol. 20x+ gets full credit, 5x gets ~60%.
                # log(vol+1)/log(21) maps: 1.5x→0, 5x→0.60, 10x→0.80, 20x→1.0, 50x→1.0
                import math as _math
                log_factor = min(_math.log(vol_ratio + 1) / _math.log(21), 1.0)
                accum_pts = log_factor * 25.0
                accum_pts *= lw.get("quiet_accumulation", 1.0)
                raw_score += accum_pts
                signals["quiet_accumulation"] = {
                    "pts": round(accum_pts, 1),
                    "vol_ratio": round(vol_ratio, 2),
                    "price_chg_pct": round(price_move_abs * 100, 1),
                }
            elif vol_ratio >= 1.5:
                import math as _math
                log_factor = min(_math.log(vol_ratio + 1) / _math.log(21), 1.0)
                partial = log_factor * 15.0
                partial *= lw.get("vol_surge", 1.0)
                raw_score += partial
                signals["vol_surge"] = {"pts": round(partial, 1), "ratio": round(vol_ratio, 2)}
    except Exception:
        pass

    # --- 2) Float-adjusted volume (30 pts) — THE KEY SIGNAL ---
    # If daily volume is a significant % of the float, the stock is being
    # repriced by the market. >30% float rotation almost always precedes
    # a major move. Low float = same volume = bigger % explosion.
    try:
        fd = float_data or {}
        float_shares = _sf(fd.get("float_shares"))
        if float_shares and float_shares > 0 and cur_vol:
            float_rotation = cur_vol / float_shares  # fraction of float trading today
            # 10% rotation = 10pts, 30% = 20pts, 50%+ = 30pts
            float_pts = _clamp(float_rotation / 0.5 * 30.0, 0.0, 30.0)
            float_pts *= lw.get("float_rotation", 1.0)
            raw_score += float_pts
            signals["float_rotation"] = {
                "pts": round(float_pts, 1),
                "rotation_pct": round(float_rotation * 100, 1),
                "float_shares_m": round(float_shares / 1_000_000, 2),
            }
    except Exception:
        pass

    # --- 3) Short squeeze potential (20 pts) ---
    # High short interest + building volume = explosive squeeze fuel.
    # short% > 20% AND vol_ratio > 2x → max points.
    try:
        fd = float_data or {}
        short_pct = _sf(fd.get("short_pct_float"))  # already multiplied by 100 (percent)
        if short_pct and vol_ratio:
            # short_pct=30%, vol_ratio=3x → raw = 0.9 → normalized
            squeeze_raw = (short_pct / 100.0) * min(vol_ratio, 5.0) / 1.5
            squeeze_pts = _clamp(squeeze_raw * 20.0, 0.0, 20.0)
            squeeze_pts *= lw.get("squeeze_potential", 1.0)
            raw_score += squeeze_pts
            signals["squeeze_potential"] = {
                "pts": round(squeeze_pts, 1),
                "short_pct": round(short_pct, 1),
                "days_to_cover": _sf(fd.get("short_ratio")),
            }
    except Exception:
        pass

    # --- 4) ATR compression (20 pts) ---
    try:
        if len(closes) >= 25:
            atr5 = _atr(highs, lows, closes, 5)
            atr20 = _atr(highs, lows, closes, 20)
            if atr5 and atr20 and atr20 > 0:
                ratio_atr = atr5 / atr20
                compress_pts = _clamp((1.25 - ratio_atr) / 0.5 * 20.0, 0.0, 20.0)
                compress_pts *= lw.get("atr_compression", 1.0)
                raw_score += compress_pts
                signals["atr_compression"] = {"pts": round(compress_pts, 1), "atr5_atr20": round(ratio_atr, 3)}
    except Exception:
        pass

    # --- 5) Close near high of day (10 pts) ---
    try:
        day_h = _sf(db.get("h"))
        day_l = _sf(db.get("l"))
        day_c = _sf(db.get("c"))
        if day_h is not None and day_l is not None and day_c is not None:
            rng = day_h - day_l
            if rng > 0:
                pos = (day_c - day_l) / rng
                close_pts = _clamp(pos * 10.0, 0.0, 10.0)
                close_pts *= lw.get("close_strength", 1.0)
                raw_score += close_pts
                signals["close_strength"] = {"pts": round(close_pts, 1), "position_in_range": round(pos, 3)}
    except Exception:
        pass

    # --- 6) Near 20-day high (10 pts) ---
    try:
        if len(closes) >= 20 and price is not None:
            high_20d = max(highs[-20:]) if len(highs) >= 20 else None
            if high_20d and high_20d > 0:
                pct_from_high = (high_20d - price) / high_20d
                breakout_pts = _clamp((0.10 - pct_from_high) / 0.10 * 10.0, 0.0, 10.0)
                breakout_pts *= lw.get("near_high", 1.0)
                raw_score += breakout_pts
                signals["near_high"] = {"pts": round(breakout_pts, 1), "pct_from_20d_high": round(pct_from_high * 100, 2)}
    except Exception:
        pass

    # --- 7) SEC 8-K catalyst or news (15 pts) ---
    # 8-K = hard material event → max pts. Generic news → partial credit.
    try:
        if has_8k:
            cat_pts = 15.0 * lw.get("catalyst", 1.0)
            raw_score += cat_pts
            signals["catalyst"] = {"pts": round(cat_pts, 1), "type": "sec_8k"}
        elif has_news:
            cat_pts = 8.0 * lw.get("catalyst", 1.0)
            raw_score += cat_pts
            signals["catalyst"] = {"pts": round(cat_pts, 1), "type": "news"}
    except Exception:
        pass

    # --- 8) RS vs SPY 3-day (5 pts) ---
    try:
        if len(closes) >= 4 and len(spy_closes) >= 4:
            stock_r3 = (closes[-1] - closes[-4]) / max(abs(closes[-4]), 0.01)
            spy_r3 = (spy_closes[-1] - spy_closes[-4]) / max(abs(spy_closes[-4]), 0.01)
            alpha = stock_r3 - spy_r3
            rs_pts = _clamp((alpha + 0.02) / 0.06 * 5.0, 0.0, 5.0)
            rs_pts *= lw.get("rs_vs_spy", 1.0)
            raw_score += rs_pts
            signals["rs_vs_spy"] = {"pts": round(rs_pts, 1), "alpha_3d_pct": round(alpha * 100, 2)}
    except Exception:
        pass

    # Entry zone and invalidation
    entry_zone: Optional[str] = None
    invalidation: Optional[str] = None
    try:
        if price is not None:
            atr5_val = _atr(highs, lows, closes, 5) if len(closes) >= 7 else None
            if atr5_val and atr5_val > 0:
                entry_zone = f"${round(price, 2)} – ${round(price * 1.05, 2)}"
                invalidation = f"${round(price - atr5_val * 1.5, 2)}"
            else:
                entry_zone = f"${round(price, 2)}"
                invalidation = f"${round(price * 0.92, 2)}"
    except Exception:
        pass

    # --- Micro-float bonus for penny stocks (unscored above, add here) ---
    # A $0.50 stock with a 2M float rotating 30% of float daily = 100%+ move coming.
    # This bonus ensures sub-$1 micro-floats aren't buried by higher-priced names.
    try:
        fd = float_data or {}
        float_shares_b = _sf(fd.get("float_shares"))
        if price is not None and price < 1.0 and float_shares_b and float_shares_b < 5_000_000 and cur_vol:
            micro_rotation = cur_vol / float_shares_b
            micro_bonus = _clamp(micro_rotation / 0.3 * 10.0, 0.0, 10.0)
            micro_bonus *= lw.get("micro_float_bonus", 1.0)
            raw_score += micro_bonus
            signals["micro_float_bonus"] = {"pts": round(micro_bonus, 1), "float_m": round(float_shares_b / 1_000_000, 2)}
    except Exception:
        pass

    # Normalize against what this stock could actually score, not the theoretical max.
    # If float data wasn't available, the float-dependent signals (30+20+10=60 pts)
    # could never fire — normalizing against 145 makes every stock look like a 40/100.
    # Instead, score against what was achievable given available data.
    has_float_data = bool(float_data and (float_data.get("float_shares") or float_data.get("short_pct_float")))
    is_penny_stock = bool(price is not None and price < 1.0)
    # No float data: max achievable = 25(accum)+20(atr)+10(close)+10(near_high)+15(catalyst)+5(rs) = 85
    # But in practice stocks rarely fire all — use 70 as practical max so good setups score 80+
    MAX_RAW = 145.0 if has_float_data else (130.0 if is_penny_stock else 70.0)
    normalized = _clamp(raw_score / MAX_RAW * 100.0, 0.0, 100.0)

    # Squeeze tag: flag high-conviction squeeze setups explicitly
    fd = float_data or {}
    short_pct_val = _sf(fd.get("short_pct_float"))
    float_shares_val = _sf(fd.get("float_shares"))
    is_squeeze_setup = bool(
        short_pct_val and short_pct_val >= 20.0
        and vol_ratio and vol_ratio >= 2.0
    )
    is_low_float = bool(float_shares_val and float_shares_val < 10_000_000)
    is_penny = bool(price is not None and price < 1.0)

    # --- Data-driven tags (no external API needed) ---
    # HIGH VOL: volume 20x+ normal = genuinely unusual activity
    is_high_vol = bool(vol_ratio and vol_ratio >= 20.0)
    # COILED: ATR5/ATR20 < 0.70 = very tight compression, spring loaded
    atr_ratio_val: Optional[float] = None
    try:
        if len(closes) >= 25:
            atr5_t = _atr(highs, lows, closes, 5)
            atr20_t = _atr(highs, lows, closes, 20)
            if atr5_t and atr20_t and atr20_t > 0:
                atr_ratio_val = atr5_t / atr20_t
    except Exception:
        pass
    is_coiled = bool(atr_ratio_val and atr_ratio_val < 0.70)
    # BREAKOUT: within 3% of 20-day high
    is_near_breakout = bool(
        len(highs) >= 20 and price is not None and
        max(highs[-20:]) > 0 and
        (max(highs[-20:]) - price) / max(highs[-20:]) <= 0.03
    )

    tags: List[str] = []
    if is_penny:
        tags.append("penny")
    if is_high_vol:
        tags.append("high_vol")
    if is_coiled:
        tags.append("coiled")
    if is_near_breakout:
        tags.append("breakout")
    if is_squeeze_setup:
        tags.append("squeeze")
    if is_low_float:
        tags.append("low_float")
    if has_8k:
        tags.append("8K_catalyst")
    elif has_news:
        tags.append("news")

    return {
        "symbol": symbol,
        "score": round(normalized, 1),
        "raw_score": round(raw_score, 1),
        "price": price,
        "float_m": round(float_shares_val / 1_000_000, 2) if float_shares_val else None,
        "short_pct": round(short_pct_val, 1) if short_pct_val else None,
        "signals": signals,
        "tags": tags,
        "entry_zone": entry_zone,
        "invalidation": invalidation,
    }


# ---------------------------------------------------------------------------
# Sector clustering — detects when a sector is setting up en masse
# ---------------------------------------------------------------------------

_SECTOR_MAP: Dict[str, str] = {
    # Crypto miners
    "MARA": "crypto", "RIOT": "crypto", "CLSK": "crypto", "BTBT": "crypto",
    "HUT": "crypto", "HIVE": "crypto", "CIFR": "crypto", "WULF": "crypto",
    "IREN": "crypto", "CORZ": "crypto",
    # EV / clean energy
    "NKLA": "ev", "RIDE": "ev", "GOEV": "ev", "SOLO": "ev", "AYRO": "ev",
    "BLNK": "ev", "CHPT": "ev", "EVGO": "ev", "PTRA": "ev", "XOS": "ev",
    # eVTOL / air mobility
    "JOBY": "evtol", "ACHR": "evtol", "LILM": "evtol", "EHANG": "evtol",
    "BLADE": "evtol", "SKYX": "evtol",
    # Uranium / nuclear
    "DNN": "uranium", "UAMY": "uranium", "MP": "uranium", "NXE": "uranium",
    "UEC": "uranium", "LTBR": "uranium",
    # AI / robotics
    "SOUN": "ai", "BBAI": "ai", "IONQ": "ai", "QUBT": "ai", "RGTI": "ai",
    "QBTS": "ai", "ARQQ": "ai", "AITX": "ai", "NVTS": "ai",
}


def _detect_hot_sectors(results: List[Dict[str, Any]]) -> List[str]:
    """Return sector names where 3+ stocks scored >= 60."""
    from collections import Counter
    sector_counts: Counter = Counter()
    for r in results:
        sym = r.get("symbol", "")
        if r.get("score", 0) >= 60:
            sector = _SECTOR_MAP.get(sym)
            if sector:
                sector_counts[sector] += 1
    return [s for s, cnt in sector_counts.items() if cnt >= 3]


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def run_premover_scan(
    scan_universe: List[str],
    max_results: int = 25,
    news_top_k: int = 50,
    max_seconds: float = 300.0,
) -> Dict[str, Any]:
    """Scan for pre-movers. Returns top candidates sorted by score desc."""
    from data_fetcher import get_snapshots_batch, get_bars_batch

    t0 = time.time()

    # --- Step 1: Build small-cap universe ---
    universe = build_smallcap_universe(scan_universe, max_candidates=400)
    if not universe:
        return {"results": [], "scanned": 0, "elapsed": 0.0, "error": "empty_universe"}

    # --- Step 2: Fetch snapshots ---
    log.info(f"premover_scan: fetching snapshots for {len(universe)} candidates")
    snapmap: Dict[str, Any] = {}
    for i in range(0, len(universe), 200):
        if time.time() - t0 > max_seconds * 0.35:
            break
        chunk = universe[i: i + 200]
        try:
            snapmap.update(get_snapshots_batch(chunk) or {})
        except Exception as e:
            log.warning(f"premover_scan: snapshot error: {e}")

    # --- Step 3: Volume-surge pre-filter to top 150 ---
    vol_scored: List[tuple] = []
    for sym in universe:
        snap = snapmap.get(sym)
        if not isinstance(snap, dict):
            continue
        db = snap.get("dailyBar") or snap.get("day") or {}
        price = _sf(db.get("c") or db.get("vw"))
        if price is None or price < 0.10 or price > 20.0:
            continue
        vol = _sf(db.get("v"))
        if not vol or vol <= 0:
            continue
        prev_vol = _sf(db.get("pv"))
        ratio = (vol / max(prev_vol, 1.0)) if prev_vol and prev_vol > 0 else 1.0
        vol_scored.append((ratio, sym, snap))

    vol_scored.sort(key=lambda x: x[0], reverse=True)
    top_candidates = [(sym, snap) for _, sym, snap in vol_scored[:150]]
    log.info(f"premover_scan: {len(top_candidates)} candidates after vol-surge pre-filter")

    if not top_candidates:
        return {"results": [], "scanned": 0, "elapsed": round(time.time() - t0, 2), "error": "no_candidates"}

    syms_to_score = [s for s, _ in top_candidates]

    # --- Step 4: Fetch daily bars ---
    bars_map: Dict[str, List[Dict]] = {}
    log.info(f"premover_scan: fetching daily bars for {len(syms_to_score)} symbols")
    for i in range(0, len(syms_to_score), 100):
        if time.time() - t0 > max_seconds * 0.60:
            break
        chunk = syms_to_score[i: i + 100]
        try:
            bars_map.update(get_bars_batch(chunk, "1Day", 30) or {})
        except Exception as e:
            log.warning(f"premover_scan: bars error: {e}")

    spy_closes: List[float] = []
    try:
        spy_data = get_bars_batch(["SPY"], "1Day", 30)
        spy_closes = [b["c"] for b in (spy_data.get("SPY") or []) if isinstance(b, dict) and _sf(b.get("c"))]
    except Exception:
        pass

    # --- Step 5: SEC 8-K catalyst scan (free, public) ---
    sec_8k_filers: Set[str] = set()
    if time.time() - t0 < max_seconds * 0.70:
        try:
            log.info("premover_scan: scanning SEC EDGAR for recent 8-K filings")
            sec_8k_filers = _get_sec_8k_filers(lookback_days=2)
        except Exception as e:
            log.warning(f"premover_scan: SEC 8-K scan error: {e}")

    # --- Step 6: News check ---
    has_news_map: Dict[str, bool] = {}
    if time.time() - t0 < max_seconds * 0.75:
        try:
            has_news_map = _check_news_alpaca(syms_to_score[:news_top_k])
        except Exception as e:
            log.warning(f"premover_scan: news error: {e}")

    # --- Step 7: Float + short interest for top 60 candidates ---
    # Only fetch for top candidates (Yahoo has rate limits).
    float_map: Dict[str, Dict] = {}
    float_fetch_syms = syms_to_score[:60]
    if time.time() - t0 < max_seconds * 0.80:
        try:
            log.info(f"premover_scan: fetching float/short data for {len(float_fetch_syms)} symbols")
            float_map = _get_float_short_data(float_fetch_syms)
        except Exception as e:
            log.warning(f"premover_scan: float/short error: {e}")

    # --- Step 8: Load learned weights from brain ---
    learned_weights: Dict[str, float] = {}
    try:
        from brain import get_learned_weights
        learned_weights = get_learned_weights()
        if learned_weights:
            log.info(f"premover_scan: loaded {len(learned_weights)} learned signal weights from brain")
    except Exception as e:
        log.debug(f"premover_scan: brain weights unavailable: {e}")

    # --- Step 9: Score ---
    results: List[Dict[str, Any]] = []
    for sym, snap in top_candidates:
        bars = bars_map.get(sym) or []
        result = _score_symbol(
            symbol=sym,
            snap=snap,
            bars=bars,
            spy_closes=spy_closes,
            has_news=has_news_map.get(sym, False),
            float_data=float_map.get(sym),
            has_8k=(sym in sec_8k_filers),
            learned_weights=learned_weights,
        )
        if result["score"] >= 15.0:
            results.append(result)

    results.sort(key=lambda x: x["score"], reverse=True)

    # --- Step 10: Record picks to brain for outcome tracking ---
    try:
        from brain import record_premover_pick
        for r in results[:max_results]:
            record_premover_pick(r)
        log.info(f"premover_scan: recorded {min(len(results), max_results)} picks to brain")
    except Exception as e:
        log.debug(f"premover_scan: brain record error: {e}")

    # Hot sector detection
    hot_sectors = _detect_hot_sectors(results)
    if hot_sectors:
        log.info(f"premover_scan: hot sectors detected: {hot_sectors}")

    elapsed = round(time.time() - t0, 2)
    log.info(
        f"premover_scan: done | candidates={len(top_candidates)} scored={len(results)} "
        f"top={results[0]['symbol'] if results else 'none'} "
        f"8k_filers={len(sec_8k_filers)} float_data={len(float_map)} elapsed={elapsed}s"
    )

    return {
        "results": results[:max_results],
        "scanned": len(top_candidates),
        "elapsed": elapsed,
        "hot_sectors": hot_sectors,
        "ts": time.time(),
    }


# ---------------------------------------------------------------------------
# Cache accessors
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
