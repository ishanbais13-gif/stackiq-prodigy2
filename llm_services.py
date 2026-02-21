import json
from typing import Any, Dict, List

from llm_client import call_llm_text, LLMDisabledError
from llm_prompts import ANALYZE_NEWS_SYSTEM, BEST_PICK_SYSTEM


def _safe_json_loads(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        # last-ditch: strip code fences
        s2 = s.strip()
        if s2.startswith("```"):
            s2 = s2.strip("`").strip()
        return json.loads(s2)


def llm_news_sentiment(symbol: str, headlines: List[str]) -> Dict[str, Any]:
    """
    Takes raw headlines list, returns:
      { direction, summary, headlines }
    Never throws: always returns something usable.
    """
    headlines = [h for h in (headlines or []) if isinstance(h, str) and h.strip()]
    if not headlines:
        return {
            "direction": "NEUTRAL",
            "summary": "unavailable",
            "macro_bias": "NEUTRAL",
            "sector_bias": "NEUTRAL",
            "trade_impact": "NO_EDGE",
            "headlines": [],
            "risk_flags": [],
        }

    user = json.dumps(
        {
            "symbol": symbol,
            "headlines": headlines[:12],
        },
        ensure_ascii=False,
    )

    try:
        out = call_llm_text(system=ANALYZE_NEWS_SYSTEM, user=user)
        data = _safe_json_loads(out)

        direction = str(data.get("direction", "NEUTRAL")).upper()
        if direction not in ("BULLISH", "BEARISH", "NEUTRAL"):
            direction = "NEUTRAL"

        summary = data.get("summary")
        if not isinstance(summary, str):
            summary = "unavailable"

        macro_bias = str(data.get("macro_bias") or "NEUTRAL").strip().upper()
        if macro_bias not in ("RISK_ON", "RISK_OFF", "NEUTRAL"):
            macro_bias = "NEUTRAL"

        sector_bias = str(data.get("sector_bias") or "NEUTRAL").strip().upper()
        if sector_bias not in ("TAILWIND", "HEADWIND", "NEUTRAL"):
            sector_bias = "NEUTRAL"

        trade_impact = str(data.get("trade_impact") or "NO_EDGE").strip().upper()
        if trade_impact not in ("SUPPORTS_LONG", "SUPPORTS_SHORT", "NO_EDGE"):
            trade_impact = "NO_EDGE"

        hl = data.get("headlines")
        if not isinstance(hl, list):
            hl = []
        hl2 = [str(x).strip() for x in hl if str(x).strip()][:8]

        risk_flags = data.get("risk_flags")
        if not isinstance(risk_flags, list):
            risk_flags = []
        rf2 = [str(x).strip() for x in risk_flags if str(x).strip()][:6]

        return {
            "direction": direction,
            "summary": summary.strip(),
            "macro_bias": macro_bias,
            "sector_bias": sector_bias,
            "trade_impact": trade_impact,
            "headlines": hl2,
            "risk_flags": rf2,
        }

    except LLMDisabledError:
        return {
            "direction": "NEUTRAL",
            "summary": "unavailable",
            "macro_bias": "NEUTRAL",
            "sector_bias": "NEUTRAL",
            "trade_impact": "NO_EDGE",
            "headlines": headlines[:8],
            "risk_flags": [],
        }
    except Exception:
        return {
            "direction": "NEUTRAL",
            "summary": "unavailable",
            "macro_bias": "NEUTRAL",
            "sector_bias": "NEUTRAL",
            "trade_impact": "NO_EDGE",
            "headlines": headlines[:8],
            "risk_flags": [],
        }


def analyze_news(symbol: str, headlines: List[str]) -> Dict[str, Any]:
    return llm_news_sentiment(symbol, headlines)


def llm_best_pick_from_candidates(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    candidates: list of dicts (symbol + your computed features)
    returns: { symbol, reason, confidence }
    """
    clean = []
    for c in candidates or []:
        if not isinstance(c, dict):
            continue
        sym = c.get("symbol")
        if isinstance(sym, str) and sym.strip():
            clean.append(c)
    clean = clean[:50]

    if not clean:
        return {"symbol": "SPY", "reason": "No candidates provided.", "confidence": 0.4}

    user = json.dumps({"candidates": clean}, ensure_ascii=False)

    try:
        out = call_llm_text(system=BEST_PICK_SYSTEM, user=user, max_output_tokens=650)
        data = _safe_json_loads(out)

        sym = data.get("symbol")
        if not isinstance(sym, str) or not sym.strip():
            sym = clean[0].get("symbol", "SPY")

        reason = data.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            reason = "Selected based on candidate features."

        conf = data.get("confidence")
        try:
            conf_f = float(conf)
        except Exception:
            conf_f = 0.55
        conf_f = max(0.0, min(1.0, conf_f))

        return {"symbol": sym.strip().upper(), "reason": reason.strip(), "confidence": conf_f}

    except LLMDisabledError:
        return {
            "symbol": str(clean[0].get("symbol", "SPY")).upper(),
            "reason": "Using deterministic top candidate.",
            "confidence": 0.55,
        }
    except Exception:
        return {
            "symbol": str(clean[0].get("symbol", "SPY")).upper(),
            "reason": "LLM failed; using deterministic top candidate.",
            "confidence": 0.5,
        }
