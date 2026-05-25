from __future__ import annotations

from typing import Any, Dict


def translate_to_human(result: dict) -> dict:
    try:
        def _normalize_input(result: dict):
            if not isinstance(result, dict):
                return {
                    "symbol": "",
                    "confidence": 0.0,
                    "technical": 0.0,
                    "execution": 0.0,
                    "trade_plan": {},
                }

            # detect analyze response
            if "technicals" in result:
                technicals = result.get("technicals", {}) if isinstance(result.get("technicals"), dict) else {}
                best_pick = result.get("best_pick", {}) if isinstance(result.get("best_pick"), dict) else {}
                trade_plan = result.get("trade_plan", {}) if isinstance(result.get("trade_plan"), dict) else {}

                try:
                    conf = float(best_pick.get("confidence_0_100", 0) or 0) / 10.0
                except Exception:
                    conf = 0.0

                return {
                    "symbol": str(technicals.get("symbol", "") or ""),
                    "confidence": conf,
                    "technical": float(technicals.get("ai_score_10", 0) or 0),
                    "execution": float(technicals.get("execution_score_10", 0) or 0),
                    "trade_plan": trade_plan,
                }

            # best_pick_v2 format
            pillars = result.get("pillar_scores_0_10", {}) if isinstance(result.get("pillar_scores_0_10"), dict) else {}
            return {
                "symbol": str(result.get("symbol", "") or ""),
                "confidence": float(result.get("confidence_0_10", 0) or 0),
                "technical": float(pillars.get("technical", 0) or 0),
                "execution": float(result.get("execution_score_0_10", 0) or 0),
                "trade_plan": result.get("trade_plan", {}) if isinstance(result.get("trade_plan"), dict) else {},
            }

        data = _normalize_input(result)

        symbol = str(data.get("symbol") or "").strip() or "This stock"
        confidence = float(data.get("confidence", 0) or 0)
        tech = float(data.get("technical", 0) or 0)
        execution = float(data.get("execution", 0) or 0)
        trade_plan = data.get("trade_plan", {}) if isinstance(data.get("trade_plan"), dict) else {}

        confidence = max(0.0, min(10.0, round(confidence, 1)))

        if tech >= 8:
            trend_text = "strong upward momentum"
        elif tech >= 6:
            trend_text = "a healthy upward trend"
        elif tech >= 4:
            trend_text = "a developing setup with mixed signals"
        else:
            trend_text = "a weaker trend setup that relies more on timing than momentum"

        if execution >= 8:
            risk_text = "risk appears well controlled"
        elif execution >= 6:
            risk_text = "risk looks manageable"
        else:
            risk_text = "risk is elevated and requires caution"

        entry = trade_plan.get("entry")
        stop = trade_plan.get("stop")
        targets = trade_plan.get("targets", []) if isinstance(trade_plan.get("targets", []), list) else []

        action = (
            f"Consider buying near {entry}, exit if price falls below {stop}, "
            f"and take profits near {targets}."
            if entry and stop and targets else
            "Trade levels are still forming."
        )

        plain_summary = (
            f"{symbol} shows {trend_text}. "
            f"The trade structure suggests {risk_text}. "
            f"System confidence: {confidence}/10."
        )

        return {
            "plain_summary": plain_summary,
            "what_the_system_sees": "The system detected consistent buying pressure and favorable conditions.",
            "what_you_should_do": action,
            "what_could_go_wrong": "Market conditions can change quickly. Always follow risk limits.",
        }
    except Exception:
        return {}
