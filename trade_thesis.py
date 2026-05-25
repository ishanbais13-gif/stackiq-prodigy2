from __future__ import annotations


def build_trade_thesis(result: dict) -> dict:
    symbol = result.get("symbol", "") if isinstance(result, dict) else ""

    pillars = result.get("pillar_scores_0_10", {}) if isinstance(result, dict) and isinstance(result.get("pillar_scores_0_10"), dict) else {}
    tech = pillars.get("technical", 0)
    risk = pillars.get("risk_structure", 0)
    upside = pillars.get("upside", 0)

    trade = result.get("trade_plan", {}) if isinstance(result, dict) and isinstance(result.get("trade_plan"), dict) else {}
    entry = trade.get("entry")
    stop = trade.get("stop")
    targets = trade.get("targets", []) if isinstance(trade.get("targets"), list) else []

    confidence = result.get("confidence_0_10", 0) if isinstance(result, dict) else 0

    # Market interpretation
    if tech >= 8:
        setup_type = "strong momentum opportunity"
    elif tech >= 6:
        setup_type = "moderate trend setup"
    else:
        setup_type = "timing-dependent trade"

    if risk >= 8:
        risk_profile = "risk is tightly controlled"
    elif risk >= 6:
        risk_profile = "risk appears acceptable"
    else:
        risk_profile = "risk is elevated"

    if upside >= 8:
        reward_profile = "reward potential is attractive"
    elif upside >= 6:
        reward_profile = "reward potential is moderate"
    else:
        reward_profile = "upside may be limited"

    thesis = (
        f"{symbol} represents a {setup_type}. "
        f"The system believes {risk_profile} and {reward_profile}. "
        f"Overall confidence is {confidence}/10."
    )

    execution = (
        f"Plan: enter near {entry}, exit below {stop}, "
        f"take profits near {targets}."
        if entry and stop and targets else
        "Execution levels unavailable."
    )

    return {
        "thesis_summary": thesis,
        "execution_logic": execution,
        "system_reasoning": "This opportunity ranked highest after passing liquidity, price, and quality filters across the scanned market.",
        "risk_statement": "All trades carry risk. Position sizing and stop discipline remain essential.",
    }
