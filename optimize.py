# optimize.py
from typing import Dict, Any, List, Tuple
import copy
import engine
import backtest as bt

def _variants(base: Dict[str, float], scale: float) -> List[Dict[str, float]]:
    # multiply each weight by {1-scale, 1, 1+scale} one at a time and jointly
    variants = []
    knobs = list(base.keys())
    mults = [1.0 - scale, 1.0, 1.0 + scale]
    # single-knob variants
    for k in knobs:
        for m in mults:
            v = copy.deepcopy(base)
            v[k] = max(0.0, v[k] * m)
            variants.append(v)
    # global scaling
    for m in mults:
        v = {k: max(0.0, base[k] * m) for k in knobs}
        variants.append(v)
    # normalize weights to sum ~1.0
    out = []
    for v in variants:
        s = sum(v.values()) or 1.0
        out.append({k: v[k]/s for k in v})
    # unique by tuple
    seen = set()
    uniq = []
    for x in out:
        key = tuple(round(x[k],4) for k in sorted(x.keys()))
        if key not in seen:
            seen.add(key)
            uniq.append(x)
    return uniq[:50]

def grid_search(symbols: List[str], start: str, end: str, budget: float, hold_days: int,
                grid_scale: float = 0.2, top_k: int = 5) -> Dict[str, Any]:
    base = engine.COMP_WEIGHTS.copy()
    cand_weights = _variants(base, grid_scale)
    thresholds = [(65.0,35.0),(67.0,33.0),(70.0,30.0)]
    trials: List[Dict[str, Any]] = []

    # try baseline first
    engine.COMP_WEIGHTS.update(base)
    base_res = []
    for s in symbols:
        try:
            base_res.append(bt.run_backtest(s, start, end, budget, hold_days, 67.0, 33.0, False))
        except Exception:
            pass
    base_agg = bt.aggregate_metrics([r for r in base_res if r.get("metrics")])
    trials.append({"weights": base, "buy": 67.0, "sell": 33.0, "metrics": base_agg})

    # grid
    for w in cand_weights:
        engine.COMP_WEIGHTS.update(w)
        for buy,sell in thresholds:
            per = []
            for s in symbols:
                try:
                    per.append(bt.run_backtest(s, start, end, budget, hold_days, buy, sell, False))
                except Exception:
                    pass
            agg = bt.aggregate_metrics([r for r in per if r.get("metrics")])
            if agg:
                trials.append({"weights": w, "buy": buy, "sell": sell, "metrics": agg})

    # ranking by Sharpe, then CAGR
    def score(tr):
        m = tr.get("metrics", {})
        return (m.get("sharpe", 0.0), m.get("CAGR", 0.0))
    trials.sort(key=score, reverse=True)
    top = trials[:top_k]
    best = top[0] if top else {}
    # restore engine weights to best
    if best:
        engine.COMP_WEIGHTS.update(best["weights"])
    return {"best": best, "top": top, "trials": len(trials)}
