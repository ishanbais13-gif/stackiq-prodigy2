# backtest.py
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import statistics

import data_fetcher as df
import indicators as ta
import engine

def _iso(ts: int) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")

def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def _date_to_index(ts_list: List[int], date_str: str) -> int:
    # find first index >= date
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    t = int(dt.timestamp())
    for i, ts in enumerate(ts_list):
        if ts >= t: return i
    return len(ts_list)-1

def _features_series(c: List[float], h: List[float], l: List[float]) -> Dict[str, List[Optional[float]]]:
    sma20 = ta.sma(c,20); sma50 = ta.sma(c,50); sma200 = ta.sma(c,200)
    rsi14 = ta.rsi(c,14); atr14 = ta.atr(h,l,c,14)
    macd_l, macd_s, macd_h = ta.macd(c)
    bbp = ta.bollinger_percent(c,20,2.0)
    pdi, mdi, adx = ta.dmi_adx(h,l,c,14)

    # returns
    def series_ret(n:int) -> List[Optional[float]]:
        out: List[Optional[float]] = []
        for i in range(len(c)):
            if i <= n: out.append(None)
            else:
                prev = c[i-n]
                out.append(None if prev==0 else (c[i]-prev)/prev)
        return out

    r5 = series_ret(5); r20 = series_ret(20); r60 = series_ret(60)
    return {
        "sma20":sma20, "sma50":sma50, "sma200":sma200, "rsi14":rsi14, "atr":atr14,
        "macd":macd_l, "macd_sig":macd_s, "macd_hist":macd_h, "bbp":bbp,
        "pdi":pdi, "mdi":mdi, "adx":adx, "r5":r5, "r20":r20, "r60":r60
    }

def _snapshot_altdata(symbol: str) -> Tuple[Optional[float], Optional[float], bool]:
    """Current snapshot proxies — used throughout backtest (no historical API)."""
    rec = df.recommendation_trends(symbol) or {}
    buy = (rec.get("strongBuy") or 0) + (rec.get("buy") or 0)
    sell= (rec.get("strongSell") or 0) + (rec.get("sell") or 0)
    hold= (rec.get("hold") or 0)
    total = buy + sell + hold
    rec_bias = None if total==0 else (buy - sell)/max(total,1)

    news = df.news_sentiment(symbol) or {}
    bull = news.get("bullishPercent") or news.get("sentiment", {}).get("bullishPercent")
    news_bias = (bull/100.0 - 0.5)*2.0 if bull is not None else None

    # we cannot know historical earnings offsets reliably; default False
    earnings_guard = False
    return rec_bias, news_bias, earnings_guard

def run_backtest(symbol: str, start: str, end: str, budget: float, hold_days: int,
                 buy_th: float, sell_th: float, skip_earnings: bool) -> Dict[str, Any]:
    raw = df.candles(symbol, days=1500)  # ~6y daily
    t = raw["t"]; c = raw["c"]; h = raw["h"]; l = raw["l"]
    if not t or len(c)<220:
        raise RuntimeError("Not enough history")

    i_start = _date_to_index(t, start)
    i_end   = _date_to_index(t, end)
    if i_end <= i_start+220:
        raise RuntimeError("Backtest window too short (need >220 trading days for indicators)")

    feats = _features_series(c,h,l)
    rec_bias, news_bias, _earn = _snapshot_altdata(symbol)

    # walk-forward
    pos = None  # dict when open: {"side":"long"/"short","entry":float,"entry_idx":int,"atr":float,"target":float,"stop":float,"shares":int}
    trades = []
    equity = [1_0]  # start equity index at 10 just to avoid 0 std; we normalize later
    cash = budget
    eq = budget
    last_price = c[i_start]

    i = i_start
    while i <= i_end:
        price = c[i]; hi = h[i]; lo = l[i]; last_price = price

        # passive mark-to-market (we track only realized PnL; equity curve approximates using price move on open pos)
        if pos:
            if pos["side"]=="long":
                unreal = pos["shares"]*(price - pos["entry"])
            else:
                unreal = pos["shares"]*(pos["entry"] - price)
            eq = budget + sum(tr["pnl"] for tr in trades) + unreal
        else:
            eq = budget + sum(tr["pnl"] for tr in trades)
        equity.append(eq)

        # check exits if in position (stop/target or max hold)
        if pos:
            exit_flag = None
            # intraday stop/target check conservative (assume worst fills if both)
            if pos["side"]=="long":
                hit_stop = lo <= pos["stop"]
                hit_tgt  = hi >= pos["target"]
                if hit_stop and hit_tgt:
                    exit_price = pos["stop"]  # conservative
                    exit_flag = "stop"
                elif hit_stop:
                    exit_price = pos["stop"]; exit_flag = "stop"
                elif hit_tgt:
                    exit_price = pos["target"]; exit_flag = "target"
            else:  # short
                hit_stop = hi >= pos["stop"]
                hit_tgt  = lo <= pos["target"]
                if hit_stop and hit_tgt:
                    exit_price = pos["stop"]
                    exit_flag = "stop"
                elif hit_stop:
                    exit_price = pos["stop"]; exit_flag = "stop"
                elif hit_tgt:
                    exit_price = pos["target"]; exit_flag = "target"

            max_hold = (i - pos["entry_idx"]) >= hold_days
            if exit_flag or max_hold:
                if not exit_flag:
                    exit_price = price
                    exit_flag = "time"

                if pos["side"]=="long":
                    pnl = pos["shares"]*(exit_price - pos["entry"])
                    ret = (exit_price - pos["entry"])/pos["entry"]
                else:
                    pnl = pos["shares"]*(pos["entry"] - exit_price)
                    ret = (pos["entry"] - exit_price)/pos["entry"]

                trades.append({
                    "entry_date": _iso(t[pos["entry_idx"]]),
                    "exit_date": _iso(t[i]),
                    "side": pos["side"], "entry": round(pos["entry"],4),
                    "exit": round(exit_price,4),
                    "target": round(pos["target"],4), "stop": round(pos["stop"],4),
                    "shares": pos["shares"], "pnl": round(pnl,2),
                    "ret": round(ret,4), "reason": exit_flag
                })
                pos = None

        # open new position if flat
        if pos is None and i <= i_end:
            # build today's one-bar features
            k = i
            f = {
                "price": price,
                "sma20": feats["sma20"][k], "sma50": feats["sma50"][k], "sma200": feats["sma200"][k],
                "rsi14": feats["rsi14"][k], "atr": feats["atr"][k],
                "macd": feats["macd"][k], "macd_sig": feats["macd_sig"][k], "macd_hist": feats["macd_hist"][k],
                "bbp": feats["bbp"][k],
                "pdi": feats["pdi"][k], "mdi": feats["mdi"][k], "adx": feats["adx"][k],
                "r5": feats["r5"][k], "r20": feats["r20"][k], "r60": feats["r60"][k],
                "rec_bias": rec_bias, "news_bias": news_bias,
                "upcoming_earnings": False  # historical guard not available
            }
            ready = all(f.get(x) is not None for x in ["sma20","sma50","sma200","rsi14","atr"])
            if ready:
                conf,_ = engine.ensemble_score(f)
                # plan like engine.position_plan
                atr = f.get("atr") or 0.0
                atrp = (atr/price) if price else 0.0
                if atrp <= 0.01: R = 0.75
                elif atrp <= 0.03: R = 1.0
                elif atrp <= 0.06: R = 1.2
                else: R = 1.5

                if conf >= buy_th:
                    side = "long"
                elif conf <= sell_th:
                    side = "short"
                else:
                    side = None

                if side:
                    entry = price
                    target = entry + R*atr if side=="long" else entry - R*atr
                    stop   = entry - 1.0*atr if side=="long" else entry + 1.0*atr
                    stop_dist = abs(entry - stop) or (0.02*entry)
                    risk_cap = max(0.01 * budget, 10.0)
                    shares = int(min(risk_cap // stop_dist, budget // entry)) if entry>0 else 0
                    if shares > 0:
                        pos = {
                            "side": side, "entry": entry, "target": target, "stop": stop,
                            "atr": atr, "shares": shares, "entry_idx": i
                        }

        i += 1

    # metrics
    total_pnl = sum(tr["pnl"] for tr in trades)
    total_ret = total_pnl / budget if budget>0 else 0.0

    # equity series for drawdown/sharpe (normalize to start at 1.0)
    if len(equity) < 3:
        daily_returns = [0.0]
    else:
        start_eq = equity[0]
        eq_norm = [e / start_eq for e in equity]
        daily_returns = []
        for j in range(1, len(eq_norm)):
            prev = eq_norm[j-1]
            daily_returns.append(0.0 if prev==0 else (eq_norm[j]-prev)/prev)

    # Max drawdown
    peak = -1e9; max_dd = 0.0; cur = 1.0
    eq_curve = [1.0]
    for r in daily_returns:
        cur *= (1.0 + r)
        eq_curve.append(cur)
        peak = max(peak, cur)
        dd = 0.0 if peak==0 else (peak - cur)/peak
        max_dd = max(max_dd, dd)

    # Sharpe (simple, daily * sqrt(252))
    if len(daily_returns) >= 2:
        mu = statistics.mean(daily_returns)
        sd = statistics.pstdev(daily_returns) or 1e-9
        sharpe = (mu / sd) * (252 ** 0.5)
    else:
        sharpe = 0.0

    # CAGR
    days = max(1, (i_end - i_start))
    years = days / 252.0
    cagr = (eq_curve[-1] ** (1/years) - 1.0) if years > 0 and eq_curve[-1] > 0 else total_ret

    wins = [tr for tr in trades if tr["pnl"] > 0]
    losses = [tr for tr in trades if tr["pnl"] <= 0]
    win_rate = (len(wins) / len(trades)) if trades else 0.0
    avg_win = (statistics.mean([w["pnl"] for w in wins]) if wins else 0.0)
    avg_loss= (statistics.mean([l["pnl"] for l in losses]) if losses else 0.0)

    metrics = {
        "symbol": symbol.upper(),
        "trades": len(trades),
        "total_pnl": round(total_pnl,2),
        "total_return": round(total_ret,4),
        "CAGR": round(cagr,4),
        "max_drawdown": round(max_dd,4),
        "sharpe": round(sharpe,3),
        "win_rate": round(win_rate,3),
        "avg_win": round(avg_win,2),
        "avg_loss": round(avg_loss,2)
    }
    summary = [
        f"Trades {metrics['trades']}",
        f"PnL ${metrics['total_pnl']}",
        f"Ret {metrics['total_return']*100:.1f}%",
        f"CAGR {metrics['CAGR']*100:.1f}%",
        f"DD {metrics['max_drawdown']*100:.1f}%",
        f"Sharpe {metrics['sharpe']:.2f}",
        f"Win {metrics['win_rate']*100:.1f}%"
    ]
    return {"symbol": symbol.upper(), "metrics": metrics, "summary": summary, "trades": trades}

def aggregate_metrics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    ks = ["trades","total_pnl","total_return","CAGR","max_drawdown","sharpe","win_rate","avg_win","avg_loss"]
    agg: Dict[str, Any] = {}
    n = len(results) if results else 1
    for k in ks:
        vals = [r["metrics"][k] for r in results if r.get("metrics") and k in r["metrics"]]
        if not vals: continue
        if k in ("max_drawdown",):  # conservative: worst
            agg[k] = round(max(vals), 4)
        elif k in ("total_pnl",):
            agg[k] = round(sum(vals), 2)
        else:
            agg[k] = round(sum(vals)/len(vals), 4 if isinstance(vals[0], float) else 0)
    agg["symbols"] = [r["symbol"] for r in results if r.get("metrics")]
    return agg
