"""
Standardized response formatting and data contracts for Aurexis API.
Ensures consistent data structure, formatting, and timestamps across all endpoints.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
import time
import re
import logging

log = logging.getLogger(__name__)

# Global cache for analysis responses to ensure single source of truth
_ANALYSIS_CACHE: Dict[str, Dict[str, Any]] = {}
_ANALYSIS_CACHE_TTL = 60  # 1 minute — keep analyze scores fresh

def _is_warrant(symbol: str) -> bool:
    """Check if a ticker is a warrant (ends with W or contains -W)"""
    if not symbol:
        return False
    sym = str(symbol).strip().upper()
    return sym.endswith('W') or '-W' in sym

def _round_price(price: Any) -> Optional[float]:
    """Round price to 2 decimal places"""
    try:
        if price is None:
            return None
        p = float(price)
        return round(p, 2)
    except Exception:
        return None

def _round_score(score: Any) -> Optional[float]:
    """Round score to 1 decimal place, ensure 0-100 scale"""
    try:
        if score is None:
            return None
        s = float(score)
        # If score is on 0-10 scale, convert to 0-100
        if s <= 10:
            s = s * 10
        return round(max(0, min(100, s)), 1)
    except Exception:
        return None

def _get_data_freshness(market_data_timestamp: Optional[float] = None) -> str:
    """Determine if market data is fresh or stale"""
    try:
        if market_data_timestamp is None:
            market_data_timestamp = time.time()
        
        now = time.time()
        age_minutes = (now - market_data_timestamp) / 60
        
        if age_minutes > 15:
            return "stale"
        elif age_minutes > 5:
            return "aging"
        else:
            return "fresh"
    except Exception:
        return "unknown"

def _get_iso_timestamp(ts: Optional[float] = None) -> str:
    """Get ISO 8601 timestamp"""
    if ts is None:
        ts = time.time()
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

def _get_market_data_timestamp(snapshots: Dict[str, Any] = None, bars: Dict[str, Any] = None) -> float:
    """Get the most recent timestamp from market data"""
    latest_ts = 0.0
    
    try:
        # Check snapshots
        if isinstance(snapshots, dict):
            for symbol, snap in snapshots.items():
                if isinstance(snap, dict):
                    # Check latest trade timestamp
                    lt = snap.get("latestTrade")
                    if isinstance(lt, dict) and lt.get("t"):
                        try:
                            lt_ts = float(lt["t"]) / 1000  # Convert from ms if needed
                            latest_ts = max(latest_ts, lt_ts)
                        except Exception:
                            pass
                    
                    # Check daily bar timestamp
                    bar = snap.get("dailyBar")
                    if isinstance(bar, dict) and bar.get("t"):
                        try:
                            bar_ts = float(bar["t"]) / 1000
                            latest_ts = max(latest_ts, bar_ts)
                        except Exception:
                            pass
        
        # Check bars
        if isinstance(bars, dict):
            for symbol, symbol_bars in bars.items():
                if isinstance(symbol_bars, list) and symbol_bars:
                    latest_bar = symbol_bars[-1]
                    if isinstance(latest_bar, dict) and latest_bar.get("t"):
                        try:
                            bar_ts = float(latest_bar["t"]) / 1000
                            latest_ts = max(latest_ts, bar_ts)
                        except Exception:
                            pass
    except Exception as e:
        log.warning(f"Error getting market data timestamp: {e}")
    
    return latest_ts or time.time()

def _standardize_analysis_response(raw_response: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Standardize analysis response to contract format"""
    try:
        # Get market data timestamp
        snapshots = raw_response.get("market_data", {}).get("snapshots", {})
        bars = raw_response.get("market_data", {}).get("bars", {})
        market_ts = _get_market_data_timestamp(snapshots, bars)
        
        # Extract and standardize scores
        ai_score_raw = raw_response.get("ai_score_0_100") or raw_response.get("ai_score")
        execution_score_raw = raw_response.get("execution_score_0_100") or raw_response.get("execution_score")
        confidence_raw = raw_response.get("confidence_0_100") or raw_response.get("confidence")
        
        # Extract trade plan
        trade_plan = raw_response.get("trade_plan", {})
        entry_price = _round_price(trade_plan.get("entry"))
        stop_price = _round_price(trade_plan.get("stop"))
        targets_raw = trade_plan.get("targets", [])
        targets = [_round_price(t) for t in targets_raw if t is not None][:3]  # Max 3 targets
        
        # Ensure we have exactly 3 target slots
        while len(targets) < 3:
            targets.append(None)
        
        # Standardize response
        standardized = {
            # Core analysis data
            "symbol": str(symbol).strip().upper(),
            "aiScore": _round_score(ai_score_raw),
            "executionScore": _round_score(execution_score_raw),
            "confidence": _round_score(confidence_raw),
            
            # Trade plan
            "entryPrice": entry_price,
            "stopPrice": stop_price,
            "targets": targets,
            
            # Supporting data
            "technicals": raw_response.get("technical_analysis", {}),
            "executionPlan": raw_response.get("execution_plan", {}),
            "newsAndSentiment": raw_response.get("news_sentiment", {}),
            
            # Metadata
            "analysisTimestamp": _get_iso_timestamp(),
            "generatedAt": _get_iso_timestamp(),
            "dataAsOf": _get_iso_timestamp(market_ts),
            "dataFreshness": _get_data_freshness(market_ts),
            
            # Raw response for debugging
            "_raw": raw_response
        }
        
        return standardized
        
    except Exception as e:
        log.error(f"Error standardizing analysis response for {symbol}: {e}")
        return _create_error_response("standardization_error", str(e), False)

def _create_error_response(error_code: str, message: str, retryable: bool) -> Dict[str, Any]:
    """Create standardized error response"""
    return {
        "error": {
            "code": error_code,
            "message": message,
            "retryable": retryable
        },
        "generatedAt": _get_iso_timestamp(),
        "dataAsOf": _get_iso_timestamp()
    }

def _add_response_metadata(response: Dict[str, Any], market_data_timestamp: Optional[float] = None) -> Dict[str, Any]:
    """Add standard metadata to any API response"""
    if not isinstance(response, dict):
        response = {}
    
    # Don't overwrite existing error structure
    if "error" not in response:
        response.update({
            "generatedAt": _get_iso_timestamp(),
            "dataAsOf": _get_iso_timestamp(market_data_timestamp),
            "dataFreshness": _get_data_freshness(market_data_timestamp)
        })
    
    return response

def _cache_analysis(symbol: str, response: Dict[str, Any]) -> None:
    """Cache analysis response for single source of truth"""
    try:
        cache_key = str(symbol).strip().upper()
        _ANALYSIS_CACHE[cache_key] = {
            "data": response,
            "cached_at": time.time()
        }
    except Exception as e:
        log.error(f"Error caching analysis for {symbol}: {e}")

def _get_cached_analysis(symbol: str) -> Optional[Dict[str, Any]]:
    """Get cached analysis response if still valid"""
    try:
        cache_key = str(symbol).strip().upper()
        cached = _ANALYSIS_CACHE.get(cache_key)
        
        if not cached:
            return None
        
        # Check if cache is still valid
        if time.time() - cached["cached_at"] > _ANALYSIS_CACHE_TTL:
            del _ANALYSIS_CACHE[cache_key]
            return None
        
        return cached["data"]
    except Exception as e:
        log.error(f"Error getting cached analysis for {symbol}: {e}")
        return None

def _invalidate_analysis_cache(symbol: str = None) -> None:
    """Invalidate cache for a symbol or all symbols"""
    try:
        if symbol:
            cache_key = str(symbol).strip().upper()
            _ANALYSIS_CACHE.pop(cache_key, None)
        else:
            _ANALYSIS_CACHE.clear()
    except Exception as e:
        log.error(f"Error invalidating analysis cache: {e}")

def _standardize_movers_response(raw_movers: List[Dict[str, Any]], min_volume: int = 10000, min_price: float = 1.00, exclude_warrants: bool = False) -> Dict[str, Any]:
    """Standardize movers response with warrant detection and filtering"""
    try:
        filtered_movers = []
        
        for mover in raw_movers:
            if not isinstance(mover, dict):
                continue
            
            symbol = str(mover.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            
            # Check if warrant
            is_warrant = _is_warrant(symbol)
            if exclude_warrants and is_warrant:
                continue
            
            # Apply filters
            price = mover.get("price") if mover.get("price") is not None else mover.get("last")
            volume = mover.get("volume") if mover.get("volume") is not None else mover.get("v")

            try:
                price_float = float(price) if price is not None else None
            except Exception:
                price_float = None
            try:
                volume_int = int(float(volume)) if volume is not None else None
            except Exception:
                volume_int = None

            if price_float is not None and price_float < min_price:
                continue
            if volume_int is not None and volume_int < min_volume:
                continue
            
            # Standardize mover data
            cp_raw = (mover.get("changePercent") or mover.get("pct_change")
                      or mover.get("change_percent") or mover.get("cp"))
            try:
                cp_val = float(cp_raw) if cp_raw is not None else None
            except Exception:
                cp_val = None
            standardized_mover = {
                "symbol": symbol,
                "last": _round_price(price),
                "price": _round_price(price),
                "change": _round_price(mover.get("change")),
                "changePercent": cp_val,
                "pct_change": cp_val,
                "change_percent": cp_val,
                "volume": volume_int,
                "isWarrant": is_warrant,
                "updated_at": mover.get("updated_at", _get_iso_timestamp())
            }
            
            filtered_movers.append(standardized_mover)
        
        # Sort by change percent descending
        filtered_movers.sort(key=lambda x: x.get("changePercent", 0) or 0, reverse=True)
        
        return {
            "movers": filtered_movers,
            "count": len(filtered_movers),
            "filters": {
                "minVolume": min_volume,
                "minPrice": min_price,
                "excludeWarrants": exclude_warrants
            },
            "generatedAt": _get_iso_timestamp(),
            "dataAsOf": _get_iso_timestamp()
        }
        
    except Exception as e:
        log.error(f"Error standardizing movers response: {e}")
        return _create_error_response("movers_standardization_error", str(e), False)

def _standardize_account_response(raw_account: Dict[str, Any], is_paper: bool = False) -> Dict[str, Any]:
    """Standardize account response with paper/live mode"""
    try:
        standardized = {
            "mode": "paper" if is_paper else "live",
            "status": raw_account.get("status", "unknown"),
            "cash": _round_price(raw_account.get("cash")),
            "equity": _round_price(raw_account.get("equity")),
            "portfolioValue": _round_price(raw_account.get("portfolio_value") or raw_account.get("account_value")),
            "buyingPower": _round_price(raw_account.get("buying_power")),
            "accountValue": _round_price(raw_account.get("account_value") or raw_account.get("portfolio_value")),
            "generatedAt": _get_iso_timestamp(),
            "dataAsOf": _get_iso_timestamp()
        }
        
        return standardized
        
    except Exception as e:
        log.error(f"Error standardizing account response: {e}")
        return _create_error_response("account_standardization_error", str(e), False)
