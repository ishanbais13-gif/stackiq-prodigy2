"""
Backend API enhancements to support Portfolio and Watchlist UI improvements.
Adds proper market status detection, timestamps, plan limits, and structured responses.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
import time
import os
import logging

log = logging.getLogger(__name__)

def get_market_status() -> Dict[str, Any]:
    """Get current market status with proper open/closed detection"""
    try:
        try:
            from data_fetcher import market_regime
        except ImportError:
            market_regime = None
        regime = (market_regime() if callable(market_regime) else None) or {}
        
        # Check if we're in market hours
        now = datetime.now(timezone.utc)
        eastern_tz = timezone(timedelta(hours=-5))  # Eastern Time (simplified, no DST)
        eastern_time = now.astimezone(eastern_tz)
        
        # Market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
        is_weekday = eastern_time.weekday() < 5  # 0=Monday, 6=Sunday
        market_open = eastern_time.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = eastern_time.replace(hour=16, minute=0, second=0, microsecond=0)
        
        is_market_hours = is_weekday and market_open <= eastern_time <= market_close
        
        return {
            "status": "open" if is_market_hours else "closed",
            "is_market_hours": is_market_hours,
            "current_time": eastern_time.isoformat(),
            "market_open": market_open.isoformat(),
            "market_close": market_close.isoformat(),
            "timezone": "ET",
            "next_open": market_open if eastern_time > market_close else market_open,
            "regime": regime.get("regime", "unknown"),
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        log.error(f"Error getting market status: {e}")
        return {
            "status": "unknown",
            "is_market_hours": False,
            "current_time": datetime.now(timezone.utc).isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }

def get_account_with_status() -> Dict[str, Any]:
    """Enhanced account response with proper paper/live distinction and market status"""
    try:
        # Get market status
        market_status = get_market_status()
        
        # Determine account mode
        is_paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
        
        # Get existing account data
        from app import trade_client, _broker_positions_sync, _safe_f, _position_field
        
        client = trade_client()
        a = client.get_account()
        
        def _num(v: Any) -> float:
            try:
                return float(v or 0.0)
            except Exception:
                return 0.0
        
        cash = _num(getattr(a, "cash", 0.0))
        equity = _num(getattr(a, "equity", 0.0))
        buying_power = _num(getattr(a, "buying_power", 0.0))
        portfolio_value = _num(getattr(a, "portfolio_value", 0.0))
        account_value = portfolio_value if portfolio_value > 0.0 else equity
        
        return {
            # Account info
            "mode": "paper" if is_paper else "live",
            "value": float(account_value),
            "status": str(getattr(a, "status", "") or ""),
            "cash": float(cash),
            "equity": float(equity),
            "portfolio_value": float(account_value),
            "buying_power": float(buying_power),
            "account_value": float(account_value),
            
            # Market status
            "market_status": market_status["status"],
            "is_market_hours": market_status["is_market_hours"],
            
            # Metadata
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "data_as_of": market_status.get("current_time"),
            "data_freshness": "fresh" if market_status["is_market_hours"] else "stale"
        }
        
    except Exception as e:
        # Fallback response
        is_paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
        market_status = get_market_status()
        
        return {
            "mode": "paper" if is_paper else "live",
            "value": 100000.0,
            "status": "degraded",
            "cash": 100000.0,  # Default paper trading amount
            "equity": 100000.0,
            "portfolio_value": 100000.0,
            "buying_power": 200000.0,
            "account_value": 100000.0,
            "market_status": market_status["status"],
            "is_market_hours": market_status["is_market_hours"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": f"Account fetch error: {str(e)}"
        }

def get_portfolio_with_metadata() -> Dict[str, Any]:
    """Enhanced portfolio response with timestamps and metadata"""
    try:
        # Get existing portfolio data
        from app import get_portfolio as original_get_portfolio
        
        portfolio_data = original_get_portfolio()
        
        # Add metadata
        market_status = get_market_status()
        
        enhanced_response = {
            **portfolio_data,
            "market_status": market_status["status"],
            "is_market_hours": market_status["is_market_hours"],
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "data_as_of": market_status.get("current_time"),
            "data_freshness": "fresh" if market_status["is_market_hours"] else "stale",
            "subtitle": "Your open positions"  # Remove "backend" reference
        }
        
        return enhanced_response
        
    except Exception as e:
        log.error(f"Error getting enhanced portfolio: {e}")
        return {
            "status": "error",
            "positions": [],
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "subtitle": "Your open positions",
            "error": str(e)
        }

def get_watchlist_with_metadata() -> Dict[str, Any]:
    """Enhanced watchlist response with plan limits and metadata"""
    try:
        # Get existing watchlist data
        from app import watchlist_get as original_get_watchlist
        
        watchlist_data = original_get_watchlist()
        symbols = watchlist_data.get("symbols", [])
        
        # Determine plan limits (could be enhanced with user tier detection)
        free_plan_limit = int(os.getenv("FREE_PLAN_WATCHLIST_LIMIT", "10"))
        current_count = len(symbols)
        
        # Get market status
        market_status = get_market_status()
        
        enhanced_response = {
            **watchlist_data,
            "symbols": symbols,
            "count": current_count,
            "plan_limit": free_plan_limit,
            "plan_usage": f"{current_count} / {free_plan_limit} symbols",
            "can_add_more": current_count < free_plan_limit,
            "market_status": market_status["status"],
            "is_market_hours": market_status["is_market_hours"],
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "data_as_of": market_status.get("current_time"),
            "data_freshness": "fresh" if market_status["is_market_hours"] else "stale",
            "subtitle": "Symbols you're monitoring"  # Remove "backend" reference
        }
        
        return enhanced_response
        
    except Exception as e:
        log.error(f"Error getting enhanced watchlist: {e}")
        return {
            "status": "error",
            "symbols": [],
            "count": 0,
            "plan_limit": 10,
            "plan_usage": "0 / 10 symbols",
            "can_add_more": True,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "subtitle": "Symbols you're monitoring",
            "error": str(e)
        }

def validate_ticker_symbol(ticker: str) -> Dict[str, Any]:
    """Validate ticker symbol format and provide feedback"""
    if not ticker:
        return {
            "valid": False,
            "error": "Ticker symbol is required",
            "normalized": None
        }
    
    # Normalize to uppercase
    normalized = ticker.strip().upper()
    
    # Check format (1-5 uppercase letters)
    if not normalized.isalpha():
        return {
            "valid": False,
            "error": "Ticker must contain only letters",
            "normalized": normalized
        }
    
    if len(normalized) < 1 or len(normalized) > 5:
        return {
            "valid": False,
            "error": "Ticker must be 1-5 letters",
            "normalized": normalized
        }
    
    # Could add additional validation against known symbols here
    # For now, just check format
    return {
        "valid": True,
        "error": None,
        "normalized": normalized
    }

def add_to_watchlist_with_validation(ticker: str) -> Dict[str, Any]:
    """Add ticker to watchlist with validation and enhanced response"""
    # Validate ticker
    validation = validate_ticker_symbol(ticker)
    if not validation["valid"]:
        return {
            "success": False,
            "error": validation["error"],
            "ticker": validation["normalized"],
            "added": False
        }
    
    normalized_ticker = validation["normalized"]
    
    try:
        # Check plan limits
        watchlist_data = get_watchlist_with_metadata()
        if not watchlist_data.get("can_add_more", True):
            return {
                "success": False,
                "error": f"Watchlist limit reached ({watchlist_data.get('plan_limit', 10)} symbols)",
                "ticker": normalized_ticker,
                "added": False,
                "plan_usage": watchlist_data.get("plan_usage")
            }
        
        # Add to watchlist (using existing logic)
        from app import watchlist_add
        result = watchlist_add({"symbol": normalized_ticker})
        
        if result.get("ok"):
            return {
                "success": True,
                "ticker": normalized_ticker,
                "added": True,
                "plan_usage": f"{watchlist_data.get('count', 0) + 1} / {watchlist_data.get('plan_limit', 10)} symbols",
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        else:
            return {
                "success": False,
                "error": "Failed to add to watchlist",
                "ticker": normalized_ticker,
                "added": False
            }
            
    except Exception as e:
        log.error(f"Error adding {normalized_ticker} to watchlist: {e}")
        return {
            "success": False,
            "error": str(e),
            "ticker": normalized_ticker,
            "added": False
        }

def get_portfolio_table_structure() -> Dict[str, Any]:
    """Return the expected table structure for portfolio when empty"""
    return {
        "columns": [
            {"key": "symbol", "label": "Symbol", "width": "80px"},
            {"key": "entry", "label": "Entry", "width": "80px"},
            {"key": "current", "label": "Current", "width": "80px"},
            {"key": "pnl", "label": "P&L", "width": "80px"},
            {"key": "stop", "label": "Stop", "width": "80px"},
            {"key": "target", "label": "Target", "width": "80px"},
            {"key": "status", "label": "Status", "width": "100px"}
        ],
        "empty_state": {
            "heading": "No positions yet",
            "cta_text": "Analyze a Stock",
            "cta_action": "navigate_to_dashboard_search"
        }
    }

def get_watchlist_table_structure() -> Dict[str, Any]:
    """Return the expected table structure for watchlist when empty"""
    return {
        "columns": [
            {"key": "symbol", "label": "Symbol", "width": "80px"},
            {"key": "price", "label": "Price", "width": "80px"},
            {"key": "change", "label": "Change", "width": "80px"},
            {"key": "ai_score", "label": "AI Score", "width": "80px"},
            {"key": "last_analyzed", "label": "Last Analyzed", "width": "120px"},
            {"key": "action", "label": "Action", "width": "100px"}
        ],
        "empty_state": {
            "heading": "Watchlist empty",
            "quick_add_symbols": ["AAPL", "MSFT", "NVDA"],
            "placeholder": "Add ticker (e.g., AAPL)"
        }
    }

def get_quick_watchlist_symbols() -> List[str]:
    """Get recommended symbols for quick-add to watchlist"""
    default_symbols = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "BRK.B"]
    
    try:
        # Could customize based on market conditions or user preferences
        return default_symbols[:5]  # Return first 5 for UI
    except Exception:
        return default_symbols[:3]  # Fallback to 3 symbols
