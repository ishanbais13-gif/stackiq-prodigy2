import sqlite3
import time
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import os
import logging

log = logging.getLogger(__name__)

# SQLite database for alerts
ALERTS_DB_PATH = os.getenv("ALERTS_DB_PATH", "stackiq.db")

# Email configuration
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_PROVIDER = os.getenv("EMAIL_PROVIDER", "smtp")  # smtp or sendgrid
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "alerts@stackiq.com")
ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL", "")

def _get_db_connection():
    """Get SQLite database connection"""
    conn = sqlite3.connect(ALERTS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _init_alerts_db():
    """Initialize alerts database tables"""
    conn = _get_db_connection()
    try:
        # Alert preferences table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alert_preferences (
                symbol TEXT PRIMARY KEY,
                enabled BOOLEAN DEFAULT 1,
                buy_zone_low REAL,
                buy_zone_high REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Alert history table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                alert_type TEXT,
                message TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                email_sent BOOLEAN DEFAULT 0,
                price_at_alert REAL
            )
        """)
        
        # Daily alert tracking (to prevent spam)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_alert_tracking (
                symbol TEXT,
                date TEXT,  -- YYYY-MM-DD format
                alert_type TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, date, alert_type)
            )
        """)
        
        conn.commit()
    finally:
        conn.close()

def set_alert_preference(symbol: str, enabled: bool, buy_zone_low: float = None, buy_zone_high: float = None) -> Dict[str, Any]:
    """Set alert preference for a symbol"""
    if not _init_alerts_db():
        _init_alerts_db()
    
    conn = _get_db_connection()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO alert_preferences 
            (symbol, enabled, buy_zone_low, buy_zone_high, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (symbol.upper(), enabled, buy_zone_low, buy_zone_high, datetime.now(timezone.utc).isoformat()))
        
        conn.commit()
        
        return {
            "success": True,
            "symbol": symbol.upper(),
            "enabled": enabled,
            "buy_zone_low": buy_zone_low,
            "buy_zone_high": buy_zone_high
        }
    except Exception as e:
        log.error(f"Failed to set alert preference for {symbol}: {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def get_alert_preferences(symbol: str = None) -> List[Dict[str, Any]]:
    """Get alert preferences, optionally filtered by symbol"""
    if not _init_alerts_db():
        _init_alerts_db()
    
    conn = _get_db_connection()
    try:
        if symbol:
            cursor = conn.execute(
                "SELECT * FROM alert_preferences WHERE symbol = ?", 
                (symbol.upper(),)
            )
        else:
            cursor = conn.execute("SELECT * FROM alert_preferences ORDER BY symbol")
        
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))
        
        return results
    except Exception as e:
        log.error(f"Failed to get alert preferences: {e}")
        return []
    finally:
        conn.close()

def _can_send_daily_alert(symbol: str, alert_type: str) -> bool:
    """Check if we can send an alert for this symbol today (to prevent spam)"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    conn = _get_db_connection()
    try:
        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM daily_alert_tracking WHERE symbol = ? AND date = ? AND alert_type = ?",
            (symbol.upper(), today, alert_type)
        )
        count = cursor.fetchone()["count"]
        return count == 0
    except Exception as e:
        log.error(f"Failed to check daily alert tracking: {e}")
        return False
    finally:
        conn.close()

def _mark_daily_alert_sent(symbol: str, alert_type: str):
    """Mark that we sent an alert for this symbol today"""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    conn = _get_db_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO daily_alert_tracking (symbol, date, alert_type, sent_at) VALUES (?, ?, ?, ?)",
            (symbol.upper(), today, alert_type, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    except Exception as e:
        log.error(f"Failed to mark daily alert sent: {e}")
    finally:
        conn.close()

def _send_email(subject: str, message: str) -> bool:
    """Send email using SMTP or SendGrid"""
    if not EMAIL_ENABLED or not ALERT_TO_EMAIL:
        log.warning("Email not enabled or recipient not configured")
        return False
    
    try:
        if EMAIL_PROVIDER == "sendgrid" and SENDGRID_API_KEY:
            return _send_sendgrid_email(subject, message)
        else:
            return _send_smtp_email(subject, message)
    except Exception as e:
        log.error(f"Failed to send email: {e}")
        return False

def _send_smtp_email(subject: str, message: str) -> bool:
    """Send email using SMTP"""
    if not SMTP_HOST or not SMTP_USERNAME or not SMTP_PASSWORD:
        log.error("SMTP configuration incomplete")
        return False
    
    try:
        msg = MIMEMultipart()
        msg['From'] = ALERT_FROM_EMAIL
        msg['To'] = ALERT_TO_EMAIL
        msg['Subject'] = subject
        
        msg.attach(MIMEText(message, 'plain'))
        
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        if SMTP_USE_TLS:
            server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        log.info(f"SMTP email sent successfully to {ALERT_TO_EMAIL}")
        return True
    except Exception as e:
        log.error(f"Failed to send SMTP email: {e}")
        return False

def _send_sendgrid_email(subject: str, message: str) -> bool:
    """Send email using SendGrid API"""
    if not SENDGRID_API_KEY:
        log.error("SendGrid API key not configured")
        return False
    
    try:
        import requests
        
        data = {
            "personalizations": [{
                "to": [{"email": ALERT_TO_EMAIL}],
                "subject": subject
            }],
            "from": {"email": ALERT_FROM_EMAIL},
            "content": [{
                "type": "text/plain",
                "value": message
            }]
        }
        
        headers = {
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code == 202:
            log.info(f"SendGrid email sent successfully to {ALERT_TO_EMAIL}")
            return True
        else:
            log.error(f"SendGrid API error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log.error(f"Failed to send SendGrid email: {e}")
        return False

def check_buy_zone_alerts(market_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check if any symbols are entering their buy zones and send alerts"""
    alerts_sent = []
    
    # Get all enabled alert preferences
    preferences = get_alert_preferences()
    enabled_prefs = [p for p in preferences if p.get("enabled")]
    
    for pref in enabled_prefs:
        symbol = pref["symbol"]
        buy_zone_low = pref.get("buy_zone_low")
        buy_zone_high = pref.get("buy_zone_high")
        
        if not buy_zone_low or not buy_zone_high:
            continue
        
        # Get current price for this symbol
        current_price = _get_current_price(symbol, market_data)
        if current_price is None:
            continue
        
        # Check if price is in buy zone
        if buy_zone_low <= current_price <= buy_zone_high:
            # Check if we can send alert today (prevent spam)
            if not _can_send_daily_alert(symbol, "buy_zone"):
                continue
            
            # Get AI score for the alert message
            ai_score = _get_ai_score(symbol, market_data)
            
            # Create alert message
            message = f"{symbol} is entering your buy zone {buy_zone_low:.2f}–{buy_zone_high:.2f}. AI Score {ai_score}/10. Entry now."
            subject = f"🔔 Buy Zone Alert: {symbol}"
            
            # Send email
            email_sent = _send_email(subject, message)
            
            # Record alert in history
            _record_alert(symbol, "buy_zone", message, current_price, email_sent)
            
            # Mark as sent today
            _mark_daily_alert_sent(symbol, "buy_zone")
            
            alerts_sent.append({
                "symbol": symbol,
                "type": "buy_zone",
                "message": message,
                "current_price": current_price,
                "buy_zone_low": buy_zone_low,
                "buy_zone_high": buy_zone_high,
                "ai_score": ai_score,
                "email_sent": email_sent
            })
    
    return alerts_sent

def _get_current_price(symbol: str, market_data: Dict[str, Any]) -> Optional[float]:
    """Get current price for a symbol from market data"""
    try:
        # Check if we have snapshot data for this symbol
        snapshots = market_data.get("snapshots", {})
        if symbol in snapshots:
            snap = snapshots[symbol]
            if isinstance(snap, dict):
                # Try latest trade first
                latest_trade = snap.get("latestTrade", {})
                if latest_trade and latest_trade.get("p"):
                    return float(latest_trade["p"])
                
                # Try daily bar close
                daily_bar = snap.get("dailyBar", {})
                if daily_bar and daily_bar.get("c"):
                    return float(daily_bar["c"])
        
        # Check if we have bars data
        bars = market_data.get("bars", {})
        if symbol in bars:
            symbol_bars = bars[symbol]
            if isinstance(symbol_bars, list) and symbol_bars:
                latest_bar = symbol_bars[-1]
                if isinstance(latest_bar, dict) and latest_bar.get("c"):
                    return float(latest_bar["c"])
        
        return None
    except Exception as e:
        log.error(f"Failed to get current price for {symbol}: {e}")
        return None

def _get_ai_score(symbol: str, market_data: Dict[str, Any]) -> float:
    """Get AI score for a symbol from market data"""
    try:
        # Check if we have analyze data
        analyze_data = market_data.get("analyze", {})
        if symbol in analyze_data:
            data = analyze_data[symbol]
            if isinstance(data, dict):
                # Try 0-10 scale first
                score = data.get("ai_score_0_10")
                if score is not None:
                    return float(score)
                
                # Fallback to 0-100 scale and convert
                score = data.get("ai_score_0_100")
                if score is not None:
                    return float(score) / 10.0
                
                # Try main ai_score field
                score = data.get("ai_score")
                if score is not None:
                    return float(score)
        
        return 0.0
    except Exception as e:
        log.error(f"Failed to get AI score for {symbol}: {e}")
        return 0.0

def _record_alert(symbol: str, alert_type: str, message: str, price: float, email_sent: bool):
    """Record alert in history"""
    conn = _get_db_connection()
    try:
        conn.execute("""
            INSERT INTO alert_history (symbol, alert_type, message, price_at_alert, email_sent)
            VALUES (?, ?, ?, ?, ?)
        """, (symbol.upper(), alert_type, message, price, email_sent))
        conn.commit()
    except Exception as e:
        log.error(f"Failed to record alert: {e}")
    finally:
        conn.close()

def get_alert_history(symbol: str = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Get alert history, optionally filtered by symbol"""
    if not _init_alerts_db():
        _init_alerts_db()
    
    conn = _get_db_connection()
    try:
        if symbol:
            cursor = conn.execute(
                "SELECT * FROM alert_history WHERE symbol = ? ORDER BY sent_at DESC LIMIT ?",
                (symbol.upper(), limit)
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM alert_history ORDER BY sent_at DESC LIMIT ?",
                (limit,)
            )
        
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))
        
        return results
    except Exception as e:
        log.error(f"Failed to get alert history: {e}")
        return []
    finally:
        conn.close()

# Initialize database on module import
try:
    _init_alerts_db()
except Exception as e:
    log.error(f"Failed to initialize alerts database: {e}")
