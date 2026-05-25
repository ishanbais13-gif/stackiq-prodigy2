# Price Alerts Feature

This feature allows users to set up price alerts for stocks and receive email notifications when a stock enters its buy zone.

## Features

- **Buy Zone Alerts**: Get notified when a stock enters your specified buy zone (price range)
- **Email Notifications**: Supports both SMTP and SendGrid for email delivery
- **Daily Spam Protection**: Maximum one alert per symbol per day
- **Background Monitoring**: Automatic checking every 5 minutes
- **SQLite Storage**: Persistent alert preferences and history
- **API Endpoints**: RESTful API for managing alerts

## Environment Variables

### Email Configuration

```bash
# Enable email alerts
EMAIL_ENABLED=true

# Email provider: smtp or sendgrid
EMAIL_PROVIDER=smtp

# SMTP Configuration (if EMAIL_PROVIDER=smtp)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-app-password
SMTP_USE_TLS=true

# SendGrid Configuration (if EMAIL_PROVIDER=sendgrid)
SENDGRID_API_KEY=your-sendgrid-api-key

# Email addresses
ALERT_FROM_EMAIL=alerts@stackiq.com
ALERT_TO_EMAIL=your-email@example.com
```

### Database Configuration

```bash
# SQLite database path (optional, defaults to stackiq.db)
ALERTS_DB_PATH=stackiq.db
```

## API Endpoints

### Get Alert Preferences
```http
GET /api/alerts/preferences?symbol=AAPL
```

### Set Alert Preference
```http
POST /api/alerts/preferences
Content-Type: application/json

{
  "symbol": "AAPL",
  "enabled": true,
  "buy_zone_low": 150.0,
  "buy_zone_high": 160.0
}
```

### Get Alert History
```http
GET /api/alerts/history?symbol=AAPL&limit=50
```

### Manual Alert Check
```http
POST /api/alerts/check
```

## Frontend Integration

A basic HTML/JS frontend is provided in `alerts_frontend.html` that can be integrated into your existing frontend. Features include:

- Toggle alerts on/off per symbol
- Set buy zone ranges
- Add new alerts
- Visual feedback for operations

## How It Works

1. **Setup**: Users configure alerts for specific symbols with buy zone ranges
2. **Monitoring**: Background task checks market data every 5 minutes
3. **Detection**: When a stock's price enters the buy zone, an alert is triggered
4. **Protection**: Daily tracking prevents spam (max 1 alert/symbol/day)
5. **Notification**: Email sent with format: "SYMBOL is entering your buy zone LOW–HIGH. AI Score X/10. Entry now."
6. **History**: All alerts logged to SQLite database

## Database Schema

### alert_preferences
- `symbol` (PRIMARY KEY): Stock symbol
- `enabled`: Whether alerts are active
- `buy_zone_low`: Lower bound of buy zone
- `buy_zone_high`: Upper bound of buy zone
- `created_at`: When preference was created
- `updated_at`: When preference was last updated

### alert_history
- `id` (PRIMARY KEY): Auto-increment ID
- `symbol`: Stock symbol
- `alert_type`: Type of alert (e.g., "buy_zone")
- `message`: Alert message content
- `sent_at`: When alert was sent
- `email_sent`: Whether email was successfully sent
- `price_at_alert`: Stock price when alert triggered

### daily_alert_tracking
- `symbol` (PART of PRIMARY KEY): Stock symbol
- `date` (PART of PRIMARY KEY): Date in YYYY-MM-DD format
- `alert_type` (PART of PRIMARY KEY): Type of alert
- `sent_at`: When alert was sent

## Email Setup Examples

### Gmail SMTP Setup
1. Enable 2-factor authentication on your Gmail account
2. Generate an App Password: Google Account → Security → App Passwords
3. Use the App Password as SMTP_PASSWORD

### SendGrid Setup
1. Create a SendGrid account
2. Generate an API key
3. Set SENDGRID_API_KEY environment variable

## Monitoring and Logs

The system logs:
- Alert checker startup/shutdown
- Successful alert deliveries
- Failed alert attempts
- Background task errors

Check logs for:
- `Background alert checker started`
- `Background alert check sent X alerts`
- `Failed to get snapshots for alert check`

## Troubleshooting

### Emails Not Sending
1. Verify EMAIL_ENABLED=true
2. Check email provider configuration
3. Verify SMTP credentials or SendGrid API key
4. Check recipient email address (ALERT_TO_EMAIL)

### Alerts Not Triggering
1. Verify alert preferences are set correctly
2. Check buy zone ranges make sense (low < high)
3. Verify symbol is enabled
4. Check market data availability

### Background Task Issues
1. Check application logs for startup errors
2. Verify database permissions
3. Check for rate limiting issues

## Security Considerations

- Store email credentials securely (environment variables, not in code)
- Use App Passwords for SMTP, not main account passwords
- Validate all user inputs in production
- Consider rate limiting API endpoints
- Use HTTPS for all API calls in production
