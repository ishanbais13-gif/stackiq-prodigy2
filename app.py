from flask import Flask, jsonify
import data_fetcher

app = Flask(__name__)

@app.get("/")
def root():
    return "StackIQ backend is live."

@app.get("/test/<ticker>")
def test_ticker(ticker):
    return jsonify({
        "ticker": ticker.upper(),
        "price": data_fetcher.get_stock_price(ticker),
        "earnings": data_fetcher.get_next_earnings(ticker)
    })

# Azure runs via gunicorn; this is for local dev only
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

