from flask import Flask
app = Flask(__name__)

@app.get("/")
def home():
    return "<h1>StackIQ is Live 🚀</h1><p>Fresh start on Azure.</p>"
