from flask import Flask, jsonify
from flask_cors import CORS
import json
import os

app = Flask(__name__)
CORS(app)

LATEST_FILE = "current_verification_report.json"
HISTORY_FILE = "verification_history.jsonl"


def load_latest():
    if not os.path.exists(LATEST_FILE):
        return []
    try:
        with open(LATEST_FILE, "r") as f:
            obj = json.load(f)
        # Grafana JSON plugin is happier when the response is ALWAYS a list
        return [obj]
    except Exception:
        return []


def load_history(limit=None):
    if not os.path.exists(HISTORY_FILE):
        return []

    items = []
    with open(HISTORY_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    # Optional: keep only last N to avoid heavy queries
    if limit is not None:
        items = items[-limit:]

    return items


@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "service": "json_server",
        "status": "ok",
        "endpoints": ["/latest", "/history", "/health"]
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/latest", methods=["GET"])
def latest():
    return jsonify(load_latest())


@app.route("/history", methods=["GET"])
def history():
    # optional: /history?limit=200 (if you want later)
    return jsonify(load_history())


if __name__ == "__main__":
    print("🚀 JSON server running on port 5001")
    app.run(host="0.0.0.0", port=5001)
