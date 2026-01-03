from flask import Flask, jsonify
from flask_cors import CORS
import json
import os

app = Flask(__name__)
CORS(app)

HISTORY_FILE = "tmp_verification_history.jsonl"


def load_history():
    if not os.path.exists(HISTORY_FILE):
        return []

    data = []
    with open(HISTORY_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return data


@app.route("/results", methods=["GET"])
def results():
    """
    Main endpoint for Grafana.
    Returns an array of verification reports.
    """
    return jsonify(load_history())


@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "service": "json_server",
        "status": "ok",
        "endpoints": ["/results", "/health"]
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print("🚀 JSON server running on port 5001")
    app.run(host="0.0.0.0", port=5001)
