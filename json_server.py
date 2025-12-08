from flask import Flask, send_file, jsonify
import os

app = Flask(__name__)

SYNC_LATEST_PATH = "/tmp/sync_latest.json"
SYNC_HISTORY_PATH = "/tmp/sync_issues.jsonl"

@app.route("/latest")
def latest():
    if not os.path.exists(SYNC_LATEST_PATH):
        return jsonify({"error": "sync_latest.json not found yet"}), 404
    return send_file(SYNC_LATEST_PATH, mimetype="application/json")

@app.route("/history")
def history():
    if not os.path.exists(SYNC_HISTORY_PATH):
        return jsonify([]), 200
    
    with open(SYNC_HISTORY_PATH, "r") as f:
        lines = f.readlines()
        
    # Converte cada linha para dict JSON
    entries = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except:
            pass
    
    return jsonify(entries), 200

@app.route("/")
def root():
    return jsonify({"status": "json-server-running"})

if __name__ == "__main__":
    # Porta 5001 → importante, vamos usar no Grafana
    app.run(host="0.0.0.0", port=5001)
