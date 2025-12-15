from flask import Flask, request, jsonify, render_template, send_from_directory, make_response
from flask_cors import CORS
from helpers.adapter_loader import run_php_adapter
from helpers.endpoints import ENTITY_ENDPOINTS
from dispatcher import dispatch
from helpers.shared_logic import fetch_entity_definition
from reports.report_writer import generate_report_files
from datetime import datetime
from urllib.parse import urlparse
import json
import sys
import os
import tempfile
import requests
import logging

app = Flask(__name__, static_folder='static')
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more verbosity
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

CORS(app)

def run_migration_dispatch(payload, migration_type, api_url, auth_token, entity, adapter_key):
    print(f"🚀 Migration started for adapter: {adapter_key}")
    summary, stats = dispatch(adapter_key, payload, migration_type, api_url, auth_token, entity)
    return summary, stats


@app.route('/')
def home():
    adapter_names = get_adapter_names()
    return render_template("index.html", adapter_names=adapter_names)

def get_bearer_token(email, password, base_url):
    parsed = urlparse(base_url)
    tenant = parsed.hostname.split(".")[0]  # e.g. 'shenderdemo'
    region = "australia-east"
    token_url = "https://auth.mysite-preview.com.au/connect/token"

    client_id = f"xxxxxxxxxx:{region}:{tenant}:xxxxxxxxxxxx"
    payload = {
        "grant_type": "password",
        "client_id": client_id,
        "scope": "openid profile mysite.xxxxxxxxxxxxxxxxxx.api",
        "username": email,
        "password": password
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(token_url, data=payload, headers=headers)
    response.raise_for_status()
    token = response.json()["access_token"]
    return token

# === Adapter Discovery ===
def get_adapter_names():
    adapter_dir = os.path.join(os.path.dirname(__file__), "adapters")
    return [
        os.path.splitext(f)[0]
        for f in os.listdir(adapter_dir)
        if f.endswith(".php") and os.path.isfile(os.path.join(adapter_dir, f))
    ]

# === Classification Flattening ===
def flatten_classifications(records):
    flat = []
    for record in records:
        for subgroup in record.get("subgroups", []):
            subgroup_copy = {**subgroup}
            try:
                subgroup_copy["id"] = int(subgroup_copy["id"])
            except (ValueError, TypeError):
                print(f"⚠️ Skipping subgroup with non-numeric ID: {subgroup_copy.get('id')}")
                continue  # or raise, depending on how strict you want to be
            flat.append(subgroup_copy)
    return flat

# === Home Page ===
@app.route('/reports/<path:filename>')
def serve_report(filename):
    return send_from_directory(app.static_folder, filename)

def index():
    adapter_names = get_adapter_names()
    return render_template('index.html', adapter_names=adapter_names)

# === Entity Schema Preview ===
@app.route('/entity_schema', methods=['POST'])
def entity_schema():
    definition_url = request.form.get('definition_url')
    email = request.form.get('email')
    password = request.form.get('password')
    base_url = request.form.get('short_api_url')

    token = get_bearer_token(email, password, base_url)
    headers = {"Authorization": f"Bearer {token}"}

    schema = fetch_entity_definition(definition_url, headers)
    return jsonify(schema)

# === Migration Execution ===
@app.route('/run_migration', methods=['POST'])
def run_migration():
    debug_logs = []
    
    try:
        # === File Upload ===
        if 'input_file' not in request.files:
            return jsonify({"status": "error", "message": "No file uploaded"}), 400

        file = request.files['input_file']
        if file.filename == '':
            return jsonify({"status": "error", "message": "Empty filename"}), 400

        content = file.read().decode("utf-8", errors="replace")

        # === Parse Form Data ===
        base_url = request.form.get('short_api_url')
        adapter_name = request.form.get('adapter_name')
        entity = request.form.get('entity')
        email = request.form.get('email')
        password = request.form.get('password')
        migration_type = request.form.get('migration_type', 'insert').strip().lower()
        if migration_type not in ['insert', 'update', 'upsert']:
            migration_type = 'insert'
        purge_existing = request.form.get('purge_existing') == 'on'

        # === Resolve Endpoint ===
        if entity not in ENTITY_ENDPOINTS:
            raise ValueError(f"Unknown entity: {entity}")
        endpoint_path = ENTITY_ENDPOINTS[entity]["path"]
        api_url = f"{base_url}{endpoint_path}"
        os.environ["ENDPOINT_BASE"] = api_url

        # === Auth ===
        token = get_bearer_token(email, password, base_url)

        # === Adapter Execution ===
        adapter_path = f"adapters/{adapter_name}.php"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as temp_file:
            temp_file.write(content)
            temp_file_path = temp_file.name
        raw_output = run_php_adapter(adapter_path, temp_file_path, migration_type)
        debug_logs.append("📄 Adapter Output:\n" + json.dumps(raw_output, indent=2))
        records = raw_output.get("records", [])
        debug_logs.append(f"🛠 Adapter path: {adapter_path}")
        debug_logs.append(f"raw_output from php adapter: {raw_output.get('details')}")

        with open("debug_output.txt", "w", encoding="utf-8") as f:
            f.write("📄 Raw PHP Adapter Output:\n")
            f.write(json.dumps(raw_output, indent=2))
            f.write("\n\n")
            for i, record in enumerate(raw_output.get("records", []), start=1):
                f.write(f"🔍 Raw Row {i}:\n")
                f.write(json.dumps(record, indent=2))
                f.write("\n\n")

        if "error" in raw_output:
            raise ValueError(f"Adapter failed. Check Adapter Name -> matches Entity?: {raw_output['error']}")

        # === Optional Classification Flattening ===
        if entity == "classifications" and any("subgroups" in r for r in records):
            records = flatten_classifications(records)

        # === Run Migration ===
        summary, stats = run_migration_dispatch(
            payload=raw_output,
            migration_type=migration_type,
            api_url=api_url,
            auth_token=token,
            entity=entity,
            adapter_key=raw_output.get("adapter_key")
        )
        # === Write Debug Log ===
        with open("ui_debug_log.txt", "a", encoding="utf-8") as f:
            for line in debug_logs:
                f.write(line + "\n")
            for err in stats.errors:
                f.write(f"❌ {err}\n")

        # === Generate Reports ===
        report_files = generate_report_files(summary, adapter_name, entity, migration_type)
        report_paths = {
            "csv": f"/reports/{report_files['csv']}"
        }

        # === Return Response ===
        response = make_response(jsonify({
            "status": "success",
            "summary": summary,
            "success_count": summary["success"],
            "skipped_count": summary["skipped"],
            "total_count": summary["total"],
            "rows": stats.rows,
            "errors": stats.errors,
            "debug": debug_logs,
            "report_paths": report_paths
        }))
        response.headers["Content-Type"] = "application/json; charset=utf-8"

        print(f"🚀 Migration started for entity: {entity}")
        print(f"📡 Posting to: {api_url}")
        print(f"📦 Records received: {len(raw_output.get('records', []))}")
        print(f"✅ Migration complete: {summary['success']} written, {summary['skipped']} skipped")
        print(f"🕒 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return response

    except Exception as e:
        error_response = make_response(jsonify({
            "status": "error",
            "message": str(e),
            "debug": debug_logs
        }))
        error_response.headers["Content-Type"] = "application/json; charset=utf-8"
        return error_response, 500 
         
# === Serve Report Downloads ===
@app.route('/reports/<path:filename>')
def download_report(filename):
    return send_from_directory('reports', filename)

# === Security Tightening after ===
@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response

# === Launch App ===
if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
    app.run(debug=True, port=port)