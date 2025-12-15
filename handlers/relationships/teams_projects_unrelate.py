#handlers/teams_projects.py
#run as UPDATE https://swcclone.api.consultationmanager-preview.com/entities/team/115 #Team ID
# {
#   "dataVersion": 1,
#   "projectOperations": {
#     "relate": [
#       1012 #project id
#     ],
#     "unrelate": []
#   },
#   "values": {}
# }
import requests
import datetime
import time
import sys
import csv
from pathlib import Path
from helpers.logger import MigrationStats, build_log_entry
from helpers.shared_logic import build_auth_headers
from helpers.logger import write_detailed_audit_csv

def handle(payload, migration_type, api_url, auth_token, entity):
    headers = build_auth_headers(auth_token)
    stats = MigrationStats()
    records = payload.get("records", [])

    print(f"🚀 Starting handler for entity: {entity}, migration_type: {migration_type}")
    print(f"📦 Received {len(records)} records")
    sys.stdout.flush()

    for i, record in enumerate(records, start=1):
        stats.total += 1

        if not isinstance(record, dict):
            print(f"⚠️ Record {i} is not a dict: {record}")
            stats.log_skip(i, {}, "Invalid record format")
            continue

        meta = record.get("meta", {})
        record_id = meta.get("id")
        if not record_id:
            print(f"⚠️ Record {i} missing ID: {meta}")
            stats.log_skip(i, meta, "Missing ID in header row for PATCH")
            continue

        endpoint = f"{record.get('endpoint', api_url)}/{record_id}"
        method = "PATCH"

        packet = {
            "dataVersion": record.get("dataVersion", 1),
            "projectOperations": record.get("projectOperations", {}),
            "values": record.get("values", {})
        }

        ops = packet["projectOperations"]
        if not ops.get("relate") and not ops.get("unrelate"):
            print(f"⚠️ Record {i} has no relate/unrelate ops: {ops}")
            stats.log_skip(i, meta, "No relate or unrelate operations provided")
            continue

        def get_log_field(field):
            return meta.get(field, "")

        def get_record_id():
            return record_id

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)
        log_entry["team"] = get_log_field("team")
        log_entry["project"] = get_log_field("project")
        status_code = None
        message = ""
        result = "Skipped"
        row_index = meta.get("rowIndex", i)

        print(f"📤 Sending PATCH to {endpoint}")
        sys.stdout.flush()

        max_retries = 1
        for attempt in range(1, max_retries + 1):
            time.sleep(0)
            try:
                response = requests.request(method, endpoint, json=packet, headers=headers, timeout=180)
                status_code = response.status_code
                message = response.text.strip()
                print(f"📄 Record {row_index} Attempt {attempt} — Relate: {ops.get('relate', [])}, Unrelate: {ops.get('unrelate', [])}, Status: {status_code}")
                sys.stdout.flush()

                if status_code in [200, 204]:
                    stats.log_success(i, log_entry)
                    result = "Success"
                    break
                elif status_code in [400, 403, 404, 405, 409]:
                    print(f"🚫 Record {row_index} — Permanent failure: {status_code}")
                    result = "Skipped"
                    break
                else:
                    message = f"HTTP {status_code}: {message}"
                    result = "Error"
            except Exception as e:
                status_code = "Exception"
                message = str(e)
                result = "Error"
                print(f"📄 Record {row_index} Attempt {attempt} — Status: Exception")
                sys.stdout.flush()

            if attempt < max_retries:
                print(f"⏳ Waiting 0 seconds before retrying ...")
                sys.stdout.flush()
                time.sleep(0)

            if attempt == max_retries:
                stats.log_skip(i, log_entry, f"Failed after {max_retries} attempts: {message}")
    
        log_entry["entity"] = entity
        log_entry["relate"] = ops.get("relate", [])
        log_entry["unrelate"] = ops.get("unrelate", [])
        log_entry["timestamp"] = datetime.datetime.now().isoformat()
        log_entry["duration"] = round(time.time() - stats.start_time, 2)
        log_entry["rowIndex"] = row_index
        log_entry["message"] = message or "No response body"
        log_entry["team"] = get_log_field("team")
        log_entry["project"] = get_log_field("project")
        log_entry["adapter_key"] = payload.get("adapter_key", "teams_projects_unrelate")
        log_entry["result"] = result
        log_entry["attempts"] = attempt
        log_entry["status_code"] = status_code
        log_entry["error"] = message if status_code == "Exception" else ""
        print(f"📥 Response for Record {row_index}: {status_code} — {message[:200]}")
        sys.stdout.flush()

    sys.stdout.flush()
    write_detailed_audit_csv(stats, "teams_projects_unrelate")
    stats.write_summary_csv("audit/migration_summary_unrelate.csv")

    print(f"✅ Migration complete: {stats.success} succeeded, {stats.skipped} skipped, {stats.total} total")
    sys.stdout.flush()
    return stats.summary(), stats