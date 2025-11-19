
# {
# "values": {
# "classificationType": 0,
# "dataVersion": 0,
# "deleted": true,
# "description": "string",
# "name": "string",
# "parentId": 0
# }
# }


import requests
import json
import time
import datetime
from helpers.shared_logic import fetch_entity_definition, auto_map_fields, build_auth_headers
from helpers.logger import MigrationStats, build_log_entry, write_detailed_audit_csv
from helpers.endpoints import ENTITY_ENDPOINTS

def handle(payload, migration_type, api_url, auth_token, entity):
    print("✅ classifications.handle() received definition_url")
    headers = build_auth_headers(auth_token)
    stats = MigrationStats()
    records = payload.get("records", [])

    max_retries = 3
    retry_delay = 0  # seconds
    entity_definition = None  # Schema fetch skipped

    for i, record in enumerate(records, start=1):
        stats.total += 1
        meta = record.get("meta", {})
        values = record.get("values", {})

        if not values.get("name") or not values.get("parentId"):
            stats.log_skip(i, meta, "Missing required fields: name or parentId")
            continue
        values["description"] = str(values.get("description") or "")
        packet = {
            "values": values
        }

        method = "POST"
        endpoint = api_url

        def get_log_field(field): return meta.get(field, "")
        def get_record_id(): return meta.get("id") or values.get("id", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)
        print(f"🔍 Row {i} POST to: {endpoint} with payload: {json.dumps(packet, indent=2)}")

        status_code = None
        message = ""

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.request(method, endpoint, json=packet, headers=headers, timeout=180)
                status_code = response.status_code
                message = response.text.strip() or "No response body"

                if status_code in [200, 201]:
                    result = "Success"
                    log_entry.update({
                        "adapter_key": payload.get("adapter_key", "classifications"),
                        "rowIndex": meta.get("rowIndex", i),
                        "timestamp": datetime.datetime.now().isoformat(),
                        "duration": round(time.time() - stats.start_time, 2),
                        "attempts": attempt,
                        "message": message,
                        "error": "",
                        "result": result
                    })
                    stats.log_success(i, log_entry)
                    break
                elif status_code in [400, 403, 404, 405, 409]:
                    result = "Skipped"
                    log_entry.update({
                        "adapter_key": payload.get("adapter_key", "classifications"),
                        "rowIndex": meta.get("rowIndex", i),
                        "timestamp": datetime.datetime.now().isoformat(),
                        "duration": round(time.time() - stats.start_time, 2),
                        "attempts": attempt,
                        "message": message,
                        "error": "",
                        "result": result
                    })
                    stats.log_skip(i, log_entry, f"Permanent failure: {status_code}")
                    break
                else:
                    print(f"⏳ Attempt {attempt} failed with status {status_code}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)

            except Exception as e:
                status_code = "Exception"
                message = str(e)
                result = "Error"
                log_entry.update({
                    "adapter_key": payload.get("adapter_key", "classifications"),
                    "rowIndex": meta.get("rowIndex", i),
                    "timestamp": datetime.datetime.now().isoformat(),
                    "duration": round(stats.elapsed(), 2),
                    "attempts": attempt,
                    "message": message,
                    "error": message,
                    "result": result
                })
                print(f"⏳ Attempt {attempt} raised exception. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

            if attempt == max_retries:
                stats.log_skip(i, log_entry, f"Failed after {max_retries} attempts: {message}")

        print(f"📥 Response for Record {i}: {status_code} — {message[:200]}")

    write_detailed_audit_csv(stats, "classifications")
    stats.write_summary_csv("audit/migration_summary_classifications.csv")
    return stats.summary(), stats