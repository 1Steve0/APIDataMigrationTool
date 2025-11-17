# {
#     "DataVersion": 1,
#     "Values": {
#         "LeftHandId": 0, #User
#         "RightHandId": 0, #Team
#         }
# }
import requests
import json
import sys
from helpers.logger import MigrationStats, build_log_entry
from helpers.shared_logic import build_auth_headers

def handle(payload, migration_type, api_url, auth_token, entity):
    headers = build_auth_headers(auth_token)
    stats = MigrationStats()
    records = payload.get("records", [])

    for i, record in enumerate(records, start=1):
        stats.total += 1
        if not isinstance(record, dict):
            stats.log_skip(i, {}, "Invalid record format")
            continue

        meta = record.get("meta", {})
        # 🔄 Flatten meta["source"] into top-level fields
        source = meta.get("source", {})
        if isinstance(source, dict):
            meta["user"] = source.get("user", "")
            meta["team"] = source.get("team", "")
        meta.pop("source", None)  # Remove nested dict to avoid CSV error

        packet = {
            "dataVersion": record.get("DataVersion", 1),
            "values": record.get("values", {})
        }

        values = packet["values"]
        print(f"🔍 Row {i} values block: {json.dumps(packet.get('values', {}), indent=2)}")
        allowed_keys = {
            "row", "rowIndex", "status", "name", "id", "message", "parentId", "description", "response_id",
            "error", "log_method", "log_endpoint", "reason", "team", "project", "user"
        }
        if not values.get("LeftHandId") or not values.get("RightHandId"):
            stats.log_skip(i, meta, "Missing LeftHandId or RightHandId")
            continue

        method = "POST"
        endpoint = api_url

        def get_log_field(field):
            return meta.get(field, "")

        def get_record_id():
            return meta.get("id") or values.get("id", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)
        print(f"📤 Sending POST to {endpoint} with payload: {packet}")
        sys.stdout.flush()

        try:
            response = requests.request(method, endpoint, json=packet, headers=headers, timeout=10)
            log_entry["message"] = response.text.strip() or "No response body"
            log_entry["error"] = ""
            log_entry["user"] = get_log_field("user")
            log_entry["team"] = get_log_field("team")

            if response.status_code in [200, 201]:
                stats.log_success(i, log_entry)
            else:
                stats.log_skip(i, log_entry, f"HTTP {response.status_code}: {response.text[:200]}")
            print(f"📥 Response for Record {i}: {response.status_code} — {response.text[:200]}")
        except Exception as e:
            log_entry["message"] = str(e)
            log_entry["error"] = str(e)
            log_entry["user"] = get_log_field("user")
            log_entry["team"] = get_log_field("team")
            stats.log_skip(i, log_entry, f"Request failed: {str(e)}")
            print(f"📥 Response for Record {i}: Exception — {str(e)[:200]}")
    return stats.summary(), stats