# teams.py
# {
#   "DataVersion": 1,
#   "ProjectOperations": {
#     "Relate": [
#       0
#     ],
#     "Unrelate": [
#       0
#     ]
#   },
#   "Values": {
#     "description": "",
#     "name": "",
#     "teamssourceid": ""
#   }
# }


# handlers/teams.py
import requests
from helpers.logger import MigrationStats, build_log_entry
from helpers.shared_logic import build_auth_headers

def handle(payload, migration_type, api_url, auth_token, entity):
    stats = MigrationStats()
    headers = build_auth_headers(auth_token)
    records = payload.get("records", [])

    for i, record in enumerate(records, start=1):
        stats.total += 1
        if not isinstance(record, dict):
            stats.log_skip(i, {}, "Invalid record format")
            continue

        meta = record.get("meta", {})
        # Adapter emits capitalized keys; we map to the packet keys expected by the API
        packet = {
            "dataVersion": record.get("DataVersion", 1),
            "projectOperations": record.get("ProjectOperations", {}),
            "values": record.get("Values", {})
        }

        values = packet["values"]

        # Optional lightweight validation; remove if you want the API to fully decide
        if "name" not in values or str(values.get("name", "")).strip() == "":
            stats.log_skip(i, meta, "Missing required field: name")
            continue

        # ID resolution: prefer meta.id, fall back to values.id or teamssourceid
        record_id = None
        if migration_type == "update":
            record_id = meta.get("id") or values.get("id") or values.get("teamssourceid")
            if not record_id:
                stats.log_skip(i, meta, "Missing ID for update")
                continue
            endpoint = f"{api_url}/{record_id}"
            method = "PATCH"
        else:
            endpoint = api_url
            method = "POST"

        def get_log_field(field):
            # enable build_log_entry to extract meta fields for audit CSVs
            return meta.get(field, "")

        def get_record_id():
            return record_id if migration_type == "update" else values.get("id") or values.get("teamssourceid", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)

        try:
            print(f"🔧 Row {i}: {method} {endpoint} (team '{values.get('name','')}', source '{values.get('teamssourceid','')}')\n{packet}")
            response = requests.request(method, endpoint, headers=headers, json=packet, timeout=180)
            print(f"📦 Response body (row {i}): {response.text}")
            if response.status_code in [200, 201, 204]:
                stats.log_success(i, log_entry)
            else:
                stats.log_skip(i, log_entry, f"HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            stats.log_skip(i, log_entry, f"Exception: {str(e)}")

    return stats.summary(), stats