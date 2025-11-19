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
        packet = {
            "dataVersion": record.get("DataVersion", 1),
            "projectOperations": record.get("ProjectOperations", {}),
            "values": record.get("values", {})
        }

        values = packet["values"]
        if not values.get("name"):
            stats.log_skip(i, meta, "Missing required field: name")
            continue

        if migration_type == "update":
            record_id = meta.get("id") or values.get("id")
            if not record_id:
                stats.log_skip(i, meta, "Missing ID for update")
                continue
            endpoint = f"{api_url}/{record_id}"
            method = "PATCH"
        else:
            endpoint = api_url
            method = "POST"

        def get_log_field(field):
            return meta.get(field, "")

        def get_record_id():
            return record_id if migration_type == "update" else values.get("id") or meta.get("teamssourceid", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)

        try:
            response = requests.request(method, endpoint, headers=headers, json=packet, timeout=180)
            if response.status_code in [200, 201, 204]:
                stats.log_success(i, log_entry)
            else:
                stats.log_skip(i, log_entry, f"HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            stats.log_skip(i, log_entry, f"Exception: {str(e)}")

    return stats.summary(), stats