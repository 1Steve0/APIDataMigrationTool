# {
#   "values": {
#     "classificationType": 0,
#     "dataVersion": 0,
#     "deleted": true,
#     "description": "string",
#     "name": "string",
#     "parentId": 0
#   }
# }
import requests
from helpers.shared_logic import fetch_entity_definition, auto_map_fields, build_auth_headers
from helpers.logger import MigrationStats, build_log_entry

def handle(payload, migration_type, api_url, auth_token, entity):
    headers = build_auth_headers(auth_token)
    stats = MigrationStats()
    records = payload.get("records", [])

    definition_url = api_url.replace("/entities/", "/definition/entity/")
    entity_definition = fetch_entity_definition(definition_url, headers)

    for i, record in enumerate(records, start=1):
        stats.total += 1
        meta = record.get("meta", {})
        values = record.get("values", {})

        if not values.get("name") or not values.get("parentId"):
            stats.log_skip(i, meta, "Missing required fields: name or parentId")
            continue

        packet = {
            "values": auto_map_fields(values, entity_definition, operation_mode="insert")
        }

        method = "POST"
        endpoint = api_url

        def get_log_field(field):
            return meta.get(field, "")

        def get_record_id():
            return meta.get("id") or values.get("id", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)

        try:
            response = requests.request(method, endpoint, json=packet, headers=headers, timeout=10)
            if response.status_code in [200, 201]:
                stats.log_success(i, log_entry)
            else:
                stats.log_skip(i, log_entry, f"HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            stats.log_skip(i, log_entry, f"Request failed: {str(e)}")

    return stats.summary(), stats