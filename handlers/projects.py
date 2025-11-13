# projects.py
# {
#   "DataVersion": 1,
#   "projectOperations":{
#       "Relate": [],
#       "Unrelate":[]
#         },
#   "Values": {
#     "address.address": "",
#     "address.autoGeocode": false,
#     "address.country": "",
#     "address.location": {
#             "latitude": "-34.5115149",
#             "longitude": "150.7931905",
#             "type": "Point"
#         },
#     "address.postCode": "",
#     "address.state": "",
#     "address.suburb": "",
#     "dateEnd": "2025-11-12T08:03:18",
#     "dateStart": "2025-11-12T08:03:18",
#     "name": "",
#     "notes": "",
#     "projectemailaddress": "",
#     "projectGroup": {
#       "assign": [
#         0
#       ],
#       "unassign": [
#         0
#       ]
#     },
#     "projectsourceid": "",
#     "timeZone": ""
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
        if not isinstance(record, dict):
            stats.log_skip(i, {}, "Invalid record format")
            continue

        meta = record.get("meta", {})
        values = record.get("Values", {})
        project_ops = record.get("projectOperations", {})
        data_version = record.get("DataVersion", 1)

        if not values.get("name"):
            stats.log_skip(i, meta, "Missing required field: name")
            continue

        mapped_values = auto_map_fields(values, entity_definition, operation_mode=migration_type)

        packet = {
            "dataVersion": data_version,
            "values": mapped_values,
            "projectOperations": project_ops
        }

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
            return record_id if migration_type == "update" else values.get("id", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)

        try:
            response = requests.request(method, endpoint, json=packet, headers=headers, timeout=10)
            if response.status_code in [200, 201, 204]:
                stats.log_success(i, log_entry)
            else:
                stats.log_skip(i, log_entry, f"HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            stats.log_skip(i, log_entry, f"Request failed: {str(e)}")

    return stats.summary(), stats