# handlers/users.py
# {
#   "DataVersion": 1,
#   "SendOnboardingEmail": false,
#   "stereotypeOperations": {
#     "Relate": ["StandardUser"],
#     "Unrelate": []
#   },
#   "Values": {
#     "usersourceid": "196",
#     "email": "Natalia.Morawski@aecom.com",
#     "userstatus": 0,
#     "useLegacyLogin": false,
#     "department": "",
#     "fax": "",
#     "firstName": "",
#     "lastName": "",
#     "login": "",
#     "mobile": "",
#     "notes": "",
#     "organisation": "",
#     "phone": "",
#     "position": ""
#   }
# }

# { #Update
#     "dataVersion": 1,
#     "SendOnboardingEmail": false,
#     "stereotypeOperations": {
#         "Relate": [],
#         "Unrelate": []
#     },
#     "values": {
#         "usersourceid": "9999"
#     }
# }
import requests
from helpers.shared_logic import auto_map_fields, fetch_entity_definition, build_auth_headers
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
        values = record.get("values", {})
        stereotype_ops = record.get("stereotypeOperations", {})
        send_email = record.get("SendOnboardingEmail", False)
        data_version = record.get("DataVersion", 1)

        # if not values.get("email") or not values.get("firstName"):
        #     stats.log_skip(i, meta, "Missing required fields: email or firstName")
        #     continue

        mapped_values = auto_map_fields(values, entity_definition, operation_mode=migration_type)

        packet = {
            "dataVersion": data_version,
            "SendOnboardingEmail": send_email,
            "stereotypeOperations": stereotype_ops,
            "values": mapped_values
        }

        if migration_type == "update":
            record_id = meta.get("id") or values.get("id")
            if not record_id:
                stats.log_skip(i, meta, "Missing ID for update")
                continue
            endpoint = f"{api_url}/{record_id}"
            method = "PATCH"
            print(f"🔧 Row {i}: PATCH to {endpoint} for user ID {record_id}\n{packet}")
        else:
            endpoint = api_url
            method = "POST"


        def get_log_field(field):
            return meta.get(field, "")

        def get_record_id():
            return record_id if migration_type == "update" else values.get("id") or values.get("usersourceid", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)

        try:
            response = requests.request(method, endpoint, headers=headers, json=packet, timeout=180)
            if response.status_code in [200, 201, 204]:
                stats.log_success(i, log_entry)
            else:
                stats.log_skip(i, log_entry, f"HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            stats.log_skip(i, log_entry, f"Request failed: {str(e)}")

    return stats.summary(), stats