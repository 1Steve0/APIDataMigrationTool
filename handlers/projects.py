# projects.py

#  {"dataVersion": 1,
#       "values": {
#         "address.address": "",
#         "address.autoGeocode": true,
#         "address.country": "",
#         "address.location": {
#                 "latitude": "-34.5115149",
#                 "longitude": "150.7931905",
#                 "type": "Point"
#             },
#         "address.postCode": "",
#         "address.state": "",
#         "address.suburb": "",
#         "dateEnd": "2025-10-22T00:32:49",
#         "dateStart": "2025-10-22T00:32:49",
#         "name": "Glasshouse Mountains9",
#         "notes": "P0001757",
#         "projectsourceid": "36",
#         "projectGroup": {
#           "assign": [5337],
#           "unassign": []
#         },
#         "timeZone": "Australia/Sydney"
#       }
#     }

import requests
import time
from urllib.parse import urlparse
from helpers.shared_logic import fetch_entity_definition, auto_map_fields, build_auth_headers
from helpers.logger import MigrationStats, build_log_entry

def handle(payload, migration_type, api_url, auth_token, entity):
    headers = build_auth_headers(auth_token)
    stats = MigrationStats()
    records = payload.get("records", [])
    definition_url = api_url.replace("/entities/", "/definition/entity/")
    entity_definition = fetch_entity_definition(definition_url, headers)
    print(f"📡 Endpoint: {api_url}")

    for i, record in enumerate(records, start=1):
        stats.total += 1
        if not isinstance(record, dict):
            stats.log_skip(i, {}, "Invalid record format")
            continue

        meta = record.get("meta", {})
        values = record.get("values", {})
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
            start_time = time.time()
            response = requests.request(method, endpoint, json=packet, headers=headers, timeout=180)
            duration = round(time.time() - start_time, 2)

            status_code = response.status_code
            project_name = values.get("name", "<no name>")
            record_id = get_record_id()
            endpoint_path = urlparse(endpoint).path

            # Build row result including API response
            response_text = response.text.strip()
            row_result = {
                "rowIndex": i,
                "recordId": record_id,
                "project": project_name,
                "endpoint": endpoint,
                "status": status_code,
                "result": "Success" if status_code in [200, 201, 204] and "ErrorMessage" not in response_text else "Skipped",
                "response": response_text[:500]  # truncate to avoid huge cells
            }
            stats.rows.append(row_result)

            if status_code in [200, 201, 204] and "ErrorMessage" not in response_text:
                print(f"✅ Row: {i} | Record Id: {record_id} | Project: {project_name} | "
                    f"Status: {status_code} | Endpoint: {endpoint_path} | Result: Success | Duration: {duration}s")
                stats.log_success(i, log_entry, f"Response: {response_text[:200]}")
            else:
                reason = response_text[:200]
                print(f"❌ Row: {i} | Record Id: {record_id} | Project: {project_name} | "
                    f"Status: {status_code} | Endpoint: {endpoint_path} | Reason: {reason} | Result: Skipped | Duration: {duration}s")
                stats.log_skip(i, log_entry, f"HTTP {status_code}: {reason}")

        except Exception as e:
            stats.log_skip(i, log_entry, f"Request failed: {str(e)}")

    print(f"🕒 Completed migration for {entity} — {stats.total} rows processed")

    # Print summary of skipped reasons
    if stats.skipped > 0:
        print("⚠️ Skipped Reasons:")
        for reason in stats.skip_reasons:
            print(f"   - {reason}")

    return stats.summary(), stats