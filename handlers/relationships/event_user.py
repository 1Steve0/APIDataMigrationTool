# {
#   "DataVersion": 1,
#   "Values": {
#     "LeftHandId": 0,
#     "RightHandId": 0
#   }
# }
import requests
from helpers.logger import MigrationStats
from helpers.shared_logic import build_auth_headers
import csv
from datetime import datetime
import time


def handle(payload, migration_type, api_url, auth_token, entity):
    audit_rows = []
    headers = build_auth_headers(auth_token)
    stats = MigrationStats()
    records = payload.get("records", [])

    for i, record in enumerate(records, start=1):
        stats.total += 1
        if not isinstance(record, dict):
            stats.log_skip(i, {}, "Invalid record format")
            continue

        method = record.get("method", "PUT")
        endpoint = record.get("endpoint", api_url)
        packet = record.get("payload", {})
        meta = record.get("meta", {})

        values = packet.get("values", {})
        if migration_type == "insert":
            if not values.get("LeftHandId") or not values.get("RightHandId"):
                stats.log_skip(i, meta, "Missing LeftHandId or RightHandId")
                continue

        if migration_type == "update":
            if not values.get("id"):
                stats.log_skip(i, meta, "Missing ID for update")
                continue
            endpoint = f"{endpoint}/{values['id']}"
            method = "PATCH"

        status_code = None
        message = ""
        result = "Skipped"
        row_index = meta.get("rowIndex", i)
        left = values.get("LeftHandId", "")
        right = values.get("RightHandId", "")

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            time.sleep(1)  # ⏳ Delay before each attempt
            try:
                response = requests.request(method, endpoint, json=packet, headers=headers, timeout=180)
                status_code = response.status_code
                message = response.text.strip()

                print(f"📄 Record {row_index} Attempt {attempt} — Left: {left}, Right: {right}, Status: {status_code}")

                if status_code in [200, 201, 204]:
                    stats.log_success(i, {**meta, "response_id": values.get("id", "")})
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
                print(f"📄 Record {row_index} Attempt {attempt} — Left: {left}, Right: {right}, Status: Exception")

            if attempt < max_retries:
                print(f"⏳ Waiting 14 seconds before retrying...")
                time.sleep(8)

            if attempt == max_retries:
                stats.log_skip(i, meta, f"Failed after {max_retries} attempts: {message}")

        audit_rows.append([
            row_index,
            left,
            right,
            method,
            endpoint,
            status_code or "Exception",
            result,
            message
        ])

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"audit_event_user_{timestamp}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Row Index", "Event ID", "User ID", "Method", "Endpoint",
            "HTTP Status", "Result", "Message"
        ])
        writer.writerows(audit_rows)

    print(f"📝 Audit log written to {filename} with {len(audit_rows)} rows")
    return stats.summary(), stats
