# /security/{teamId}/{userId}/removeuserfromteam
import requests
import json
import sys
import time
import datetime
from helpers.logger import MigrationStats, build_log_entry, write_detailed_audit_csv
from helpers.shared_logic import build_auth_headers

def handle(payload, migration_type, api_url, auth_token, entity):
    headers = build_auth_headers(auth_token)
    stats = MigrationStats()
    records = payload.get("records", [])

    max_retries = 3
    retry_delay = 0  # seconds

    for i, record in enumerate(records, start=1):
        stats.total += 1
        if not isinstance(record, dict):
            stats.log_skip(i, {}, "Invalid record format")
            continue

        meta = record.get("meta", {})
        source = meta.get("source", {})
        if isinstance(source, dict):
            meta["user"] = source.get("user", "")
            meta["team"] = source.get("team", "")
        meta.pop("source", None)

        packet = {
            "userId": record.get("userId"),
        }
        print(f"🔍 Row {i} payload: {json.dumps(packet, indent=2)}")

        method = "POST"
        team_id = meta.get("team_id")
        user_id = meta.get("user_id")
        endpoint = f"{api_url}/{team_id}/{user_id}/removeuserfromteam"
        if not team_id or not user_id:
            stats.log_skip(i, meta, "Missing team_id or user_id")
            continue

        def get_log_field(field):
            return meta.get(field, "")

        def get_record_id():
            return meta.get("id") or record.get("id", "")

        log_entry = build_log_entry(i, method, endpoint, record, get_log_field, get_record_id)
        print(f"🔍 Row {i} POST to: {endpoint} with payload: {packet}")
        sys.stdout.flush()

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
                        "user": get_log_field("user"),
                        "team": get_log_field("team"),
                        "adapter_key": payload.get("adapter_key", "users_teams_unrelate"),
                        "rowIndex": meta.get("rowIndex", i),
                        "timestamp": datetime.datetime.now().isoformat(),
                        "duration": round(stats.elapsed(), 2),
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
                        "user": get_log_field("user"),
                        "team": get_log_field("team"),
                        "adapter_key": payload.get("adapter_key", "users_teams_unrelate"),
                        "rowIndex": meta.get("rowIndex", i),
                        "timestamp": datetime.datetime.now().isoformat(),
                        "duration": round(stats.elapsed(), 2),
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
                    "user": get_log_field("user"),
                    "team": get_log_field("team"),
                    "adapter_key": payload.get("adapter_key", "users_teams_unrelate"),
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
        sys.stdout.flush()

    write_detailed_audit_csv(stats, "users_teams_unrelate")
    stats.write_summary_csv("audit/migration_summary_users_teams_unrelate.csv")
    return stats.summary(), stats