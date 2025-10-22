#migration_engine.py
import requests, json
import time
import csv


class MigrationStats:
    def __init__(self):
        self.total = 0
        self.success = 0
        self.skipped = 0
        self.errors = []
        self.rows = [] 
        self.start_time = time.time()

    def log_success(self, row_index, log_entry):
        self.success += 1
        self.rows.append(log_entry)

    def log_skip(self, row_index, log_entry, reason):
        self.skipped += 1
        log_entry["reason"] = reason
        log_entry["status"] = "Skipped"
        self.errors.append(reason)
        self.rows.append(log_entry)

    def summary(self):
        return {
            "total": self.total,
            "success": self.success,
            "skipped": self.skipped,
            "errors": self.errors,
            "duration": round(time.time() - self.start_time, 2),
            "rows": self.rows 
        }
    def write_csv(self, path):
        fieldnames = ["row", "status", "name", "id", "message", "parentId", "description", "response_id", "error"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i, row in enumerate(self.rows, start=1):
                normalized = {key: row.get(key, "") for key in fieldnames}
                writer.writerow(normalized)

def migrate_records(records, migration_type, api_url, auth_token, entity, purge_existing=False):

    stats = MigrationStats()
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    print("📡 Posting to:", api_url)

    # === Required Fields Per Entity ===
    nullable_fields = {"classificationType", "description", "hierarchy", "name", "id"}
    REQUIRED_FIELDS = {
        "stakeholder": ["name", "parentId"],
        "event": ["name", "startDate"],
        "property": ["name"],
        "action": ["name"],
        "documents": ["name"],
        "project": ["name"],
        "organisation": ["name"],
        "xStakeholder Classifications": [
            "name", "parentId", "dataVersion", "description", "deleted", "classificationType"
        ]
    }
    required_fields = REQUIRED_FIELDS.get(entity, [])

    # === Migration Loop ===
    for i, record in enumerate(records, start=1):
        stats.total += 1

        # === Determine payload format ===
        # The API for CM "Classification" uses "values"
        # The API for CM "Project" uses "Values"
        if "values" in record:
            payload = record
            values = record["values"]
        elif "Values" in record:
            payload = record
            values = record["Values"]
        else:
            payload = {"values": record}
            values = record

        # === Normalize nulls ===
        for field in ["description", "parentId"]:
            if field in values and values[field] is None:
                values[field] = ""

        # === Required field check ===
        missing = [
            f for f in required_fields
            if f not in values or (values[f] is None and f not in nullable_fields)
        ]
        if missing:
            log_entry = {
                "row": i,
                "name": values.get("name", ""),
                "parentId": values.get("parentId", ""),
                "description": values.get("description", ""),
                "response_id": "",
                "reason": f"Missing required fields: {missing}",
                "status": "Skipped"
            }
            stats.log_skip(i, log_entry, log_entry["reason"])
            continue

        # === POST to API ===
        try:
            print(f"📤 Row {i} → {api_url}")
            print(json.dumps(payload, indent=2))
            response = requests.post(api_url, json=payload, headers=headers)

            if response.status_code in [200, 201, 204]:
                print(f"📬 Response status: {response.status_code}")
                print(f"📬 Response body: {response.text[:500]}")
                try:
                    response_data = response.json()
                    if "Errors" in response_data:
                        reason = response_data["Errors"][0]["Outcome"]["InvalidOperationValidationError"]["Reason"]
                        stats.log_skip(i, {
                            "row": i,
                            "name": values.get("name", ""),
                            "parentId": values.get("parentId", ""),
                            "description": values.get("description", ""),
                            "response_id": "",
                            "reason": reason,
                            "status": "Skipped"
                        }, reason)
                    elif "error" in response_data or "errors" in response_data:
                        reason = json.dumps(response_data, indent=2)
                        stats.log_skip(i, {
                            "row": i,
                            "name": values.get("name", ""),
                            "parentId": values.get("parentId", ""),
                            "description": values.get("description", ""),
                            "response_id": "",
                            "reason": reason,
                            "status": "Skipped"
                        }, reason)
                    else:
                        stats.log_success(i, {
                            "row": i,
                            "name": values.get("name", ""),
                            "parentId": values.get("parentId", ""),
                            "description": values.get("description", ""),
                            "response_id": response_data.get("id", ""),
                            "reason": "",
                            "status": "Success"
                        })
                except Exception as e:
                    print(f"⚠️ Failed to parse JSON: {str(e)}")
                    stats.log_success(i, {
                        "row": i,
                        "name": values.get("name", ""),
                        "parentId": values.get("parentId", ""),
                        "description": values.get("description", ""),
                        "response_id": "",
                        "reason": "",
                        "status": "Success"
                    })
            else:
                stats.log_skip(i, {
                    "row": i,
                    "name": values.get("name", ""),
                    "parentId": values.get("parentId", ""),
                    "description": values.get("description", ""),
                    "response_id": "",
                    "reason": f"HTTP {response.status_code}: {response.text[:200]}",
                    "status": "Skipped"
                }, f"HTTP {response.status_code}")
        except Exception as e:
            print(f"⚠️ Exception during migration: {str(e)}")
            stats.log_skip(i, {
                "row": i,
                "name": values.get("name", ""),
                "parentId": values.get("parentId", ""),
                "description": values.get("description", ""),
                "response_id": "",
                "reason": str(e),
                "status": "Exception"
            }, str(e))

    return stats.summary(), stats

