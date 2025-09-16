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

    # === Optional Purge Step ===
    if purge_existing:
        try:
            purge_response = requests.delete(api_url, headers=headers)
            if purge_response.status_code in [200, 204]:
                print(f"[INFO] Existing {entity} data purged successfully.")
            else:
                log_entry = {
                    "row": 0,
                    "name": "",
                    "parentId": "",
                    "description": "",
                    "response_id": "",
                    "reason": f"Purge failed: {purge_response.text}",
                    "status": str(purge_response.status_code)
                }
                stats.log_skip(0, log_entry, log_entry["reason"])
        except Exception as e:
            log_entry = {
                "row": 0,
                "name": "",
                "parentId": "",
                "description": "",
                "response_id": "",
                "reason": str(e),
                "status": "Exception"
            }
            stats.log_skip(0, log_entry, log_entry["reason"])

    # === Required Fields Per Entity ===
    nullable_fields = {
        "classificationType", "description", "hierarchy", "name", "id"
    }

    REQUIRED_FIELDS = {
        "stakeholder": ["name", "parentId"],
        "event": ["name", "startDate"],
        "property": ["name"],
        "action": ["name"],
        "documents": ["name"],
        "organisation": ["name"],
        "xStakeholder Classifications": [
            "name", "parentId", "dataVersion", "description", "deleted", "classificationType"
        ]
    }
    required_fields = REQUIRED_FIELDS.get(entity, [])

    # === Migration Loop ===
    for i, record in enumerate(records, start=1):
        stats.total += 1
        missing = []
        for f in required_fields:
            if f not in record or (record[f] is None and f not in nullable_fields):
                missing.append(f)

        if missing:
            log_entry = {
                "row": i,
                "name": record.get("name", ""),
                "parentId": record.get("parentId", ""),
                "description": record.get("description", ""),
                "response_id": "",
                "reason": "",
                "status": ""
            }
            stats.log_skip(i, log_entry, f"Missing required fields: {missing}")
            continue

        # === Ensure ID is an integer — only for update/upsert ===
        if migration_type != "insert":
            try:
                record["id"] = int(record["id"])
            except (ValueError, TypeError):
                log_entry = {
                    "row": i,
                    "name": record.get("name", ""),
                    "parentId": record.get("parentId", ""),
                    "description": record.get("description", ""),
                    "response_id": "",
                    "reason": "",
                    "status": ""
                }
                stats.log_skip(i, log_entry, "Invalid 'id' format — must be integer")
                continue

        try:
            payload = {"values": record}
            response = None

            # === Determine Endpoint and Method ===
            if migration_type == "insert":
                print(f"📤 Row {i} → {api_url}")
                print(json.dumps(payload, indent=2))
                response = requests.post(api_url, json=payload, headers=headers)

            elif migration_type == "update":
                record_id = record.get("id")
                if not record_id:
                    log_entry = {
                        "row": i,
                        "name": record.get("name", ""),
                        "parentId": record.get("parentId", ""),
                        "description": record.get("description", ""),
                        "response_id": "",
                        "reason": "",
                        "status": ""
                    }
                    stats.log_skip(i, log_entry, "Missing 'id' for update")
                    continue
                patch_url = f"{api_url}/{record_id}"
                print(f"📤 Row {i} → {patch_url}")
                print(json.dumps(payload, indent=2))
                response = requests.patch(patch_url, json=payload, headers=headers)

            elif migration_type == "upsert":
                record_id = record.get("id")
                if not record_id:
                    log_entry = {
                        "row": i,
                        "name": record.get("name", ""),
                        "parentId": record.get("parentId", ""),
                        "description": record.get("description", ""),
                        "response_id": "",
                        "reason": "",
                        "status": ""
                    }
                    stats.log_skip(i, log_entry, "Missing 'id' for upsert")
                    continue
                patch_url = f"{api_url}/{record_id}"
                check = requests.get(patch_url, headers=headers)
                if check.status_code == 404:
                    print(f"📤 Row {i} → {api_url}")
                    print(json.dumps(payload, indent=2))
                    response = requests.post(api_url, json=payload, headers=headers)
                else:
                    print(f"📤 Row {i} → {patch_url}")
                    print(json.dumps(payload, indent=2))
                    response = requests.patch(patch_url, json=payload, headers=headers)

            else:
                log_entry = {
                    "row": i,
                    "name": record.get("name", ""),
                    "parentId": record.get("parentId", ""),
                    "description": record.get("description", ""),
                    "response_id": "",
                    "reason": f"Unknown migration type '{migration_type}'",
                    "status": ""
                }
                stats.log_skip(i, log_entry, log_entry["reason"])
                continue

            # === Handle Response ===
            if response and response.status_code in [200, 201, 204]:
                print(f"📬 Response status: {response.status_code}")
                print(f"📬 Response body: {response.text[:500]}")
                try:
                    response_data = response.json()
                    if "Errors" in response_data:
                        error_reason = response_data["Errors"][0]["Outcome"]["InvalidOperationValidationError"]["Reason"]
                        log_entry = {
                            "row": i,
                            "name": record.get("name", ""),
                            "parentId": record.get("parentId", ""),
                            "description": record.get("description", ""),
                            "response_id": "",
                            "reason": error_reason,
                            "status": ""
                        }
                        stats.log_skip(i, log_entry, error_reason)
                    elif "error" in response_data or "errors" in response_data:
                        error_reason = json.dumps(response_data, indent=2)
                        log_entry = {
                            "row": i,
                            "name": record.get("name", ""),
                            "parentId": record.get("parentId", ""),
                            "description": record.get("description", ""),
                            "response_id": "",
                            "reason": error_reason,
                            "status": ""
                        }
                        stats.log_skip(i, log_entry, error_reason)
                    else:
                        log_entry = {
                            "row": i,
                            "status": "Migrated",
                            "name": record.get("name", ""),
                            "parentId": record.get("parentId", ""),
                            "description": record.get("description", ""),
                            "response_id": response_data.get("id", ""),
                            "reason": ""
                        }
                        stats.log_success(i, log_entry)
                except Exception as e:
                    print(f"⚠️ Failed to parse JSON: {str(e)}")
                    log_entry = {
                        "row": i,
                        "status": "Migrated",
                        "name": record.get("name", ""),
                        "parentId": record.get("parentId", ""),
                        "description": record.get("description", ""),
                        "response_id": "",
                        "reason": ""
                    }
                    stats.log_success(i, log_entry)
            else:
                log_entry = {
                    "row": i,
                    "name": record.get("name", ""),
                    "parentId": record.get("parentId", ""),
                    "description": record.get("description", ""),
                    "response_id": "",
                    "reason": response.text[:500],
                    "status": str(response.status_code)
                }
                stats.log_skip(i, log_entry, log_entry["reason"])

        except Exception as e:
            print(f"⚠️ Exception during migration: {str(e)}")
            log_entry = {
                "row": i,
                "name": record.get("name", ""),
                "parentId": record.get("parentId", ""),
                "description": record.get("description", ""),
                "response_id": "",
                "reason": str(e),
                "status": "Exception"
            }
            stats.log_skip(i, log_entry, log_entry["reason"])

    return stats.summary(), stats