import requests
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

    def log_success(self, row_index, record):
        self.success += 1
        self.rows.append({
            "row": row_index + 1,
            "status": "Success",
            "name": record.get("name", ""),
            "id": record.get("id", ""),
            "message": ""
        })

    def log_skip(self, row_index, record, reason):
        self.skipped += 1
        self.errors.append(f"Row {row_index + 1}: {reason}")
        self.rows.append({
            "row": row_index + 1,
            "status": "Skipped",
            "name": record.get("name", ""),
            "id": record.get("id", ""),
            "message": reason
        })

    def summary(self):
        return {
            "total": self.total,
            "success": self.success,
            "skipped": self.skipped,
            "errors": self.errors,
            "duration": round(time.time() - self.start_time, 2)
        }
    def write_csv(self, path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["row", "status", "name", "id", "message"])
            writer.writeheader()
            writer.writerows(self.rows)

def migrate_records(records, migration_type, api_url, auth_token, entity,
                    purge_existing=False):
    import requests, json
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
                stats.log_skip(0, {}, f"[WARN] Purge failed: {purge_response.status_code} - {purge_response.text}")
        except Exception as e:
            stats.log_skip(0, {}, f"[ERROR] Exception during purge: {str(e)}")

    # === Required Fields Per Entity ===
    nullable_fields = {
        "classificationType",
        "description",
        "hierarchy",
        "name",
        "id"  # optional for inserts
    }


    REQUIRED_FIELDS = {
        "stakeholder": ["name", "parentId"],
        "event": ["name", "startDate"],
        "property": ["name"],
        "action": ["name"],
        "documents": ["name"],
        "organisation": ["name"],
        "xStakeholder Classifications": [
            "name", "hierarchyLevel", "hierarchy", "description", "deleted", "classificationType"
        ]
    }
    required_fields = REQUIRED_FIELDS.get(entity, [])

    # === Migration Loop ===
    for i, record in enumerate(records, start=1):
        stats.total += 1
        print(f"🔍 Raw keys for Row {i}: {list(record.keys())}")

        # === Required Field Validation ===
        print(f"\n🔍 Validating record:")
        for f in required_fields:
            print(f"  {f}: {record.get(f)} (type: {type(record.get(f))})")
        print(f"✅ Nullable fields: {nullable_fields}")

        missing = []
        for f in required_fields:
            if f not in record:
                missing.append(f)
            elif record[f] is None and f not in nullable_fields:
                missing.append(f)

        if missing:
            stats.log_skip(i, record, f"Missing required fields {missing}")
            continue

        # === Ensure ID is an integer ===
        try:
            record["id"] = int(record["id"])
        except (ValueError, TypeError):
            stats.log_skip(i, record, "Invalid 'id' format — must be integer")
            continue

        try:
            # === Determine Endpoint and Method ===
            response = None

            if migration_type == "insert":
                print(f"📤 Row {i} → {api_url}")
                print(json.dumps(record, indent=2))
                response = requests.post(api_url, json=record, headers=headers)

            elif migration_type == "update":
                record_id = record.get("id")
                if not record_id:
                    stats.log_skip(i, record, "Missing 'id' for update")
                    continue
                patch_url = f"{api_url}/{record_id}"
                print(f"📤 Row {i} → {patch_url}")
                print(json.dumps(record, indent=2))
                response = requests.patch(patch_url, json=record, headers=headers)

            elif migration_type == "upsert":
                record_id = record.get("id")
                if not record_id:
                    stats.log_skip(i, record, "Missing 'id' for upsert")
                    continue
                patch_url = f"{api_url}/{record_id}"
                check = requests.get(patch_url, headers=headers)
                if check.status_code == 404:
                    print(f"📤 Row {i} → {api_url}")
                    print(json.dumps(record, indent=2))
                    response = requests.post(api_url, json=record, headers=headers)
                else:
                    print(f"📤 Row {i} → {patch_url}")
                    print(json.dumps(record, indent=2))
                    response = requests.patch(patch_url, json=record, headers=headers)

            else:
                stats.log_skip(i, record, f"Unknown migration type '{migration_type}'")
                continue

            # === Handle Response ===
            if response and response.status_code in [200, 201]:
                stats.log_success(i, record)
            else:
                reason = f"API error {response.status_code} - {response.text}" if response else "No response"
                stats.log_skip(i, record, reason)

        except Exception as e:
            stats.log_skip(i, record, f"Exception during migration: {str(e)}")

    return stats.summary(), stats
