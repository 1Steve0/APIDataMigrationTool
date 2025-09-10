import requests
import time
import json

class MigrationStats:
    def __init__(self):
        self.total = 0
        self.success = 0
        self.skipped = 0
        self.errors = []
        self.start_time = time.time()

    def log_success(self):
        self.success += 1

    def log_skip(self, reason):
        self.skipped += 1
        self.errors.append(reason)

    def summary(self):
        return {
            "total": self.total,
            "success": self.success,
            "skipped": self.skipped,
            "errors": self.errors,
            "duration": round(time.time() - self.start_time, 2)
        }

def ensure_nested_category(path, api_url, headers):
    levels = path.split("/")
    parent_id = None

    for level in levels:
        check = requests.get(f"{api_url}/categories/{level}", headers=headers)
        if check.status_code == 200:
            parent_id = check.json().get("id")
        elif check.status_code == 404:
            payload = {"name": level}
            if parent_id:
                payload["parent_id"] = parent_id
            create = requests.post(f"{api_url}/categories", json=payload, headers=headers)
            if create.status_code in [200, 201]:
                parent_id = create.json().get("id")
            else:
                return None
        else:
            return None

    return parent_id

def migrate_records(records, migration_type, api_url, auth_token, entity,
                    purge_existing=False, create_categories=False):
    stats = MigrationStats()
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    endpoint = api_url  # ← no trailing /{entity}
    print("📡 Posting to:", endpoint)

    # === Optional Purge Step ===
    if purge_existing:
        try:
            purge_response = requests.delete(endpoint, headers=headers)
            if purge_response.status_code in [200, 204]:
                print(f"[INFO] Existing {entity} data purged successfully.")
            else:
                stats.log_skip(f"[WARN] Purge failed: {purge_response.status_code} - {purge_response.text}")
        except Exception as e:
            stats.log_skip(f"[ERROR] Exception during purge: {str(e)}")

    # === Migration Loop ===
    for i, record in enumerate(records, start=1):
        stats.total += 1
        try:
            # === Optional Nested Category Creation ===
            if create_categories:
                category_path = record.get("category_path")
                if category_path:
                    category_id = ensure_nested_category(category_path, api_url, headers)
                    if not category_id:
                        stats.log_skip(f"Row {i}: Failed to create nested category path '{category_path}'")
                        continue
                    record["category_id"] = category_id

            # === INSERT ===
            if migration_type == "insert":
                if "id" in record:
                    del record["id"]
                print(f"📤 Row {i} → {endpoint}")
                print(json.dumps(record, indent=2))
                response = requests.post(endpoint, json=record, headers=headers)

            # === UPDATE ===
            elif migration_type == "update":
                record_id = record.get("id")
                if not record_id:
                    stats.log_skip(f"Row {i}: Missing 'id' for update")
                    continue
                patch_url = f"{endpoint}/{record_id}"
                print(f"📤 Row {i} → {patch_url}")
                print(json.dumps(record, indent=2))
                response = requests.patch(patch_url, json=record, headers=headers)

            # === UPSERT ===
            elif migration_type == "upsert":
                record_id = record.get("id")
                if not record_id:
                    stats.log_skip(f"Row {i}: Missing 'id' for upsert")
                    continue
                patch_url = f"{endpoint}/{record_id}"
                check = requests.get(patch_url, headers=headers)
                if check.status_code == 404:
                    print(f"📤 Row {i} → {endpoint}")
                    print(json.dumps(record, indent=2))
                    response = requests.post(endpoint, json=record, headers=headers)
                else:
                    print(f"📤 Row {i} → {patch_url}")
                    print(json.dumps(record, indent=2))
                    response = requests.patch(patch_url, json=record, headers=headers)

            # === Unknown Type ===
            else:
                stats.log_skip(f"Row {i}: Unknown migration type '{migration_type}'")
                continue

            # === Response Handling ===
            if response.status_code in [200, 201]:
                stats.log_success()
            else:
                stats.log_skip(f"Row {i}: API error {response.status_code} - {response.text}")

        except Exception as e:
            stats.log_skip(f"Row {i}: Exception - {str(e)}")

    return stats.summary()
