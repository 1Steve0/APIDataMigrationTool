import requests
import time

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

def migrate_records(records, migration_type, api_url, auth_token, entity):
    """
    Core migration logic: Insert, Update, Upsert
    """
    stats = MigrationStats()
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    for i, record in enumerate(records, start=1):
        stats.total += 1
        try:
            record_id = record.get("id")
            endpoint = f"{api_url}/{entity}"

            if migration_type == "insert":
                if not record_id:
                    response = requests.post(endpoint, json=record, headers=headers)
                else:
                    # Check if ID exists
                    check = requests.get(f"{endpoint}/{record_id}", headers=headers)
                    if check.status_code == 404:
                        response = requests.post(endpoint, json=record, headers=headers)
                    else:
                        stats.log_skip(f"Row {i}: ID already exists")
                        continue

            elif migration_type == "update":
                if not record_id:
                    stats.log_skip(f"Row {i}: Missing ID for update")
                    continue
                response = requests.put(f"{endpoint}/{record_id}", json=record, headers=headers)

            elif migration_type == "upsert":
                if not record_id:
                    response = requests.post(endpoint, json=record, headers=headers)
                else:
                    check = requests.get(f"{endpoint}/{record_id}", headers=headers)
                    if check.status_code == 404:
                        response = requests.post(endpoint, json=record, headers=headers)
                    else:
                        response = requests.put(f"{endpoint}/{record_id}", json=record, headers=headers)

            else:
                stats.log_skip(f"Row {i}: Unknown migration type '{migration_type}'")
                continue

            if response.status_code in [200, 201]:
                stats.log_success()
            else:
                stats.log_skip(f"Row {i}: API error {response.status_code} - {response.text}")

        except Exception as e:
            stats.log_skip(f"Row {i}: Exception - {str(e)}")

    return stats.summary()