import requests, json, time, csv

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
        fieldnames = [
            "row", "status", "name", "id", "message",
            "parentId", "description", "response_id",
            "error", "log_method", "log_endpoint"
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})

def migrate_records(payload, migration_type, api_url, auth_token, entity, purge_existing=False):
    records = payload.get("records", [])
    value_key = payload.get("valueKey", "values")
    project_ops_key = payload.get("projectOperationsKey", "projectOperations")
    data_version_key = payload.get("dataVersionKey", "dataVersion")
    stats = MigrationStats()
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    print("📡 Posting to:", api_url)

    REQUIRED_FIELDS = {
        "stakeholder": ["name", "parentId"],
        "event": ["name", "startDate"],
        "property": ["name"],
        "action": ["name"],
        "team": ["name"],
        "documents": ["name"],
        "project": ["name"],
        "users": ["firstName", "email"],
        "organisation": ["name"],
        "xStakeholder Classifications": [
            "name", "parentId", "dataVersion", "deleted", "classificationType"
        ]
    }
    required_fields = REQUIRED_FIELDS.get(entity, [])

    for i, record in enumerate(records, start=1):
        project_ops = record.get(project_ops_key)
        if not isinstance(project_ops, dict):
            print(f"⚠️ Using default empty projectOperations for row {i}")
            project_ops = {"relate": [], "unrelate": []}

        stats.total += 1
        values = record.get(value_key) if isinstance(record.get(value_key), dict) else record

        def get_log_field(field):
            return record.get(field) or values.get(field, "")

        for field in ["description", "parentId"]:
            if field in values and values[field] is None:
                values[field] = ""

        if migration_type != "update":
            missing = [f for f in required_fields if values.get(f) in [None, ""]]
            if missing:
                stats.log_skip(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": "",
                    "log_method": "POST",
                    "log_endpoint": api_url
                }, f"Missing required fields: {missing}")
                continue

        if migration_type == "update":
            record_id = record.get("id") or record.get("Id") or values.get("id")
            if not record_id:
                stats.log_skip(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": "",
                    "log_method": "PATCH",
                    "log_endpoint": f"{api_url}/<missing>"
                }, "Missing ID for update")
                continue

            endpoint = f"{api_url}/{record_id}"
            method = "PATCH"
            values.pop("id", None)

            payload_body = {
                data_version_key: record.get(data_version_key, 1),
                project_ops_key: project_ops,
                value_key: record.get(value_key, {})
            }

            if not isinstance(payload_body.get(value_key), dict):
                stats.log_skip(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": "",
                    "log_method": method,
                    "log_endpoint": endpoint,
                    "id": record_id
                }, f"Invalid '{value_key}' structure: expected dict")
                continue
        else:
            endpoint = api_url
            method = "POST"
            payload_body = record

        print(f"📤 Row {i} → {method} {endpoint}")
        print(json.dumps(payload_body, indent=2))

        MAX_RETRIES = 5
        RETRY_DELAY = 30 #seconds
        response = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.request(method, endpoint, json=payload_body, headers=headers, timeout=10)
                break
            except requests.exceptions.RequestException as e:
                print(f"⏳ Retry {attempt}/{MAX_RETRIES} on row {i} due to: {str(e)}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    stats.log_skip(i, {
                        "row": i,
                        "name": get_log_field("name") or get_log_field("firstName"),
                        "parentId": get_log_field("parentId"),
                        "description": get_log_field("description"),
                        "response_id": "",
                        "log_method": method,
                        "log_endpoint": endpoint,
                        "id": record_id if migration_type == "update" else record.get("id", "")
                    }, f"Request failed after {MAX_RETRIES} attempts: {str(e)}")

        if not response:
            continue

        if response.status_code in [200, 201, 204]:
            try:
                response_data = response.json()
                if "Errors" in response_data:
                    reason = response_data["Errors"][0]["Outcome"]["InvalidOperationValidationError"]["Reason"]
                    stats.log_skip(i, {
                        "row": i,
                        "name": get_log_field("name") or get_log_field("firstName"),
                        "parentId": get_log_field("parentId"),
                        "description": get_log_field("description"),
                        "response_id": "",
                        "log_method": method,
                        "log_endpoint": endpoint,
                        "id": record_id if migration_type == "update" else record.get("id", "")
                    }, reason)
                elif "error" in response_data or "errors" in response_data:
                    reason = json.dumps(response_data, indent=2)
                    stats.log_skip(i, {
                        "row": i,
                        "name": get_log_field("name") or get_log_field("firstName"),
                        "parentId": get_log_field("parentId"),
                        "description": get_log_field("description"),
                        "response_id": "",
                        "log_method": method,
                        "log_endpoint": endpoint,
                        "id": record_id if migration_type == "update" else record.get("id", "")
                    }, reason)
                else:
                    stats.log_success(i, {
                        "row": i,
                        "name": get_log_field("name") or get_log_field("firstName"),
                        "parentId": get_log_field("parentId"),
                        "description": get_log_field("description"),
                        "response_id": response_data.get("id", ""),
                        "log_method": method,
                        "log_endpoint": endpoint,
                        "id": record_id if migration_type == "update" else record.get("id", "")
                    })
            except Exception as e:
                print(f"⚠️ Failed to parse JSON: {str(e)}")
                stats.log_success(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": "",
                    "log_method": method,
                    "log_endpoint": endpoint,
                    "id": record_id if migration_type == "update" else record.get("id", ""),
                    "reason": "Non-JSON response, assumed success"
                })
            else:
                stats.log_skip(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": "",
                    "log_method": method,
                    "log_endpoint": endpoint,
                    "id": record_id if migration_type == "update" else record.get("id", "")
                }, f"HTTP {response.status_code}: {response.text[:200]}")

    return stats.summary(), stats