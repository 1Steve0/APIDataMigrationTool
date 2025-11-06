import requests, json, time, csv
from helpers.shared_logic import auto_map_fields, fetch_entity_definition

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
            "error", "log_method", "log_endpoint","reason"
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})

def migrate_records(payload, migration_type, api_url, auth_token, entity, purge_existing=False):
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    definition_url = f"{api_url.replace('/entities/', '/definition/entity/')}"
    entity_definition = fetch_entity_definition(definition_url, headers)
    records = payload.get("records", [])
    stats = MigrationStats()

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
        stats.total += 1

        # Build clean packet excluding 'id'
        packet = {k: v for k, v in record.items() if k != "id"}

        # Extract and clean values
        values = record.get("values", {})
        values = {k: v for k, v in values.items() if v not in ["", None]}

        # Auto-map values to schema
        mapped_values = auto_map_fields(values, entity_definition, operation_mode=migration_type)
        packet["values"] = mapped_values

        def get_log_field(field):
            return record.get(field) or values.get(field, "")

        for field in ["description", "parentId"]:
            if field in values and values[field] is None:
                values[field] = ""

        # Validate required fields
        if migration_type == "insert":
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

        # Determine endpoint and method
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
        else:
            endpoint = api_url
            method = "POST"

        print(f"📤 Row {i} → {method} {endpoint}")
        print(f"📦 Final packet for row {i}:\n{json.dumps(packet, indent=2)}")

        # Send request with retry
        response = None
        for attempt in range(1, 6):
            try:
                response = requests.request(method, endpoint, json=packet, headers=headers)
                break
            except requests.exceptions.RequestException as e:
                print(f"⏳ Retry {attempt}/5 on row {i} due to: {str(e)}")
                time.sleep(3)

        if not response:
            stats.log_skip(i, {
                "row": i,
                "name": get_log_field("name") or get_log_field("firstName"),
                "parentId": get_log_field("parentId"),
                "description": get_log_field("description"),
                "response_id": "",
                "log_method": method,
                "log_endpoint": endpoint
            }, "No response from server")
            continue

        # Parse response
        try:
            response_data = response.json()
        except Exception as e:
            print(f"⚠️ Failed to parse JSON for row {i}: {str(e)}")
            stats.log_success(i, {
                "row": i,
                "name": get_log_field("name") or get_log_field("firstName"),
                "parentId": get_log_field("parentId"),
                "description": get_log_field("description"),
                "response_id": "",
                "log_method": method,
                "log_endpoint": endpoint,
                "id": record.get("id", ""),
                "reason": "Non-JSON response, assumed success"
            })
            continue

        # Handle success or error
        if response.status_code in [200, 201, 204]:
            response_id = response_data.get("id", "")
            print(f"✅ Row {i} inserted successfully → ID {response_id}")
            if "Errors" in response_data:
                error = response_data["Errors"][0]
                outcome = error.get("Outcome", {})
                reason = ""
                if outcome:
                    for err_type, err_detail in outcome.items():
                        reason = err_detail.get("Reason") or err_detail.get("Message") or err_type
                        break
                else:
                    reason = error.get("Message", "Unknown error")

                if "duplicate" in reason.lower() and "email" in reason.lower():
                    reason = "Duplicate email in system"
                if not reason:
                    reason = "Unknown validation error"
                print(f"❌ Row {i} rejected: {reason}")

                stats.log_skip(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": "",
                    "log_method": method,
                    "log_endpoint": endpoint,
                    "id": record.get("id", "")
                }, reason)
            elif "error" in response_data or "errors" in response_data:
                stats.log_skip(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": "",
                    "log_method": method,
                    "log_endpoint": endpoint,
                    "id": record.get("id", "")
                }, json.dumps(response_data, indent=2))
            else:
                stats.log_success(i, {
                    "row": i,
                    "name": get_log_field("name") or get_log_field("firstName"),
                    "parentId": get_log_field("parentId"),
                    "description": get_log_field("description"),
                    "response_id": response_data.get("id", ""),
                    "log_method": method,
                    "log_endpoint": endpoint,
                    "id": record.get("id", "")
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
                "id": record.get("id", "")
            }, f"HTTP {response.status_code}: {response.text[:200]}")

    return stats.summary(), stats