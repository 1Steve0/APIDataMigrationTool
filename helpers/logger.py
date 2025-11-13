#helpers/logger.py
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
        log_entry["status"] = "Success"
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
            "error", "log_method", "log_endpoint", "reason"
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})

def build_log_entry(i, method, endpoint, record, get_log_field, get_record_id):
    return {
        "row": i,
        "name": get_log_field("name") or get_log_field("firstName"),
        "parentId": get_log_field("parentId"),
        "description": get_log_field("description"),
        "response_id": "",
        "log_method": method,
        "log_endpoint": endpoint,
        "id": get_record_id()
    }