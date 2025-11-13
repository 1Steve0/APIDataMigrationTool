#helpers/logger.py
import time
import csv
from datetime import datetime

def debug(msg):
    print(f"🐛 [debug] {datetime.now().isoformat()} — {msg}")

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
        log_entry["rowIndex"] = row_index 
        self.rows.append(log_entry)

    def log_skip(self, row_index, log_entry, reason):
        self.skipped += 1
        log_entry["reason"] = reason
        log_entry["status"] = "Skipped"
        log_entry["rowIndex"] = row_index 
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
        base_path = path
        print(f"🧾 [logger.py] Writing CSV to: {path}")
        print(f"🧾 [logger.py] Total rows to write: {len(self.rows)}")

        fieldnames = [
            "row", "rowIndex","status", "name", "id", "message", "parentId", "description", "response_id",
            "error", "log_method", "log_endpoint", "reason", "team", "project", "source","user"
        ]
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = f"{base_path}_{timestamp}.csv"
        
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                for row in self.rows:
                    if not isinstance(row, dict):
                        print(f"❌ [logger.py] Skipping non-dict row: {row}")
                        continue
                    try:
                        # Ensure all required fields are present
                        for key in fieldnames:
                            row.setdefault(key, "")
                        writer.writerow({key: row[key] for key in fieldnames})
                    except Exception as e:
                        print(f"❌ [logger.py] Failed to write row {row.get('row', '?')}: {e}")
        except Exception as e:
            print(f"❌ [logger.py] Failed to open/write file: {e}")
            return

        print(f"✅ [logger.py] CSV write complete: {path}")
        

def build_log_entry(i, method, endpoint, record, get_log_field, get_record_id):
    return {
        "row": i,
        "name": get_log_field("name") or get_log_field("firstName"),
        "parentId": get_log_field("parentId"),
        "description": get_log_field("description"),
        "response_id": "",
        "log_method": method,
        "log_endpoint": endpoint,
        "id": get_record_id(),
        "reason": ""  # ✅ Default placeholder
    }