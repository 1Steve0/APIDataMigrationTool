# helpers/shared_logic.py
import requests

def auto_map_fields(adapter_record, entity_definition, operation_mode="insert"):
    if operation_mode == "insert":
        return map_insert_fields(adapter_record, entity_definition)
    elif operation_mode == "update":
        return map_update_fields(adapter_record, entity_definition)
    else:
        raise ValueError(f"Unsupported operation_mode: {operation_mode}")

# === Validate Payload Structure ===
def fetch_entity_definition(definition_url, headers):
    response = requests.get(definition_url, headers=headers)

    print("🔗 Definition URL:", definition_url)
    print("📦 Response status:", response.status_code)
    print("📦 Response content:", response.text[:500])

    response.raise_for_status()

    try:
        return response.json()
    except requests.exceptions.JSONDecodeError as e:
        print("❌ JSON decode failed:", e)
        raise

def map_insert_fields(adapter_record, entity_definition):
    mapped = {}
    for field in entity_definition.get("fieldDefinitionSet", {}).values():
        alias = field["alias"]
        if alias in adapter_record:
            mapped[alias] = adapter_record[alias]
        elif field.get("required"):
            mapped[alias] = None
    return mapped

def map_update_fields(adapter_record, entity_definition):
    mapped = {}
    for field in entity_definition.get("fieldDefinitionSet", {}).values():
        alias = field["alias"]
        if alias in adapter_record:
            mapped[alias] = adapter_record[alias]
    if "id" in adapter_record:
        mapped["id"] = adapter_record["id"]
    return mapped

def get_log_field(record, field):
    if isinstance(record, dict):
        if "values" in record and isinstance(record["values"], dict):
            return record["values"].get(field, "")
        elif "payload" in record and isinstance(record["payload"], dict):
            return record["payload"].get("values", {}).get(field, "")
    return ""

def get_record_id(record):
    if isinstance(record, dict):
        return (
            record.get("id") or
            record.get("Id") or
            record.get("meta", {}).get("id") or
            record.get("payload", {}).get("id") or
            record.get("payload", {}).get("values", {}).get("id", "")
        )
    return ""

def build_auth_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }