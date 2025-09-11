# adapter_loader.py
import subprocess
import json
import os

def run_php_adapter(adapter_path, input_file):
    """
    Executes a PHP adapter script and returns parsed JSON output.
    """
    if not os.path.exists(adapter_path):
        raise FileNotFoundError(f"Adapter not found: {adapter_path}")
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    try:
        result = subprocess.run(
            ['php', adapter_path, input_file],
            capture_output=True,
            text=True,
            encoding="utf-8",  # 👈 This is the key fix
            check=True
        )
        output = result.stdout.strip()
        output = output.lstrip('\ufeff').strip()
        
        try:
            print("📤 Raw adapter stdout:", output[:500])
            return json.loads(output)
        except json.JSONDecodeError as e:
            return {
                "error": "Adapter did not return valid JSON",
                "details": str(e),
                "stdout": output,
                "stderr": result.stderr
            }

    except subprocess.CalledProcessError as e:
        return {
            "error": "Adapter execution failed",
            "details": str(e),
            "stdout": e.stdout,
            "stderr": e.stderr
        }

def validate_adapter_output(parsed_output):
    print("🔍 Parsed adapter output (truncated):", str(parsed_output)[:500])

    if not isinstance(parsed_output, dict):
        raise ValueError("Adapter output is not a dictionary")

    if "records" not in parsed_output:
        raise ValueError("Adapter output missing 'records' list")

    records = parsed_output["records"]

    # 🔍 Log each record's keys and values for validation clarity
    for i, record in enumerate(records, start=1):
        print(f"\n🔍 Record {i} keys: {list(record.keys())}")
        for key in record:
            print(f"    {key}: {record[key]} (type: {type(record[key])})")

    return records