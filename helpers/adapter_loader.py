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
            check=True
        )
        output = result.stdout.strip()
        return json.loads(output)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Adapter execution failed: {e.stderr.strip()}")

    except json.JSONDecodeError:
        raise ValueError("Adapter did not return valid JSON")

def validate_adapter_output(data):
    """
    Validates the structure of adapter output.
    """
    if not isinstance(data, dict):
        raise TypeError("Adapter output must be a JSON object")

    if data.get("status") != "success":
        raise ValueError(f"Adapter error: {data.get('error', 'Unknown error')}")

    if "records" not in data or not isinstance(data["records"], list):
        raise ValueError("Adapter output missing 'records' list")

    return data["records"]