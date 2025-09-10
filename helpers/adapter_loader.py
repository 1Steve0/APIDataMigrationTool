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

        try:
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

def validate_adapter_output(data):
    """
    Validates the structure of adapter output.
    """
    if not isinstance(data, dict):
        raise TypeError("Adapter output must be a JSON object")

    if "records" not in data or not isinstance(data["records"], list):
        raise ValueError("Adapter output missing 'records' list")

    return data["records"]