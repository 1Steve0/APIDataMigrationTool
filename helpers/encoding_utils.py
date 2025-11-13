#encoding_utils.py
import requests

def validate_credentials(email: str, password: str, base_url: str) -> bool:
    """
    Validates credentials by attempting a harmless API call.
    Returns True if successful, False otherwise.
    """
    url = f"{base_url}/classifications"

    try:
        response = requests.get(url, auth=(email, password), timeout=10)
        response.raise_for_status()
        print("✅ Credential validation succeeded")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Credential validation failed: {e}")
        return False