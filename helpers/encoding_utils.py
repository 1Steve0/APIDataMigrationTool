import requests

def validate_credentials(email: str, password: str, base_url: str) -> bool:
    """
    Validates credentials by attempting a harmless API call.
    Returns True if successful, False otherwise.
    """
    url = f"{base_url}/classifications"

    try:
        response = requests.get(url, auth=(email, password))
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Credential validation failed: {e}")
        return False


def get_stakeholder_group_root(email: str, password: str, base_url: str) -> int:
    """
    Fetches all classifications and extracts the root hierarchy ID for 'Stakeholder Groups'.
    """
    url = f"{base_url}/classifications"
    print(f"📊 Fetching all classifications from: {url}")

    response = requests.get(url, auth=(email, password))
    response.raise_for_status()

    data = response.json()
    for item in data:
        if item.get("name") == "Stakeholder Groups":
            hierarchy = item.get("hierarchy")
            if hierarchy and hierarchy.startswith("/"):
                segments = hierarchy.strip("/").split("/")
                if segments and segments[0].isdigit():
                    return int(segments[0])

    print("⚠️ 'Stakeholder Groups' not found or hierarchy format unexpected")
    raise ValueError("Could not determine root hierarchy number for Stakeholder Groups")