import requests
from urllib.parse import urlparse

def get_auth_token(email: str, password: str, base_url: str) -> str:
    parsed = urlparse(base_url)
    host_parts = parsed.netloc.split('.')
    tenant = host_parts[0]

    if "preview.com" in parsed.netloc:
        region = "australia"
    elif "consultationmanager.com" in parsed.netloc:
        region = "us"
    elif "consultationmanager.ca" in parsed.netloc:
        region = "canada"
    else:
        raise ValueError("Unknown region in API URL")

    login_url = f"{base_url}/auth/login"
    payload = {
        "email": email,
        "password": password
    }
    response = requests.post(login_url, json=payload)
    response.raise_for_status()

    token = response.json().get("token")
    if not token:
        raise Exception("Authentication succeeded but no token returned")

    print(f"🔐 Authenticated successfully for {email}")
    return token

def get_stakeholder_group_root(email: str, password: str, base_url: str) -> int:
    token = get_auth_token(email, password, base_url)
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{base_url}/classifications/byname/Stakeholder Groups"

    print(f"📊 Parsing hierarchy from: {url}")
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    for item in data:
        hierarchy = item.get("hierarchy")
        if hierarchy and hierarchy.startswith("/"):
            segments = hierarchy.strip("/").split("/")
            if len(segments) >= 1 and segments[0].isdigit():
                return int(segments[0])

    print("⚠️ Unexpected hierarchy format:", data)
    raise ValueError("Could not determine root hierarchy number for Stakeholder Groups")

if __name__ == "__main__":
    print("🔍 Available functions:")
    print(" - get_auth_token:", callable(get_auth_token))
    print(" - get_stakeholder_group_root:", callable(get_stakeholder_group_root))