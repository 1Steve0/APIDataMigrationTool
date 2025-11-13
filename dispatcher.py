# dispatcher.py

# === Entity handlers ===
from handlers import users, classifications, projects, teams

# === Relationship handlers ===
from handlers.relationships import (
    event_user,
    teams_users,
    teams_projects,
)

# === Adapter key → handler mapping ===
ADAPTER_HANDLERS = {
    "users": users.handle,
    "classifications": classifications.handle,
    "projects": projects.handle,
    "teams": teams.handle,
    "event_user_relationship": event_user.handle,
    "teams_users_relationship": teams_users.handle,
    "teams_projects_relationship": teams_projects.handle
}

# === Dispatcher entry point ===
def dispatch(adapter_key, payload, migration_type, api_url, auth_token, entity):
    handler = ADAPTER_HANDLERS.get(adapter_key)
    if not handler:
        raise ValueError(f"❌ No handler defined for adapter key: '{adapter_key}'")
    return handler(payload, migration_type, api_url, auth_token, entity)