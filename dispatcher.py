# dispatcher.py

# === Entity handlers ===
from handlers import users, classifications, projects, teams

# === Relationship handlers ===
from handlers.relationships import (
    event_user,
    teams_users,
    teams_users_unrelate,
    teams_projects,
    teams_projects_unrelate
)

# === Adapter key → handler mapping ===
ADAPTER_HANDLERS = {
    "users": users.handle,
    "classifications": classifications.handle,
    "projects": projects.handle,
    "teams": teams.handle,
    "event_user_relationship": event_user.handle,
    "users_teams_role": teams_users.handle,
    "users_teams_unrelate": teams_users_unrelate.handle,
    "teams_projects_relationship": teams_projects.handle,
    "teams_projects_unrelate": teams_projects_unrelate.handle
}

# === Dispatcher entry point ===
def dispatch(adapter_key, payload, migration_type, api_url, auth_token, entity):
    handler = ADAPTER_HANDLERS.get(adapter_key)
    if not handler:
        raise ValueError(f"❌ No handler defined for adapter key: '{adapter_key}'")
    return handler(payload, migration_type, api_url, auth_token, entity)