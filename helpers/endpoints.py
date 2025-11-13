# helpers/endpoints.py
def get_entity_path(entity_name):
    entry = ENTITY_ENDPOINTS.get(entity_name)
    return entry["path"] if entry else None

def get_entity_definition(entity_name):
    entry = ENTITY_ENDPOINTS.get(entity_name)
    return entry["definition"] if entry else None

ENTITY_ENDPOINTS = {
    "classifications": {
        "path": "/classifications",
        "definition": "/definition/classifications"
    },
    "stakeholder": {
        "path": "/entities/stakeholder",
        "definition": "/definition/entity/stakeholder"
    },
    "event": {
        "path": "/entities/event",
        "definition": "/definition/entity/event"
    },
    "users": {
        "path": "/entities/user",
        "definition": "/definition/entity/user"
    },
    "property": {
        "path": "/entities/property",
        "definition": "/definition/entity/property"
    },
    "action": {
        "path": "/entities/action",
        "definition": "/definition/entity/action"
    },
    "teams": {
        "path": "/entities/team",
        "definition": "/definition/entity/team"
    },
    "documents": {
        "path": "/entities/documents",
        "definition": "/definition/entity/documents"
    },
    "organisation": {
        "path": "/entities/organisation",
        "definition": "/definition/entity/organisation"
    },
    "project": {
        "path": "/entities/project",
        "definition": "/definition/entity/project"
    },
    "teamProjectRelationship": {
        "path": "/relationships/teamProjectRelationship",
        "definition": "/definition/relationship/teamProjectRelationship"
    },
    "membershipUserTeam": {
        "path": "/relationships/membership",
        "definition": "/definition/relationships/membership"
    },
    "eventUserRelationshipUpdate": {
        "path": "/relationships/EventUser",
        "definition": "/definition/relationships/EventUser"
    }
}