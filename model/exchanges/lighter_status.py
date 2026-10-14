"""Read-only Better Stack status parsing for the two Lighter instances."""

from datetime import datetime


STATUS_URLS = {
    "mainnet.zklighter.elliot.ai": "https://status.lighter.xyz/index.json",
    "api.rh.lighter.xyz": "https://robinhoodstatus.lighter.xyz/index.json",
}


def parse_service_status(payload, now):
    attributes = payload["data"]["attributes"]
    state = attributes["aggregate_state"]
    included = payload["included"]
    if not isinstance(state, str) or not isinstance(included, list):
        raise ValueError("Invalid Lighter status feed")
    resources, maintenances = [], []
    active = state == "maintenance"
    for item in included:
        attrs = item["attributes"]
        if item["type"] == "status_page_resource":
            resources.append({"name": attrs["public_name"], "state": attrs["status"]})
            active = active or attrs["status"] == "maintenance"
        elif item["type"] == "status_report" and attrs.get("report_type") == "maintenance":
            if attrs.get("aggregate_state") == "resolved":
                continue
            starts_at = attrs.get("starts_at")
            if starts_at is None:
                raise ValueError("Maintenance start time missing")
            start = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
            if start.tzinfo is None:
                raise ValueError("Maintenance start time lacks timezone")
            # An expired estimate is not proof that unresolved maintenance ended.
            is_active = start.timestamp() <= now
            active = active or is_active
            maintenances.append({
                "id": item["id"], "title": attrs.get("title"),
                "starts_at": starts_at, "ends_at": attrs.get("ends_at"),
                "active": is_active,
            })
    return {"state": state, "maintenance_active": active,
            "maintenances": maintenances, "resources": resources}
