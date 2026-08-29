"""Named Bid Optimizer rule presets - added 2026-08-29 at the user's request
to make saved rule profiles browser-independent (the first version used
localStorage, which doesn't carry over between browsers/devices). Low-stakes
UI preference data (not money-affecting), so unlike most write endpoints in
this project these are deliberately left ungated - a wrong/malicious save
here has no real cost, same reasoning as UpdateEtsyListings' unauthenticated
"Pull Listings Now" button."""
import json
import os

import requests

POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "").rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ.get("POCKETBASE_ADMIN_EMAIL", "")
POCKETBASE_ADMIN_PASSWORD = os.environ.get("POCKETBASE_ADMIN_PASSWORD", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")
COLLECTION = "ads_bid_rule_profiles"


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def pb_authenticate():
    response = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["token"]


def GetBidRuleProfiles(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    try:
        token = pb_authenticate()
        profiles = []
        page = 1
        while True:
            response = requests.get(
                f"{POCKETBASE_URL}/api/collections/{COLLECTION}/records",
                headers={"Authorization": token},
                params={"perPage": 200, "page": page, "sort": "name"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                profiles.append({
                    "name": item.get("name", ""),
                    "rules": item.get("rules", {}),
                    "country": item.get("country", ""),
                    "portfolio": item.get("portfolio", ""),
                })
            if page >= data.get("totalPages", 1):
                break
            page += 1
        return json_response({"profiles": profiles})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def SaveBidRuleProfile(request):
    """Upsert by name - deletes any existing profile with the same name
    first, so re-saving under an existing name cleanly overwrites it."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return json_response({"error": "name is required"}, 400)

    try:
        token = pb_authenticate()
        existing = requests.get(
            f"{POCKETBASE_URL}/api/collections/{COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": f'name = "{name}"', "fields": "id", "perPage": 1},
            timeout=30,
        ).json().get("items", [])
        for item in existing:
            requests.delete(f"{POCKETBASE_URL}/api/collections/{COLLECTION}/records/{item['id']}",
                             headers={"Authorization": token}, timeout=15)

        response = requests.post(
            f"{POCKETBASE_URL}/api/collections/{COLLECTION}/records",
            headers={"Authorization": token},
            json={"name": name, "rules": body.get("rules", {}), "country": body.get("country", ""),
                  "portfolio": body.get("portfolio", "")},
            timeout=15,
        )
        response.raise_for_status()
        return json_response({"saved": True})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def DeleteBidRuleProfile(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    name = request.args.get("name") if hasattr(request, "args") else None
    if not name:
        return json_response({"error": "name is required"}, 400)

    try:
        token = pb_authenticate()
        items = requests.get(
            f"{POCKETBASE_URL}/api/collections/{COLLECTION}/records",
            headers={"Authorization": token},
            params={"filter": f'name = "{name}"', "fields": "id", "perPage": 200},
            timeout=30,
        ).json().get("items", [])
        for item in items:
            requests.delete(f"{POCKETBASE_URL}/api/collections/{COLLECTION}/records/{item['id']}",
                             headers={"Authorization": token}, timeout=15)
        return json_response({"deleted": len(items)})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
