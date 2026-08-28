import json
import os

import requests

ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
POCKETBASE_URL = os.environ.get("POCKETBASE_URL", "").rstrip("/")
POCKETBASE_ADMIN_EMAIL = os.environ.get("POCKETBASE_ADMIN_EMAIL", "")
POCKETBASE_ADMIN_PASSWORD = os.environ.get("POCKETBASE_ADMIN_PASSWORD", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "GET, PATCH, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def pb_authenticate():
    resp = requests.post(
        f"{POCKETBASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": POCKETBASE_ADMIN_EMAIL, "password": POCKETBASE_ADMIN_PASSWORD},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def GetScheduledJobs(request):
    """Read-only, public: the full scheduled_jobs registry - both apps'
    Cloud Scheduler jobs and SocialMarketting's local cron jobs - each row
    doubling as its own notification-routing config (notify_telegram /
    notify_email_on_error / notify_email_always)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    try:
        token = pb_authenticate()
        resp = requests.get(
            f"{POCKETBASE_URL}/api/collections/scheduled_jobs/records",
            headers={"Authorization": token},
            params={"perPage": 200, "sort": "app,job_name"},
            timeout=15,
        )
        resp.raise_for_status()
        return json_response({"items": resp.json().get("items", [])})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def UpdateScheduledJob(request):
    """Writes to one scheduled_jobs row - the "define what goes as
    notification" config page's save action. Gated behind ADMIN_KEY since
    it's a write, even though it only affects notification routing."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if ADMIN_KEY and (not hasattr(request, "args") or request.args.get("key") != ADMIN_KEY):
        return json_response({"error": "Unauthorized"}, 401)

    body = request.get_json(silent=True) or {}
    record_id = body.get("id")
    if not record_id:
        return json_response({"error": "id is required"}, 400)

    allowed_fields = {
        "notify_telegram", "notify_email_on_error", "notify_email_always",
        "enabled", "description",
    }
    patch = {k: v for k, v in body.items() if k in allowed_fields}
    if not patch:
        return json_response({"error": "no updatable fields provided"}, 400)

    try:
        token = pb_authenticate()
        resp = requests.patch(
            f"{POCKETBASE_URL}/api/collections/scheduled_jobs/records/{record_id}",
            headers={"Authorization": token},
            json=patch,
            timeout=15,
        )
        resp.raise_for_status()
        return json_response({"updated": True, "record": resp.json()})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)


def GetJobRunsLog(request):
    """Read-only, public: the event log of scheduled_jobs runs (only for
    jobs that have been retrofitted to call NotificationRouting.notify() -
    see CLAUDE.md for which ones that currently covers)."""
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    job_name = request.args.get("job_name") if hasattr(request, "args") else None
    try:
        per_page = int(request.args.get("perPage", "100")) if hasattr(request, "args") else 100
    except ValueError:
        per_page = 100
    per_page = max(1, min(per_page, 500))

    filter_parts = []
    if job_name:
        filter_parts.append(f'job_name = "{job_name}"')
    filter_str = " && ".join(filter_parts)

    try:
        token = pb_authenticate()
        resp = requests.get(
            f"{POCKETBASE_URL}/api/collections/job_runs/records",
            headers={"Authorization": token},
            params={"perPage": per_page, "sort": "-ran_at", "filter": filter_str},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return json_response({"items": data.get("items", []), "totalItems": data.get("totalItems", 0)})
    except Exception as exc:
        return json_response({"error": str(exc)}, 500)
