import json
import os

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, x-admin-key",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def UpdateSkuSalesMonth(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()

    return json_response(
        {
            "status": "disabled",
            "message": "UpdateSkuSalesMonth is disabled because Supabase was removed from this codebase.",
        },
        410,
    )
