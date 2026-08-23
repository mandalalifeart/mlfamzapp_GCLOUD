import json
import os

CLIENT_ID_USA = os.environ["CLIENT_ID_USA"]
CLIENT_SECRET_USA = os.environ["CLIENT_SECRET_USA"]
REFRESH_TOKEN_USA = os.environ["REFRESH_TOKEN_USA"]
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://mlfamzappfire.web.app")


def cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def json_response(body, status=200):
    return json.dumps(body), status, cors_headers()


def GetAwdInventory(request):
    if request.method == "OPTIONS":
        return "", 204, cors_headers()
    if request.method != "POST":
        return json_response({"error": "Method not allowed"}, 405)

    # Diagnostic first pass: confirm the installed python-amazon-sp-api build
    # actually exposes an AWD client before wiring up the real call - the
    # class/module name isn't confirmed yet.
    try:
        import inspect
        from sp_api.api import AmazonWarehousingAndDistribution

        init_source = inspect.getsource(AmazonWarehousingAndDistribution.__init__)
        class_source = inspect.getsource(AmazonWarehousingAndDistribution)

        return json_response({
            "status": "diagnostic",
            "init_source": init_source,
            "class_source_head": class_source[:3000],
        })
    except Exception as exc:
        return json_response({"error": str(exc), "type": exc.__class__.__name__}, 500)
