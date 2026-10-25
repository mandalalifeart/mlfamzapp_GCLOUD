"""Runs one Cloud Function's handler locally, straight from main.py, with no
Flask/HTTP layer - used by local cron entries to run the heaviest/slowest
scheduled jobs on this machine instead of on GCP (Cloud Run bills for the
full wall-clock time a function is active, including time spent sleeping in
an Amazon Reporting API poll loop - see CLAUDE.md "Local job runner" note).

Usage: python3 run_local.py <FunctionName> [query_param=value ...]
Reads ADMIN_KEY from the environment (same .env already used for deploys)
and passes it as the function's usual ?key= query param automatically -
UNLESS the function is gated by its own narrower key (FINANCES_REFRESH_KEY,
WISE_REFRESH_KEY, etc, per FUNCTION_KEY_ENV below), matching this project's
standing practice of not reusing the shared ADMIN_KEY for those. Confirmed
live 2026-09-09: UpdateAmazonFinances's daily cron entry had been silently
401'ing every single day since it was set up, because this defaulted to
ADMIN_KEY while that function actually checks FINANCES_REFRESH_KEY - the
cron line itself has no way to pass a different key (a static crontab line
can't reference a value from the .env run_local.sh sources at run time), so
the fix has to live here, not in the crontab.
"""
import os
import sys

# Functions gated by a narrower secret than the shared ADMIN_KEY - see the
# module docstring. Add an entry here whenever a new function reuses this
# pattern (a frontend page holding a key scoped to just that one endpoint).
FUNCTION_KEY_ENV = {
    "UpdateAmazonFinances": "FINANCES_REFRESH_KEY",
    "UpdateWiseFinances": "WISE_REFRESH_KEY",
    "UpdateAmazonReturns": "RETURNS_REFRESH_KEY",
}


class FakeArgs(dict):
    """dict.get() doesn't accept Flask MultiDict's type= kwarg, which some
    functions (e.g. EtsyOrders.UpdateEtsyOrders) rely on - override get() to
    support it so those functions behave the same locally as on GCP."""

    def get(self, key, default=None, type=None):
        value = super().get(key, default)
        if type is not None and value is not None:
            try:
                value = type(value)
            except (TypeError, ValueError):
                return default
        return value


class FakeRequest:
    """Minimal stand-in for the Flask Request object these functions
    already expect - only .method and .args are ever touched."""

    def __init__(self, args):
        self.method = "GET"
        self.args = FakeArgs(args)

    def get_json(self, silent=True):
        return None


class _CapturedResponse:
    ok = True
    status_code = 200
    text = '{"ok": true, "result": {"message_id": 0}}'

    def json(self):
        return {"ok": True, "result": {"message_id": 0}}

    def raise_for_status(self):
        return None


def install_telegram_capture(name, function_name):
    """Combined Monday digest (2026-10-25, per the user): with
    TELEGRAM_CAPTURE=<name> set (e.g. on the Monday cron lines), every Telegram
    sendMessage this job makes is written to logs/digest/<name>/<date>/
    <function>.txt instead of being sent; MondayDigest.py sends them later as
    one message + a file. Files (sendDocument etc.) still go out normally."""
    import json as _json
    from datetime import date as _date
    import requests

    folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "digest", name, _date.today().isoformat())
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{function_name}.txt")
    real_post = requests.post

    def post(url, *a, **kw):
        if "api.telegram.org" in str(url) and str(url).endswith("/sendMessage"):
            body = kw.get("json") or kw.get("data") or {}
            if isinstance(body, (str, bytes)):
                try:
                    body = _json.loads(body)
                except ValueError:
                    body = {"text": str(body)}
            with open(path, "a", encoding="utf-8") as fh:
                fh.write((body.get("text") or "") + "\n\n")
            return _CapturedResponse()
        return real_post(url, *a, **kw)

    requests.post = post


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 run_local.py <FunctionName> [key=value ...]")
        sys.exit(1)

    function_name = sys.argv[1]
    args = {}
    for pair in sys.argv[2:]:
        if "=" in pair:
            k, v = pair.split("=", 1)
            args[k] = v
    key_env = FUNCTION_KEY_ENV.get(function_name, "ADMIN_KEY")
    args.setdefault("key", os.environ.get(key_env, ""))

    # PocketBase is on this machine - skip the public tunnel URL from .env
    # (same reasoning as local_server.py).
    os.environ["POCKETBASE_URL"] = os.environ.get("LOCAL_POCKETBASE_URL", "http://127.0.0.1:8090")
    if os.environ.get("TELEGRAM_CAPTURE"):
        install_telegram_capture(os.environ["TELEGRAM_CAPTURE"], function_name)
    import main as functions_module

    handler = getattr(functions_module, function_name, None)
    if handler is None:
        print(f"No such function: {function_name}")
        sys.exit(1)

    result = handler(FakeRequest(args))
    if isinstance(result, tuple):
        body, status = result[0], result[1]
        print(f"{function_name} finished: status={status}")
        print(body[:2000] if isinstance(body, str) else body)
    else:
        print(f"{function_name} finished: {result}")


if __name__ == "__main__":
    main()
