"""Runs one Cloud Function's handler locally, straight from main.py, with no
Flask/HTTP layer - used by local cron entries to run the heaviest/slowest
scheduled jobs on this machine instead of on GCP (Cloud Run bills for the
full wall-clock time a function is active, including time spent sleeping in
an Amazon Reporting API poll loop - see CLAUDE.md "Local job runner" note).

Usage: python3 run_local.py <FunctionName> [query_param=value ...]
Reads ADMIN_KEY from the environment (same .env already used for deploys)
and passes it as the function's usual ?key= query param automatically.
"""
import os
import sys


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
    args.setdefault("key", os.environ.get("ADMIN_KEY", ""))

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
